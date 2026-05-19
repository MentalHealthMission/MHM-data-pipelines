"""Run custom derived feature scripts from a YAML spec."""
from __future__ import annotations

import importlib.util
import inspect
import json
import logging
from pathlib import Path
import sys
import types
from typing import Any, Dict, Mapping, Optional

import pandas as pd
from pandas.errors import EmptyDataError
import yaml

from .context import RunContext, entity_group
from ..derived_features.utils import ensure_output_dir


log = logging.getLogger(__name__)


def _is_static_literal(node: Any) -> bool:
    import ast

    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return all(_is_static_literal(item) for item in node.elts)
    if isinstance(node, ast.Dict):
        return all(_is_static_literal(key) for key in node.keys if key is not None) and all(
            _is_static_literal(value) for value in node.values
        )
    return False


def _load_notebook_source(path: Path) -> str:
    raw = json.loads(path.read_text(encoding="utf-8"))
    cells = raw.get("cells") or []
    chunks = []
    for cell in cells:
        if not isinstance(cell, Mapping):
            continue
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source") or []
        if isinstance(source, list):
            text = "".join(str(part) for part in source)
        else:
            text = str(source)
        if text.strip():
            chunks.append(text)
    return "\n\n".join(chunks)


def _load_module_from_notebook(script_path: Path):
    import ast

    source = _load_notebook_source(script_path)
    tree = ast.parse(source, filename=str(script_path))
    future_imports = []
    imports = []
    allowed_nodes = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            future_imports.append(node)
            continue
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            imports.append(node) if isinstance(node, (ast.Import, ast.ImportFrom)) else allowed_nodes.append(node)
            continue
        if isinstance(node, ast.Assign) and _is_static_literal(node.value):
            allowed_nodes.append(node)
            continue
        if isinstance(node, ast.AnnAssign) and node.value is not None and _is_static_literal(node.value):
            allowed_nodes.append(node)
            continue
    ordered_nodes = future_imports + imports + allowed_nodes
    module = types.ModuleType(script_path.stem)
    module.__file__ = str(script_path)
    notebook_dir = script_path.parent.resolve()
    notebook_root = notebook_dir.parent.resolve()
    for candidate in [notebook_dir, notebook_root]:
        candidate_str = str(candidate)
        if candidate_str not in sys.path:
            sys.path.insert(0, candidate_str)
    for node in ordered_nodes:
        snippet = ast.Module(body=[node], type_ignores=[])
        ast.fix_missing_locations(snippet)
        code = compile(snippet, str(script_path), "exec")
        try:
            exec(code, module.__dict__)
        except ModuleNotFoundError:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                continue
            raise
    return module


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _load_module(script_path: Path):
    if script_path.suffix == ".ipynb":
        return _load_module_from_notebook(script_path)
    spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _resolve_metric_path(
    merged_dir: Path,
    group: str,
    entity_id: str,
    metric: str,
    input_spec: Mapping[str, Any],
    run_id: Optional[str],
) -> Optional[Path]:
    path_override = input_spec.get("path")
    if path_override:
        try:
            rendered = str(path_override).format(
                group=group,
                entity_id=entity_id,
                entity=entity_id,
                site=group,
                participant_id=entity_id,
                metric=metric,
                run_id=run_id or "",
            )
        except KeyError:
            rendered = str(path_override)
        return Path(rendered).expanduser()
    metric_dir = merged_dir / group / entity_id / metric
    if not metric_dir.exists():
        return None
    for suffix in (".csv.gz", ".csv", ".parquet"):
        candidate = metric_dir / f"{metric}{suffix}"
        if candidate.exists():
            return candidate
    for candidate in metric_dir.iterdir():
        if candidate.suffix in {".csv", ".gz", ".parquet"}:
            return candidate
    return None


def _read_metric_file(path: Path) -> pd.DataFrame:
    if path.exists() and path.stat().st_size == 0:
        return pd.DataFrame()
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".gz" or path.name.endswith(".csv.gz"):
        try:
            return pd.read_csv(path, compression="gzip")
        except EmptyDataError:
            return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _parse_inputs(raw_inputs: object) -> Dict[str, Dict[str, Any]]:
    if raw_inputs is None:
        return {}
    if isinstance(raw_inputs, dict):
        return {str(name): dict(spec or {}) for name, spec in raw_inputs.items()}
    if isinstance(raw_inputs, list):
        inputs: Dict[str, Dict[str, Any]] = {}
        for item in raw_inputs:
            if not isinstance(item, Mapping):
                continue
            name = str(item.get("name", "")).strip()
            if not name:
                raise ValueError("Input entry missing name")
            inputs[name] = {k: v for k, v in item.items() if k != "name"}
        return inputs
    raise ValueError("inputs must be a mapping or list")


def run_derived_features_for_entity(
    context: RunContext,
    entity_id: str,
    *,
    spec_path: Path,
    output_dir: Path,
    input_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    spec = _load_yaml(spec_path)
    features = list(spec.get("features", []))
    if not features:
        return {"status": "skipped", "reason": "no features"}

    group = entity_group(context, entity_id)
    if not group:
        for candidate in context.merged_dir.iterdir():
            if (candidate / entity_id).exists():
                group = candidate.name
                context.entity_groups[entity_id] = group
                context.participant_sites[entity_id] = group
                break
    if not group:
        raise KeyError(f"Unknown group for entity {entity_id}")

    merged_dir = input_dir or context.merged_dir
    entity_out_dir = ensure_output_dir(output_dir, entity_id)
    outputs: Dict[str, str] = {}

    for feature in features:
        feature_id = str(feature.get("id", "")).strip()
        if not feature_id:
            raise ValueError("Feature definition missing id")
        script_path = Path(str(feature.get("script", ""))).expanduser()
        if not script_path.is_absolute():
            script_path = (Path.cwd() / script_path).resolve()
        if not script_path.exists():
            raise FileNotFoundError(f"Feature script not found: {script_path}")

        input_specs = _parse_inputs(feature.get("inputs"))
        inputs: Dict[str, Optional[pd.DataFrame]] = {}
        for name, input_spec in input_specs.items():
            metric = input_spec.get("metric")
            if not metric:
                inputs[name] = None
                continue
            metric_path = _resolve_metric_path(
                merged_dir,
                group,
                entity_id,
                str(metric),
                input_spec,
                run_id=context.run_id,
            )
            if not metric_path or not metric_path.exists():
                inputs[name] = None
                continue
            inputs[name] = _read_metric_file(metric_path)

        module = _load_module(script_path)
        if not hasattr(module, "compute"):
            raise AttributeError(f"{script_path} must define compute()")

        params = dict(feature.get("params") or {})
        compute_kwargs = {
            "inputs": inputs,
            "input_specs": input_specs,
            "params": params,
            "participant_id": entity_id,
            "logger": context.logger,
        }
        try:
            signature = inspect.signature(module.compute)
        except (TypeError, ValueError):
            signature = None
        if signature is not None and (
            "entity_id" in signature.parameters
            or any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values())
        ):
            compute_kwargs["entity_id"] = entity_id
        result = module.compute(**compute_kwargs)
        if result is None:
            context.logger.info("[derived ] %s produced no output for %s", feature_id, entity_id)
            continue

        output_map: Dict[str, pd.DataFrame]
        if isinstance(result, pd.DataFrame):
            output_map = {"default": result}
        elif isinstance(result, dict):
            output_map = {str(k): v for k, v in result.items() if isinstance(v, pd.DataFrame)}
        else:
            raise ValueError(f"Unexpected output type from {script_path}: {type(result)}")

        output_names = feature.get("outputs") or {}
        for name, df in output_map.items():
            filename = output_names.get(name)
            if not filename:
                filename = f"{feature_id}.csv" if name == "default" else f"{feature_id}_{name}.csv"
            target = entity_out_dir / filename
            df.to_csv(target, index=False)
            outputs[f"{feature_id}:{name}"] = str(target)

    return {"status": "ok", "outputs": outputs}


def run_derived_features_for_participant(
    context: RunContext,
    participant_id: str,
    *,
    spec_path: Path,
    output_dir: Path,
    input_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """Compatibility wrapper for participant-named callers."""

    return run_derived_features_for_entity(
        context,
        participant_id,
        spec_path=spec_path,
        output_dir=output_dir,
        input_dir=input_dir,
    )


__all__ = ["run_derived_features_for_entity", "run_derived_features_for_participant"]
