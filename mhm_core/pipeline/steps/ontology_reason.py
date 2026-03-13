"""Apply ontology reasoning to unified features."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import csv
import gzip
import json
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import pandas as pd
import yaml

from .base import PipelineStep
from ..context import RunContext, active_participants
from ...derived_features.catalog import build_catalog
from ...derived_features.utils import ensure_output_dir
from ...ontology.reason import (
    apply_rules,
    build_graph,
    inferred_phenotypes,
    load_feature_plan,
    load_metric_mapping,
    summarize_trace,
    trace_phenotypes,
)


class OntologyReasonStep(PipelineStep):
    def __init__(self, options: Dict[str, object]) -> None:
        super().__init__("ontology_reason", options, run_per_participant=False)

    def run(self, context: RunContext) -> Dict[str, object]:
        plan_raw = self.options.get("plan")
        if not plan_raw:
            raise ValueError("ontology_reason requires plan path")
        plan_path = Path(str(plan_raw).format(run_id=context.run_id)).expanduser()
        if not plan_path.exists():
            raise FileNotFoundError(f"Ontology feature plan not found: {plan_path}")

        mapping_raw = self.options.get("mapping")
        mapping_path = Path(str(mapping_raw).format(run_id=context.run_id)).expanduser() if mapping_raw else None
        if mapping_path is None:
            plan_data = yaml.safe_load(plan_path.read_text(encoding="utf-8")) or {}
            plan_mapping = plan_data.get("mapping")
            if plan_mapping:
                mapping_path = Path(str(plan_mapping).format(run_id=context.run_id)).expanduser()

        rules_raw = self.options.get("rules")
        rules_dir_raw = self.options.get("rules_dir")
        rule_entries = []
        if rules_raw:
            rule_entries = list(rules_raw) if isinstance(rules_raw, (list, tuple)) else [rules_raw]
        elif rules_dir_raw:
            rules_dir = Path(str(rules_dir_raw).format(run_id=context.run_id)).expanduser()
            if rules_dir.exists():
                rule_entries = sorted(rules_dir.glob("*.rq"))
        if not rule_entries:
            raise ValueError("ontology_reason requires rules or rules_dir")

        rules_paths = []
        for entry in rule_entries:
            rule_path = Path(str(entry).format(run_id=context.run_id)).expanduser()
            if not rule_path.exists():
                raise FileNotFoundError(f"Ontology rule file not found: {rule_path}")
            rules_paths.append(rule_path)

        unified_dir = Path(str(self.options.get("unified_dir", context.workspace_dir / "ontology" / "unified")).format(run_id=context.run_id)).expanduser()
        output_dir = Path(str(self.options.get("output_dir", context.workspace_dir / "ontology" / "reasoned")).format(run_id=context.run_id)).expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)

        ontology_paths = []
        ontology_files = self.options.get("ontology_files")
        if ontology_files:
            for entry in ontology_files:
                candidate = Path(str(entry).format(run_id=context.run_id)).expanduser()
                ontology_paths.append(candidate)
        else:
            ontology_dir_raw = self.options.get("ontology_dir")
            if ontology_dir_raw:
                candidate = Path(str(ontology_dir_raw).format(run_id=context.run_id)).expanduser() / "mhm_ontology.owl"
                ontology_paths.append(candidate)
            else:
                candidate = Path("external/mhm-ontology/mhm_ontology.owl")
                if candidate.exists():
                    ontology_paths.append(candidate)

        tokens: Dict[str, object] = {}
        for key in [
            "steps_threshold",
            "unlock_threshold",
            "low_activity_steps_threshold",
            "sleep_onset_std_threshold",
            "sleep_onset_deviation_threshold",
            "nights_under_4h_threshold",
            "awakenings_threshold",
            "low_activity_window_threshold",
            "sleep_irregularity_window_threshold",
        ]:
            if key in self.options:
                tokens[key] = self.options.get(key)
        extra_tokens = self.options.get("tokens") or {}
        if isinstance(extra_tokens, dict):
            tokens.update(extra_tokens)

        feature_plan = load_feature_plan(plan_path)
        feature_plan_raw = yaml.safe_load(plan_path.read_text(encoding="utf-8")) or {}
        metric_mapping = load_metric_mapping(mapping_path) if mapping_path else {}
        trace_sources = _build_trace_source_index(feature_plan_raw)
        derived_output_index = _build_derived_output_index(
            context=context,
            options=self.options,
        )
        rapids_output_index = _build_rapids_output_index(context=context)
        emit_trace_graph = bool(self.options.get("trace_graph", True))

        participants = active_participants(context)
        processed = 0
        for participant_id in participants:
            unified_path = unified_dir / participant_id / "unified_features.csv"
            participant_out = ensure_output_dir(output_dir, participant_id)
            inferred = pd.DataFrame()
            trace = pd.DataFrame()
            unified = pd.DataFrame()
            if unified_path.exists():
                unified = pd.read_csv(unified_path)
            if not unified.empty:
                graph = build_graph(
                    participant_id=participant_id,
                    unified=unified,
                    feature_plan=feature_plan,
                    metric_mapping=metric_mapping,
                    ontology_paths=ontology_paths,
                )
                for rule_path in rules_paths:
                    graph = apply_rules(graph=graph, rule_path=rule_path, tokens=tokens)

                ttl_path = participant_out / "ontology.ttl"
                graph.serialize(destination=str(ttl_path), format="turtle")

                inferred = inferred_phenotypes(graph)
                if not inferred.empty:
                    inferred.to_csv(participant_out / "inferred_phenotypes.csv", index=False)
                if self.options.get("trace"):
                    trace = trace_phenotypes(graph)
                    if not trace.empty:
                        trace = _attach_trace_sources(
                            trace=trace,
                            participant_id=participant_id,
                            trace_sources=trace_sources,
                        )
                        trace.to_csv(participant_out / "trace.csv", index=False)
                        if self.options.get("trace_summary", True):
                            max_paths = int(self.options.get("trace_max_paths", 3))
                            summary = summarize_trace(trace, max_paths=max_paths)
                            if not summary.empty:
                                summary.to_csv(participant_out / "trace_summary.csv", index=False)
            if emit_trace_graph:
                trace_graph = _build_trace_graph(
                    participant_id=participant_id,
                    run_id=context.run_id,
                    run_dir=context.workspace_dir,
                    feature_plan_raw=feature_plan_raw,
                    unified=unified,
                    reasoned_trace=trace,
                    inferred=inferred,
                    derived_output_index=derived_output_index,
                    rapids_output_index=rapids_output_index,
                )
                (participant_out / "trace_graph.json").write_text(
                    json.dumps(trace_graph, indent=2),
                    encoding="utf-8",
                )
                trace_view = _build_trace_view(trace_graph)
                (participant_out / "trace_view.json").write_text(
                    json.dumps(trace_view, indent=2),
                    encoding="utf-8",
                )
            processed += 1

        return {"status": "ok", "participants": processed, "output_dir": str(output_dir)}


def _build_trace_source_index(feature_plan: Dict[str, object]) -> Dict[str, Dict[str, str]]:
    sources: Dict[str, Dict[str, str]] = {}
    for feature in feature_plan.get("features", []) or []:
        odim_feature = str(feature.get("odim_feature") or "")
        if not odim_feature:
            continue
        key = odim_feature.split(":", 1)[-1]
        inputs = feature.get("inputs") or []
        if not inputs:
            continue
        inputs_sorted = sorted(inputs, key=lambda item: item.get("priority", 999))
        input_spec = inputs_sorted[0] if inputs_sorted else {}
        path = str(input_spec.get("path") or "")
        value_column = str(input_spec.get("value_column") or feature.get("output_column") or "")
        segment_column = str(input_spec.get("segment_column") or "segment_date")
        source_metric = str(input_spec.get("source_metric") or input_spec.get("metric") or "")
        sources[key] = {
            "path": path,
            "value_column": value_column,
            "segment_column": segment_column,
            "source_metric": source_metric,
        }
    return sources


def _attach_trace_sources(
    *,
    trace: pd.DataFrame,
    participant_id: str,
    trace_sources: Dict[str, Dict[str, str]],
) -> pd.DataFrame:
    if trace.empty:
        return trace
    cache: Dict[str, pd.DataFrame] = {}
    trace = trace.copy()
    trace["source_value"] = ""
    trace["source_column"] = ""
    trace["source_path"] = ""
    trace["source_segment_column"] = ""
    for idx, row in trace.iterrows():
        evidence_class = str(row.get("evidence_class") or "")
        spec = trace_sources.get(evidence_class)
        if not spec:
            continue
        path_template = spec.get("path") or ""
        if not path_template:
            continue
        path = Path(path_template.format(participant_id=participant_id))
        if not path.exists():
            continue
        if str(path) not in cache:
            cache[str(path)] = pd.read_csv(path)
        df = cache[str(path)]
        segment_col = spec.get("segment_column") or "segment_date"
        value_col = spec.get("value_column") or ""
        if segment_col not in df.columns or value_col not in df.columns:
            continue
        segment_date = str(row.get("segment_date") or "")
        subset = df[df[segment_col].astype(str) == segment_date]
        if subset.empty:
            continue
        value = subset.iloc[0][value_col]
        trace.at[idx, "source_value"] = str(value)
        trace.at[idx, "source_column"] = value_col
        trace.at[idx, "source_path"] = str(path)
        trace.at[idx, "source_segment_column"] = segment_col
    return trace


_MEASUREMENT_SOURCE_RE = re.compile(r"^(.*)_[0-9a-fA-F-]{36}_[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_FEATURE_EVIDENCE_RE = re.compile(r"^Feature_(?P<feature>.+)_[0-9a-fA-F-]{36}_[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_ISO_DATE_RE = re.compile(r"(?P<year>20\d{2})-(?P<month>\d{2})-(?P<day>\d{2})")
_COMPACT_DATE_RE = re.compile(r"(?P<year>20\d{2})(?P<month>\d{2})(?P<day>\d{2})")
_DATE_CANDIDATE_COLUMNS = [
    "segment_date",
    "date",
    "local_date",
    "_timestamp",
    "timestamp",
    "file_timestamp",
    "time",
    "value.time",
    "value.endTime",
    "value.timeReceived",
    "start_time",
    "end_time",
    "recorded_time",
    "datetime",
]


def _load_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    if path.name.endswith(".csv.gz"):
        handle_ctx = gzip.open(path, mode="rt", newline="", encoding="utf-8")
    else:
        handle_ctx = path.open(mode="r", newline="", encoding="utf-8")
    with handle_ctx as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _parse_date_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    # Some CONNECT metrics store epoch seconds in value.time/value.endTime.
    try:
        ts = float(text)
        if ts > 10_000_000:  # guard against small non-epoch numeric values
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
    except (ValueError, OverflowError):
        pass
    iso_match = _ISO_DATE_RE.search(text)
    if iso_match:
        return f"{iso_match.group('year')}-{iso_match.group('month')}-{iso_match.group('day')}"
    compact_match = _COMPACT_DATE_RE.search(text)
    if compact_match:
        return f"{compact_match.group('year')}-{compact_match.group('month')}-{compact_match.group('day')}"
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        return parsed.date().isoformat()
    except ValueError:
        return ""


def _parse_timestamp_token(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        ts = float(text)
        if ts > 10_000_000:
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    except (ValueError, OverflowError):
        pass
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_source_metric(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("Measurement_"):
        measurement = text[len("Measurement_") :]
        match = _MEASUREMENT_SOURCE_RE.match(measurement)
        if match:
            return match.group(1)
        return measurement
    return text


def _normalize_ontology_id(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("odim:"):
        return text
    if text.startswith("http://") or text.startswith("https://"):
        if "#" in text:
            return f"odim:{text.rsplit('#', 1)[-1]}"
        return f"odim:{text.rsplit('/', 1)[-1]}"
    if ":" not in text:
        return f"odim:{text}"
    return text


def _resolve_derived_spec_path(context: RunContext, options: Mapping[str, object]) -> Optional[Path]:
    configured = options.get("derived_spec")
    if configured:
        candidate = Path(str(configured).format(run_id=context.run_id)).expanduser()
        if candidate.exists():
            return candidate
    for step in context.spec.processing.steps:
        if step.type != "derived_features":
            continue
        raw = step.options.get("spec")
        if not raw:
            continue
        candidate = Path(str(raw).format(run_id=context.run_id)).expanduser()
        if candidate.exists():
            return candidate
    fallback = context.workspace_dir / "derived_features" / "scanned-derived-features.yaml"
    if fallback.exists():
        return fallback
    return None


def _build_derived_output_index(
    *,
    context: RunContext,
    options: Mapping[str, object],
) -> Dict[str, List[Dict[str, object]]]:
    spec_path = _resolve_derived_spec_path(context, options)
    if not spec_path or not spec_path.exists():
        return {}
    spec_data = yaml.safe_load(spec_path.read_text(encoding="utf-8")) or {}
    for feature in spec_data.get("features") or []:
        if not isinstance(feature, dict):
            continue
        script_raw = str(feature.get("script") or "").strip()
        if not script_raw:
            continue
        script_path = Path(script_raw)
        if script_path.is_absolute():
            continue
        spec_relative = (spec_path.parent / script_path).resolve()
        repo_relative = (Path.cwd() / script_path).resolve()
        if spec_relative.exists():
            feature["script"] = str(spec_relative)
        elif repo_relative.exists():
            feature["script"] = str(repo_relative)
    input_metrics_map: Dict[str, List[str]] = {}
    for feature in spec_data.get("features") or []:
        if not isinstance(feature, Mapping):
            continue
        feature_id = str(feature.get("id") or "").strip()
        if not feature_id:
            continue
        metrics: set[str] = set()
        for input_spec in (feature.get("inputs") or {}).values():
            if not isinstance(input_spec, Mapping):
                continue
            metric = input_spec.get("metric") or input_spec.get("source_metric")
            if metric:
                metrics.add(str(metric))
        if metrics:
            input_metrics_map[feature_id] = sorted(metrics)
    catalog = build_catalog(spec_data, base_dir=spec_path.parent)
    file_index: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for entry in catalog:
        feature_id = str(entry.get("feature_id") or "").strip()
        column = str(entry.get("column") or "").strip()
        file_name = str(entry.get("file") or "").strip()
        if not feature_id or not column or not file_name:
            continue
        output_id = f"derived:{feature_id}:{column}"
        file_index[file_name].append(
            {
                "id": output_id,
                "feature_id": feature_id,
                "column": column,
                "input_metrics": input_metrics_map.get(feature_id, []),
            }
        )
    return dict(file_index)


def _build_rapids_output_index(
    *,
    context: RunContext,
) -> Dict[str, Dict[str, object]]:
    """Map RAPIDS feature column names to canonical output ids + declared inputs."""
    output_index: Dict[str, Dict[str, object]] = {}
    for step in context.spec.processing.steps:
        if step.type != "rapids":
            continue
        options = step.options or {}
        inputs_raw = options.get("inputs") or {}
        features_raw = options.get("features") or {}
        if not isinstance(features_raw, Mapping):
            continue

        sensor_inputs: Dict[str, List[str]] = {}
        if isinstance(inputs_raw, Mapping):
            for sensor, input_spec in inputs_raw.items():
                if not isinstance(input_spec, Mapping):
                    continue
                metrics: set[str] = set()
                for raw in input_spec.values():
                    if isinstance(raw, str):
                        metric = raw.strip()
                    elif isinstance(raw, Mapping):
                        metric = str(raw.get("metric") or raw.get("source_metric") or "").strip()
                    else:
                        metric = ""
                    if metric:
                        metrics.add(metric)
                if metrics:
                    sensor_inputs[str(sensor)] = sorted(metrics)

        for sensor, sensor_spec in features_raw.items():
            sensor_name = str(sensor or "").strip()
            if not sensor_name or not isinstance(sensor_spec, Mapping):
                continue
            providers = sensor_spec.get("providers") or {}
            if not isinstance(providers, Mapping):
                continue
            for provider, provider_spec in providers.items():
                provider_name = str(provider or "").strip() or "RAPIDS"
                provider_payload = provider_spec if isinstance(provider_spec, Mapping) else {}
                group_name = str(provider_payload.get("group") or "BASE").strip() or "BASE"
                feature_names = provider_payload.get("features") or []
                if isinstance(feature_names, (str, bytes)) or not isinstance(feature_names, Iterable):
                    continue
                for raw_name in feature_names:
                    feature_name = str(raw_name or "").strip()
                    if not feature_name:
                        continue
                    column = f"{sensor_name.lower()}_{provider_name.lower()}_{feature_name.lower()}"
                    output_id = f"rapids:{sensor_name}:{provider_name}:{group_name}:{feature_name}"
                    output_index[column] = {
                        "id": output_id,
                        "input_metrics": sensor_inputs.get(sensor_name, []),
                    }
    return output_index


def _build_rapids_source_lookup(
    rapids_output_index: Mapping[str, Dict[str, object]],
) -> Dict[Tuple[str, str], str]:
    """Map (sensor, feature) to canonical RAPIDS output id when unambiguous."""
    resolved: Dict[Tuple[str, str], str] = {}
    conflicts: set[Tuple[str, str]] = set()
    for payload in rapids_output_index.values():
        output_id = str(payload.get("id") or "").strip()
        parts = output_id.split(":")
        if len(parts) < 5 or parts[0] != "rapids":
            continue
        key = (parts[1].lower(), parts[4].lower())
        existing = resolved.get(key)
        if existing and existing != output_id:
            conflicts.add(key)
            continue
        resolved[key] = output_id
    for key in conflicts:
        resolved.pop(key, None)
    return resolved


def _canonicalize_rapids_source_metric(
    source_metric: str,
    rapids_source_lookup: Mapping[Tuple[str, str], str],
) -> str:
    text = str(source_metric or "").strip()
    if not text.startswith("rapids:"):
        return text
    parts = text.split(":")
    # Canonical id already: rapids:<sensor>:<provider>:<group>:<feature>
    if len(parts) >= 5:
        return text
    # Short source emitted by unify_rapids_reduce: rapids:<sensor>:<feature>[:label]
    if len(parts) >= 3:
        sensor = parts[1].strip().lower()
        feature = parts[2].strip().lower()
        if sensor and feature:
            canonical = rapids_source_lookup.get((sensor, feature))
            if canonical:
                return canonical
    return text


def _metric_files_for_participant(run_dir: Path, participant_id: str) -> Dict[str, List[Path]]:
    merged_dir = run_dir / "merged"
    files_by_metric: Dict[str, List[Path]] = {}
    if not merged_dir.exists():
        return files_by_metric
    for site_dir in merged_dir.iterdir():
        if not site_dir.is_dir():
            continue
        participant_dir = site_dir / participant_id
        if not participant_dir.exists():
            continue
        for metric_dir in participant_dir.iterdir():
            if not metric_dir.is_dir():
                continue
            metric_files = [
                path
                for path in metric_dir.iterdir()
                if path.is_file() and (path.suffix == ".csv" or path.name.endswith(".csv.gz"))
            ]
            if metric_files:
                files_by_metric.setdefault(metric_dir.name, []).extend(sorted(metric_files))
    return files_by_metric


def _metric_observation_index(
    run_dir: Path,
    participant_id: str,
) -> Tuple[Dict[str, set[str]], Dict[str, Dict[str, object]]]:
    metric_days: Dict[str, set[str]] = {}
    metric_evidence: Dict[str, Dict[str, object]] = {}
    files_by_metric = _metric_files_for_participant(run_dir, participant_id)
    for metric, files in files_by_metric.items():
        days: set[str] = set()
        columns: set[str] = set()
        first_ts: Optional[datetime] = None
        last_ts: Optional[datetime] = None
        row_count = 0
        file_count = 0
        for path in files:
            file_count += 1
            file_date = _parse_date_token(path.name)
            if file_date:
                days.add(file_date)
            if path.name.endswith(".csv.gz"):
                handle_ctx = gzip.open(path, mode="rt", newline="", encoding="utf-8")
            else:
                handle_ctx = path.open(mode="r", newline="", encoding="utf-8")
            with handle_ctx as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames:
                    columns.update([name for name in reader.fieldnames if name])
                for row in reader:
                    row_count += 1
                    parsed_ts: Optional[datetime] = None
                    for column in _DATE_CANDIDATE_COLUMNS:
                        value = row.get(column)
                        parsed_ts = _parse_timestamp_token(value)
                        if parsed_ts:
                            day = parsed_ts.date().isoformat()
                            days.add(day)
                            if first_ts is None or parsed_ts < first_ts:
                                first_ts = parsed_ts
                            if last_ts is None or parsed_ts > last_ts:
                                last_ts = parsed_ts
                            break
                    if parsed_ts is not None:
                        continue
                    for column in _DATE_CANDIDATE_COLUMNS:
                        parsed_day = _parse_date_token(row.get(column))
                        if parsed_day:
                            days.add(parsed_day)
                            break
        metric_days[metric] = days
        metric_evidence[metric] = {
            "kind": "raw_metric",
            "row_count": row_count,
            "file_count": file_count,
            "column_count": len(columns),
            "first_timestamp": first_ts.isoformat() if first_ts else "",
            "last_timestamp": last_ts.isoformat() if last_ts else "",
        }
    return metric_days, metric_evidence


def _default_source_metric(feature: Mapping[str, Any]) -> str:
    for raw in feature.get("inputs") or []:
        if not isinstance(raw, Mapping):
            continue
        source_metric = raw.get("source_metric") or raw.get("metric")
        if source_metric:
            return str(source_metric)
    return ""


def _select_observed_source_metrics(
    *,
    input_metrics: Iterable[str],
    segment_date: str,
    metric_days: Mapping[str, set[str]],
) -> List[str]:
    metrics = sorted({str(metric) for metric in input_metrics if str(metric).strip()})
    if not metrics:
        return []
    if segment_date:
        on_date = [metric for metric in metrics if segment_date in metric_days.get(metric, set())]
        if on_date:
            return on_date
    return [metric for metric in metrics if metric_days.get(metric)]


def _resolve_derived_output_defs(
    derived_output_index: Mapping[str, List[Dict[str, object]]],
    file_name: str,
) -> List[Dict[str, object]]:
    defs = list(derived_output_index.get(file_name) or [])
    if defs:
        return defs
    if file_name.endswith(".csv.gz"):
        return list(derived_output_index.get(file_name[:-3]) or [])
    if file_name.endswith(".csv"):
        return list(derived_output_index.get(f"{file_name}.gz") or [])
    return []


def _feature_id_from_evidence_id(evidence_id: str) -> str:
    text = str(evidence_id or "").strip()
    if not text:
        return ""
    token = text.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
    match = _FEATURE_EVIDENCE_RE.match(token)
    if match:
        return match.group("feature")
    return ""


def _node_type(
    node_id: str,
    *,
    phenotype_nodes: set[str],
    unified_ids: set[str],
    derived_output_ids: set[str],
) -> str:
    if node_id in phenotype_nodes:
        return "phenotype"
    if node_id in unified_ids:
        return "feature"
    if node_id in derived_output_ids:
        return "derived"
    if node_id.startswith("derived:"):
        return "derived"
    if node_id.startswith("rapids:"):
        return "rapids"
    return "metric"


def _build_trace_graph(
    *,
    participant_id: str,
    run_id: str,
    run_dir: Path,
    feature_plan_raw: Mapping[str, object],
    unified: pd.DataFrame,
    reasoned_trace: pd.DataFrame,
    inferred: pd.DataFrame,
    derived_output_index: Mapping[str, List[Dict[str, object]]],
    rapids_output_index: Mapping[str, Dict[str, object]],
) -> Dict[str, object]:
    # Graph is data-first: only observed derivations become edges.
    node_dates: Dict[str, set[str]] = defaultdict(set)
    node_value_counts: Dict[str, int] = defaultdict(int)
    edge_dates: Dict[Tuple[str, str], set[str]] = defaultdict(set)
    edge_counts: Dict[Tuple[str, str], int] = defaultdict(int)
    edge_rules: Dict[Tuple[str, str], set[str]] = defaultdict(set)
    rapids_source_lookup = _build_rapids_source_lookup(rapids_output_index)

    def add_node(node_id: str, *, date: str = "", has_value: bool = False) -> None:
        if not node_id:
            return
        if date:
            node_dates[node_id].add(date)
        if has_value:
            node_value_counts[node_id] += 1

    def add_edge(source: str, target: str, *, date: str = "", rule: str = "") -> None:
        source_id = _normalize_source_metric(source)
        target_id = _normalize_source_metric(target)
        if not source_id or not target_id:
            return
        add_node(source_id, date=date)
        add_node(target_id, date=date)
        key = (source_id, target_id)
        edge_counts[key] += 1
        if date:
            edge_dates[key].add(date)
        if rule:
            edge_rules[key].add(rule)

    features = [raw for raw in (feature_plan_raw.get("features") or []) if isinstance(raw, Mapping)]
    feature_ids = {str(feature.get("id") or "").strip() for feature in features if str(feature.get("id") or "").strip()}
    default_sources = {str(feature.get("id") or ""): _default_source_metric(feature) for feature in features}
    derived_to_unified: Dict[str, set[str]] = defaultdict(set)
    for feature in features:
        target_id = str(feature.get("id") or "").strip()
        if not target_id:
            continue
        for input_spec in feature.get("inputs") or []:
            if not isinstance(input_spec, Mapping):
                continue
            source_metric = str(input_spec.get("source_metric") or input_spec.get("metric") or "").strip()
            if source_metric.startswith("derived:"):
                derived_to_unified[source_metric].add(target_id)

    metric_days, metric_evidence = _metric_observation_index(run_dir, participant_id)
    for metric, days in metric_days.items():
        if days:
            for day in days:
                add_node(metric, date=day, has_value=True)

    rapids_root = run_dir / "rapids"
    if rapids_root.exists() and rapids_output_index:
        for platform_dir in sorted(rapids_root.iterdir()):
            if not platform_dir.is_dir():
                continue
            features_dir = platform_dir / "data" / "processed" / "features" / participant_id
            if not features_dir.exists():
                continue
            for file_path in sorted(features_dir.iterdir()):
                if not file_path.is_file() or file_path.suffix != ".csv":
                    continue
                rows = _load_csv_rows(file_path)
                if not rows:
                    continue
                file_columns = set(rows[0].keys()) if rows else set()
                relevant_columns = [column for column in rapids_output_index.keys() if column in file_columns]
                if not relevant_columns:
                    continue
                for row in rows:
                    segment_date = _parse_date_token(
                        row.get("segment_date")
                        or row.get("date")
                        or row.get("local_date")
                        or row.get("local_segment_start_datetime")
                        or row.get("local_segment_end_datetime")
                        or row.get("_timestamp")
                    )
                    for column in relevant_columns:
                        output_spec = rapids_output_index[column]
                        value = str(row.get(column) or "").strip()
                        if not value:
                            continue
                        output_id = str(output_spec.get("id") or "").strip()
                        if not output_id:
                            continue
                        add_node(output_id, date=segment_date, has_value=True)
                        input_metrics = [str(metric) for metric in (output_spec.get("input_metrics") or []) if str(metric).strip()]
                        sources = _select_observed_source_metrics(
                            input_metrics=input_metrics,
                            segment_date=segment_date,
                            metric_days=metric_days,
                        )
                        for source_metric in sources:
                            add_edge(source_metric, output_id, date=segment_date)

    if not unified.empty:
        for _, row in unified.iterrows():
            segment_date = _parse_date_token(row.get("segment_date")) or ""
            for feature in features:
                feature_id = str(feature.get("id") or "").strip()
                output_col = str(feature.get("output_column") or feature_id).strip()
                if not feature_id or not output_col:
                    continue
                raw_value = row.get(output_col)
                if pd.isna(raw_value):
                    continue
                value = str(raw_value).strip()
                if not value:
                    continue
                add_node(feature_id, date=segment_date, has_value=True)
                source_metric = str(row.get(f"{feature_id}_source") or "").strip() or default_sources.get(feature_id, "")
                if source_metric:
                    normalized_source = _normalize_source_metric(source_metric)
                    normalized_source = _canonicalize_rapids_source_metric(
                        normalized_source,
                        rapids_source_lookup,
                    )
                    # `derived:<feature>` is a feature-level alias from unification source fields,
                    # not a concrete value node. Concrete derived outputs are linked below from
                    # observed derived rows as `derived:<feature>:<column>`.
                    if normalized_source.startswith("derived:") and normalized_source.count(":") == 1:
                        continue
                    add_edge(normalized_source, feature_id, date=segment_date)

    derived_root = run_dir / "derived_features" / participant_id
    derived_output_ids: set[str] = set()
    if derived_root.exists():
        for file_path in sorted(derived_root.iterdir()):
            if not file_path.is_file():
                continue
            if file_path.suffix != ".csv" and not file_path.name.endswith(".csv.gz"):
                continue
            output_defs = _resolve_derived_output_defs(derived_output_index, file_path.name)
            if not output_defs:
                continue
            rows = _load_csv_rows(file_path)
            if not rows:
                continue
            for raw in output_defs:
                derived_output_ids.add(str(raw.get("id") or ""))
            for row in rows:
                segment_date = _parse_date_token(
                    row.get("segment_date") or row.get("date") or row.get("local_date") or row.get("_timestamp")
                )
                for output_def in output_defs:
                    output_id = str(output_def.get("id") or "").strip()
                    column = str(output_def.get("column") or "").strip()
                    if not output_id or not column or column not in row:
                        continue
                    value = str(row.get(column) or "").strip()
                    if not value:
                        continue
                    add_node(output_id, date=segment_date, has_value=True)
                    input_metrics = [str(metric) for metric in (output_def.get("input_metrics") or []) if str(metric).strip()]
                    sources = _select_observed_source_metrics(
                        input_metrics=input_metrics,
                        segment_date=segment_date,
                        metric_days=metric_days,
                    )
                    for source_metric in sources:
                        add_edge(source_metric, output_id, date=segment_date)
                    # Connect observed derived outputs to unified features that use this derived source.
                    source_key = f"derived:{str(output_def.get('feature_id') or '').strip()}"
                    for unified_feature_id in sorted(derived_to_unified.get(source_key, set())):
                        add_edge(output_id, unified_feature_id, date=segment_date)

    phenotype_nodes: set[str] = set()
    if not inferred.empty:
        for _, row in inferred.iterrows():
            phenotype = _normalize_ontology_id(str(row.get("phenotype") or ""))
            if not phenotype:
                continue
            segment_date = _parse_date_token(row.get("segment_date")) or ""
            add_node(phenotype, date=segment_date, has_value=True)
            phenotype_nodes.add(phenotype)

    if not reasoned_trace.empty:
        for _, row in reasoned_trace.iterrows():
            segment_date = _parse_date_token(row.get("segment_date")) or ""
            rule = str(row.get("rule") or "").strip()
            phenotype = _normalize_ontology_id(str(row.get("phenotype") or ""))
            evidence_node = _feature_id_from_evidence_id(str(row.get("evidence_id") or ""))
            if not evidence_node:
                evidence_node = str(row.get("feature_id") or "").strip()
            if not evidence_node:
                evidence_node = str(row.get("evidence_class") or "").strip()
                if evidence_node:
                    evidence_node = _normalize_ontology_id(evidence_node)
            source_metric = _normalize_source_metric(str(row.get("source_metric") or ""))
            if phenotype:
                phenotype_nodes.add(phenotype)
                add_node(phenotype, date=segment_date)
            if evidence_node:
                add_node(evidence_node, date=segment_date, has_value=True)
            if evidence_node and phenotype:
                add_edge(evidence_node, phenotype, date=segment_date, rule=rule)
            if source_metric and evidence_node:
                add_edge(source_metric, evidence_node, date=segment_date, rule=rule)

    node_ids = sorted(node_dates.keys() | node_value_counts.keys())
    nodes = []
    for node_id in node_ids:
        node_type = _node_type(
            node_id,
            phenotype_nodes=phenotype_nodes,
            unified_ids=feature_ids,
            derived_output_ids=derived_output_ids,
        )
        payload: Dict[str, object] = {
            "id": node_id,
            "type": node_type,
            "date_count": len(node_dates.get(node_id, set())),
            "dates": sorted(node_dates.get(node_id, set())),
            "value_count": int(node_value_counts.get(node_id, 0)),
        }
        if node_type == "metric":
            evidence = metric_evidence.get(node_id)
            if evidence:
                payload["evidence"] = evidence
        nodes.append(payload)
    edges = []
    for (source, target), count in sorted(edge_counts.items()):
        edges.append(
            {
                "source": source,
                "target": target,
                "kind": "observed",
                "count": int(count),
                "dates": sorted(edge_dates.get((source, target), set())),
                "rules": sorted(edge_rules.get((source, target), set())),
            }
        )

    all_dates = sorted({date for dates in node_dates.values() for date in dates if date})
    return {
        "version": "trace-graph/v1",
        "run_id": run_id,
        "participant_id": participant_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "nodes": nodes,
        "edges": edges,
        "dates": all_dates,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }


def _build_trace_view(trace_graph: Mapping[str, object]) -> Dict[str, object]:
    nodes_raw = trace_graph.get("nodes") or []
    edges_raw = trace_graph.get("edges") or []
    nodes_by_id: Dict[str, Dict[str, object]] = {}
    children_map: Dict[str, set[str]] = defaultdict(set)
    for raw in nodes_raw:
        if not isinstance(raw, Mapping):
            continue
        node_id = str(raw.get("id") or "").strip()
        if not node_id:
            continue
        payload: Dict[str, object] = {
            "id": node_id,
            "label": node_id,
            "type": str(raw.get("type") or "metric"),
            "date_count": int(raw.get("date_count") or 0),
            "dates": [str(day) for day in (raw.get("dates") or []) if str(day).strip()],
            "value_count": int(raw.get("value_count") or 0),
        }
        evidence_raw = raw.get("evidence")
        if isinstance(evidence_raw, Mapping):
            payload["evidence"] = {
                "kind": str(evidence_raw.get("kind") or "raw_metric"),
                "row_count": int(evidence_raw.get("row_count") or 0),
                "file_count": int(evidence_raw.get("file_count") or 0),
                "column_count": int(evidence_raw.get("column_count") or 0),
                "first_timestamp": str(evidence_raw.get("first_timestamp") or ""),
                "last_timestamp": str(evidence_raw.get("last_timestamp") or ""),
            }
        nodes_by_id[node_id] = payload
    for raw in edges_raw:
        if not isinstance(raw, Mapping):
            continue
        source = str(raw.get("source") or "").strip()
        target = str(raw.get("target") or "").strip()
        if not source or not target:
            continue
        if target in nodes_by_id and source in nodes_by_id:
            children_map[target].add(source)

    memo_desc: Dict[str, set[str]] = {}
    visiting_desc: set[str] = set()

    def descendants(node_id: str) -> set[str]:
        cached = memo_desc.get(node_id)
        if cached is not None:
            return cached
        if node_id in visiting_desc:
            return set()
        visiting_desc.add(node_id)
        result: set[str] = set()
        for child_id in children_map.get(node_id, set()):
            result.add(child_id)
            result.update(descendants(child_id))
        visiting_desc.remove(node_id)
        memo_desc[node_id] = result
        return result

    memo_dates: Dict[str, set[str]] = {}
    visiting_dates: set[str] = set()

    def closure_dates(node_id: str) -> set[str]:
        cached = memo_dates.get(node_id)
        if cached is not None:
            return cached
        if node_id in visiting_dates:
            return set(nodes_by_id.get(node_id, {}).get("dates") or [])
        visiting_dates.add(node_id)
        result = set(nodes_by_id.get(node_id, {}).get("dates") or [])
        for child_id in children_map.get(node_id, set()):
            result.update(closure_dates(child_id))
        visiting_dates.remove(node_id)
        memo_dates[node_id] = result
        return result

    for node_id, payload in nodes_by_id.items():
        payload["children"] = sorted(children_map.get(node_id, set()))
        payload["descendants"] = sorted(descendants(node_id))
        payload["closure_dates"] = sorted(closure_dates(node_id))

    categories = [
        ("phenotype", "phenotypes", "Phenotypes"),
        ("feature", "features", "Features"),
        ("derived", "derived", "Derived Outputs"),
        ("metric", "metrics", "Metrics"),
        ("rapids", "rapids", "RAPIDS Outputs"),
    ]
    category_rows = []
    for node_type, category_id, title in categories:
        roots = sorted(
            [
                node_id
                for node_id, node in nodes_by_id.items()
                if str(node.get("type") or "") == node_type and int(node.get("value_count") or 0) > 0
            ]
        )
        category_rows.append(
            {
                "id": category_id,
                "title": title,
                "roots": roots,
                "count": len(roots),
            }
        )

    return {
        "version": "trace-view/v1",
        "run_id": str(trace_graph.get("run_id") or ""),
        "participant_id": str(trace_graph.get("participant_id") or ""),
        "generated_at": str(trace_graph.get("generated_at") or ""),
        "dates": [str(day) for day in (trace_graph.get("dates") or []) if str(day).strip()],
        "node_count": len(nodes_by_id),
        "categories": category_rows,
        "nodes": nodes_by_id,
    }


__all__ = ["OntologyReasonStep"]
