"""Executable package-boundary checks for the MHM pipeline rehearsal."""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class PackageImportContract:
    """Import contract for one rehearsed package surface."""

    name: str
    roots: tuple[Path, ...]
    forbidden_prefixes: tuple[str, ...] = field(default_factory=tuple)
    lazy_only_prefixes: tuple[str, ...] = field(default_factory=tuple)
    lazy_modules: tuple[Path, ...] = field(default_factory=tuple)
    excluded_patterns: tuple[str, ...] = field(default_factory=tuple)


def minimal_pipeline_contract(repo_root: str | Path = ".") -> PackageImportContract:
    root = Path(repo_root)
    return PackageImportContract(
        name="mhm-pipelines:minimal",
        roots=(root / "mhm_core" / "pipeline", root / "mhm_core" / "profiles" / "minimal"),
        forbidden_prefixes=(
            "connect_summary",
            "pandas",
            "rdflib",
            "fastapi",
            "uvicorn",
        ),
        lazy_only_prefixes=("boto3", "botocore"),
        lazy_modules=(
            root / "mhm_core" / "pipeline" / "object_store.py",
            root / "mhm_core" / "pipeline" / "backends" / "s3.py",
        ),
        excluded_patterns=(
            "*/mhm_core/pipeline/derived_features_runner.py",
            "*/mhm_core/pipeline/integrations/*.py",
            "*/mhm_core/pipeline/steps/combine_features.py",
            "*/mhm_core/pipeline/steps/derived_features.py",
            "*/mhm_core/pipeline/steps/ontology_*.py",
        ),
    )


def check_import_contract(contract: PackageImportContract) -> list[str]:
    """Return import-boundary violations for a package contract."""

    violations: list[str] = []
    lazy_modules = {path.resolve() for path in contract.lazy_modules}
    for path in _python_files(contract.roots, excluded_patterns=contract.excluded_patterns):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            violations.append(f"{path}:{exc.lineno}: syntax error: {exc.msg}")
            continue
        for module_name, line_no in _imports(tree):
            if _matches_prefix(module_name, contract.forbidden_prefixes):
                violations.append(f"{path}:{line_no}: forbidden import for {contract.name}: {module_name}")
            if _matches_prefix(module_name, contract.lazy_only_prefixes) and path.resolve() not in lazy_modules:
                violations.append(f"{path}:{line_no}: eager optional import for {contract.name}: {module_name}")
    return violations


def _python_files(roots: Iterable[Path], *, excluded_patterns: tuple[str, ...] = ()) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        if root.is_file() and root.suffix == ".py":
            files.append(root)
        elif root.exists():
            files.extend(path for path in root.rglob("*.py") if path.is_file())
    return sorted(
        path
        for path in files
        if not any(fnmatch(path.as_posix(), pattern) for pattern in excluded_patterns)
    )


def _imports(tree: ast.AST) -> list[tuple[str, int]]:
    imports: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend((alias.name, node.lineno) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append((node.module or "", node.lineno))
    return imports


def _matches_prefix(module_name: str, prefixes: tuple[str, ...]) -> bool:
    return any(module_name == prefix or module_name.startswith(prefix + ".") for prefix in prefixes)


__all__ = ["PackageImportContract", "check_import_contract", "minimal_pipeline_contract"]
