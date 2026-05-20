"""Production-spec parity comparison helpers.

The refactor parity invariant is that an unchanged CONNECT production spec can
run through the current CONNECT entrypoint and the refactored implementation
with equivalent materialized outputs. This module compares two already
materialized run/output directories and normalizes known volatile metadata.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from fnmatch import fnmatch
import gzip
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

DEFAULT_IGNORE_PATTERNS = (
    ".DS_Store",
    "logs/pipeline.log",
)

DEFAULT_VOLATILE_KEYS = {
    "built_at",
    "completed_at",
    "created_at",
    "duration_seconds",
    "elapsed_seconds",
    "generated_at",
    "last_run_id",
    "modified_at",
    "run_id",
    "started_at",
    "start_time",
    "timestamp",
    "updated_at",
}

PROVENANCE_DERIVED_KEYS = {
    "dataset_id",
    "document_hash",
    "operation_id",
}

PROVENANCE_DOCUMENT_NAMES = {
    "artifact_inventory.jsonl",
    "coverage_summary.json",
    "dataset_manifest.json",
    "history.jsonl",
    "observed_state.json",
    "operation_event.json",
    "pipeline_spec_manifest.json",
}


@dataclass(frozen=True)
class ParityNormalization:
    """Explicit optional normalization for old/new parity comparisons."""

    value_replacements: tuple[tuple[str, str], ...] = ()
    provenance_refactor: bool = False

    @classmethod
    def from_pairs(
        cls,
        pairs: Sequence[tuple[str, str]] = (),
        *,
        provenance_refactor: bool = False,
    ) -> "ParityNormalization":
        replacements: list[tuple[str, str]] = []
        for index, (old_value, new_value) in enumerate(pairs, start=1):
            placeholder = f"<NORMALIZED:{index}>"
            if old_value:
                replacements.append((old_value, placeholder))
            if new_value and new_value != old_value:
                replacements.append((new_value, placeholder))
        return cls(value_replacements=tuple(replacements), provenance_refactor=provenance_refactor)

    def normalize_string(self, value: str) -> str:
        normalized = value
        replacements = sorted(
            self.value_replacements,
            key=lambda item: len(item[0]),
            reverse=True,
        )
        for needle, replacement in replacements:
            normalized = normalized.replace(needle, replacement)
        return normalized


@dataclass(frozen=True)
class ParityChangedFile:
    path: str
    old_digest: str
    new_digest: str
    kind: str


@dataclass
class PipelineParityReport:
    old_root: str
    new_root: str
    matched: list[str] = field(default_factory=list)
    changed: list[ParityChangedFile] = field(default_factory=list)
    missing_from_new: list[str] = field(default_factory=list)
    missing_from_old: list[str] = field(default_factory=list)
    ignored: list[str] = field(default_factory=list)

    @property
    def equivalent(self) -> bool:
        return not self.changed and not self.missing_from_new and not self.missing_from_old

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["equivalent"] = self.equivalent
        return payload


def compare_run_directories(
    old_root: str | Path,
    new_root: str | Path,
    *,
    ignore_patterns: Sequence[str] = DEFAULT_IGNORE_PATTERNS,
    volatile_keys: Iterable[str] = DEFAULT_VOLATILE_KEYS,
    normalization: ParityNormalization | None = None,
) -> PipelineParityReport:
    """Compare two materialized run/output directories."""

    old_path = Path(old_root).expanduser().resolve()
    new_path = Path(new_root).expanduser().resolve()
    volatile_key_set = {str(key) for key in volatile_keys}
    normalization = normalization or ParityNormalization()
    old_files = _file_index(old_path)
    new_files = _file_index(new_path)
    report = PipelineParityReport(old_root=str(old_path), new_root=str(new_path))

    all_paths = sorted(set(old_files) | set(new_files))
    for rel_path in all_paths:
        if _ignored(rel_path, ignore_patterns):
            report.ignored.append(rel_path)
            continue
        old_file = old_files.get(rel_path)
        new_file = new_files.get(rel_path)
        if old_file is None:
            report.missing_from_old.append(rel_path)
            continue
        if new_file is None:
            report.missing_from_new.append(rel_path)
            continue
        old_digest, old_kind = parity_digest(
            old_file,
            root=old_path,
            volatile_keys=volatile_key_set,
            normalization=normalization,
        )
        new_digest, new_kind = parity_digest(
            new_file,
            root=new_path,
            volatile_keys=volatile_key_set,
            normalization=normalization,
        )
        if old_digest == new_digest and old_kind == new_kind:
            report.matched.append(rel_path)
        else:
            report.changed.append(
                ParityChangedFile(
                    path=rel_path,
                    old_digest=old_digest,
                    new_digest=new_digest,
                    kind=old_kind if old_kind == new_kind else f"{old_kind}->{new_kind}",
                )
            )

    return report


def parity_digest(
    path: Path,
    *,
    root: Path,
    volatile_keys: Iterable[str] = DEFAULT_VOLATILE_KEYS,
    normalization: ParityNormalization | None = None,
) -> tuple[str, str]:
    """Return a parity digest and content kind for one file."""

    suffixes = [suffix.lower() for suffix in path.suffixes]
    normalization = normalization or ParityNormalization()
    rel_path = _relative_path(path, root)
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        normalized = _normalize_value(
            payload,
            root=root,
            volatile_keys=set(volatile_keys),
            normalization=normalization,
            rel_path=rel_path,
        )
        body = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return sha256(body).hexdigest(), "json"
    if path.suffix.lower() == ".jsonl":
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rows.append(
                _normalize_value(
                    json.loads(line),
                    root=root,
                    volatile_keys=set(volatile_keys),
                    normalization=normalization,
                    rel_path=rel_path,
                )
            )
        body = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return sha256(body).hexdigest(), "jsonl"
    if ".gz" in suffixes:
        with gzip.open(path, "rb") as handle:
            return sha256(handle.read()).hexdigest(), "gzip"
    if ".gz" not in suffixes and _looks_like_utf8(path):
        text = path.read_text(encoding="utf-8")
        text = text.replace(str(root), "<RUN_ROOT>")
        text = normalization.normalize_string(text)
        return sha256(text.encode("utf-8")).hexdigest(), "text"
    return sha256(path.read_bytes()).hexdigest(), "binary"


def write_report(report: PipelineParityReport, path: str | Path) -> None:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


def _file_index(root: Path) -> dict[str, Path]:
    if not root.exists():
        raise FileNotFoundError(root)
    return {
        path.relative_to(root).as_posix(): path
        for path in root.rglob("*")
        if path.is_file()
    }


def _ignored(rel_path: str, ignore_patterns: Sequence[str]) -> bool:
    return any(fnmatch(rel_path, pattern) for pattern in ignore_patterns)


def _normalize_value(
    value: Any,
    *,
    root: Path,
    volatile_keys: set[str],
    normalization: ParityNormalization,
    rel_path: str,
) -> Any:
    if isinstance(value, Mapping):
        output: dict[str, Any] = {}
        for key, child in sorted(value.items(), key=lambda item: str(item[0])):
            text_key = str(key)
            if _drop_provenance_refactor_alias(text_key, value, normalization):
                continue
            if text_key in volatile_keys or _normalize_provenance_derived_field(text_key, rel_path, normalization):
                output[text_key] = "<VOLATILE>"
            else:
                output[text_key] = _normalize_value(
                    child,
                    root=root,
                    volatile_keys=volatile_keys,
                    normalization=normalization,
                    rel_path=rel_path,
                )
        return output
    if isinstance(value, list):
        return [
            _normalize_value(
                item,
                root=root,
                volatile_keys=volatile_keys,
                normalization=normalization,
                rel_path=rel_path,
            )
            for item in value
        ]
    if isinstance(value, str):
        normalized = value.replace(str(root), "<RUN_ROOT>")
        return normalization.normalize_string(normalized)
    return value


def _drop_provenance_refactor_alias(
    key: str,
    mapping: Mapping[str, Any],
    normalization: ParityNormalization,
) -> bool:
    if not normalization.provenance_refactor:
        return False
    if key == "coordinates":
        return _is_redundant_coordinates(mapping.get(key), mapping)
    if key == "labels":
        return _is_redundant_labels(mapping.get(key), mapping)
    if key == "locator":
        return _is_redundant_source_locator(mapping.get(key), mapping)
    if key == "entity_group_map":
        return mapping.get(key) == {}
    legacy_partner = {
        "entity_count": "participant_count",
        "group_count": "site_count",
        "group_summary": "site_summary",
        "entities": "participants",
        "groups": "sites",
        "entity_id": "participant_id",
        "group": "site",
    }.get(key)
    return bool(legacy_partner and legacy_partner in mapping)


def _is_redundant_coordinates(value: Any, parent: Mapping[str, Any]) -> bool:
    if not isinstance(value, Mapping):
        return False
    allowed_keys = {"group", "entity_id", "stream"}
    if set(str(key) for key in value) - allowed_keys:
        return False
    return all(
        _mapping_value_matches_parent(value, key, parent)
        for key in allowed_keys
        if key in value
    )


def _is_redundant_labels(value: Any, parent: Mapping[str, Any]) -> bool:
    if not isinstance(value, Mapping):
        return False
    allowed_keys = {"group", "site", "entity_id", "participant_id", "stream"}
    if set(str(key) for key in value) - allowed_keys:
        return False
    return all(
        _mapping_value_matches_parent(value, key, parent)
        for key in allowed_keys
        if key in value
    )


def _is_redundant_source_locator(value: Any, parent: Mapping[str, Any]) -> bool:
    if not isinstance(value, str):
        return False
    bucket = parent.get("bucket")
    prefix = parent.get("prefix")
    if not bucket or prefix is None:
        return False
    expected = f"s3://{bucket}/{str(prefix).strip('/')}"
    return value.rstrip("/") == expected.rstrip("/")


def _mapping_value_matches_parent(value: Mapping[str, Any], key: str, parent: Mapping[str, Any]) -> bool:
    text_key = str(key)
    candidate = value.get(text_key)
    parent_keys = {
        "group": ("group", "site"),
        "site": ("site", "group"),
        "entity_id": ("entity_id", "participant_id"),
        "participant_id": ("participant_id", "entity_id"),
        "stream": ("stream",),
    }.get(text_key, (text_key,))
    return any(str(candidate) == str(parent[parent_key]) for parent_key in parent_keys if parent_key in parent)


def _normalize_provenance_derived_field(
    key: str,
    rel_path: str,
    normalization: ParityNormalization,
) -> bool:
    if not normalization.provenance_refactor or not _is_provenance_document(rel_path):
        return False
    if key in PROVENANCE_DERIVED_KEYS:
        return True
    return key.endswith("_hash") or key.endswith("_sha256")


def _is_provenance_document(rel_path: str) -> bool:
    path = rel_path.replace("\\", "/")
    if "/provenance/" in f"/{path}/":
        return True
    return Path(path).name in PROVENANCE_DOCUMENT_NAMES


def _relative_path(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.name


def _looks_like_utf8(path: Path) -> bool:
    try:
        path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return False
    return True


__all__ = [
    "DEFAULT_IGNORE_PATTERNS",
    "DEFAULT_VOLATILE_KEYS",
    "PROVENANCE_DERIVED_KEYS",
    "PROVENANCE_DOCUMENT_NAMES",
    "ParityNormalization",
    "ParityChangedFile",
    "PipelineParityReport",
    "compare_run_directories",
    "parity_digest",
    "write_report",
]
