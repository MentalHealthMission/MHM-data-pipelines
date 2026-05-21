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
    classification: str = "unresolved"
    blocking: bool = True
    reason: str = ""


@dataclass
class PipelineParityReport:
    old_root: str
    new_root: str
    matched: list[str] = field(default_factory=list)
    matched_count: int = 0
    changed: list[ParityChangedFile] = field(default_factory=list)
    missing_from_new: list[str] = field(default_factory=list)
    missing_from_old: list[str] = field(default_factory=list)
    ignored: list[str] = field(default_factory=list)

    @property
    def equivalent(self) -> bool:
        return not self.changed and not self.missing_from_new and not self.missing_from_old

    @property
    def blocking_equivalent(self) -> bool:
        return (
            not self.missing_from_new
            and not self.missing_from_old
            and not any(item.blocking for item in self.changed)
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["matched_count"] = self.matched_count
        payload["changed_count"] = len(self.changed)
        payload["accepted_changed_count"] = len([item for item in self.changed if not item.blocking])
        payload["blocking_changed_count"] = len([item for item in self.changed if item.blocking])
        payload["changed_count_by_classification"] = _changed_count_by_classification(self.changed)
        payload["equivalent"] = self.equivalent
        payload["strict_equivalent"] = self.equivalent
        payload["blocking_equivalent"] = self.blocking_equivalent
        return payload


def compare_run_directories(
    old_root: str | Path,
    new_root: str | Path,
    *,
    ignore_patterns: Sequence[str] = DEFAULT_IGNORE_PATTERNS,
    volatile_keys: Iterable[str] = DEFAULT_VOLATILE_KEYS,
    normalization: ParityNormalization | None = None,
    record_matched_paths: bool = True,
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
            report.matched_count += 1
            if record_matched_paths:
                report.matched.append(rel_path)
        else:
            classification, blocking, reason = _classify_changed_file(
                rel_path,
                old_file,
                new_file,
                old_root=old_path,
                new_root=new_path,
                volatile_keys=volatile_key_set,
                normalization=normalization,
            )
            report.changed.append(
                ParityChangedFile(
                    path=rel_path,
                    old_digest=old_digest,
                    new_digest=new_digest,
                    kind=old_kind if old_kind == new_kind else f"{old_kind}->{new_kind}",
                    classification=classification,
                    blocking=blocking,
                    reason=reason,
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
        digest = sha256()
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            normalized_line = json.dumps(
                _normalize_value(
                    json.loads(line),
                    root=root,
                    volatile_keys=set(volatile_keys),
                    normalization=normalization,
                    rel_path=rel_path,
                ),
                sort_keys=True,
                separators=(",", ":"),
            )
            digest.update(normalized_line.encode("utf-8"))
            digest.update(b"\n")
        return digest.hexdigest(), "jsonl"
    if ".gz" in suffixes:
        return _sha256_gzip_payload(path), "gzip"
    if ".gz" not in suffixes and _looks_like_utf8(path):
        text = path.read_text(encoding="utf-8")
        text = text.replace(str(root), "<RUN_ROOT>")
        text = normalization.normalize_string(text)
        return sha256(text.encode("utf-8")).hexdigest(), "text"
    return _sha256_file(path), "binary"


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
                normalized_child = _normalize_value(
                    child,
                    root=root,
                    volatile_keys=volatile_keys,
                    normalization=normalization,
                    rel_path=rel_path,
                )
                output[text_key] = _normalize_run_manifest_identity_rows(text_key, normalized_child, rel_path, normalization)
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
    if key == "document_type":
        return _is_redundant_document_type(mapping.get(key), mapping)
    if key == "entity_group_map":
        return _is_redundant_entity_group_map(mapping.get(key), mapping)
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


def _is_redundant_document_type(value: Any, parent: Mapping[str, Any]) -> bool:
    if not isinstance(value, str):
        return False
    locator = parent.get("locator")
    if not isinstance(locator, str) or not locator.strip():
        return False
    expected = Path(locator).suffix.lstrip(".")
    return bool(expected) and value == expected


def _is_redundant_entity_group_map(value: Any, parent: Mapping[str, Any]) -> bool:
    if value == {}:
        return True
    if not isinstance(value, Mapping):
        return False
    normalized = {str(key): str(child) for key, child in value.items()}
    site_map = parent.get("site_map")
    if isinstance(site_map, Mapping):
        return normalized == {str(key): str(child) for key, child in site_map.items()}
    entities = parent.get("entities") or parent.get("participants")
    groups = parent.get("groups") or parent.get("sites")
    if not isinstance(entities, list) or not isinstance(groups, list):
        return False
    entity_set = {str(item) for item in entities}
    group_set = {str(item) for item in groups}
    return set(normalized) == entity_set and set(normalized.values()).issubset(group_set)


def _normalize_run_manifest_identity_rows(
    key: str,
    value: Any,
    rel_path: str,
    normalization: ParityNormalization,
) -> Any:
    if not normalization.provenance_refactor:
        return value
    if key not in {"entities", "participants"} or not _is_run_manifest_document(rel_path):
        return value
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        return value
    return sorted(
        value,
        key=lambda item: (
            str(item.get("group", item.get("site", ""))),
            str(item.get("entity_id", item.get("participant_id", ""))),
            json.dumps(item, sort_keys=True, separators=(",", ":")),
        ),
    )


def _is_run_manifest_document(rel_path: str) -> bool:
    path = rel_path.replace("\\", "/")
    return path in {"manifest.json", "logs/manifest.json", "manifests/manifest.json"}


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


def _classify_changed_file(
    rel_path: str,
    old_file: Path,
    new_file: Path,
    *,
    old_root: Path,
    new_root: Path,
    volatile_keys: set[str],
    normalization: ParityNormalization,
) -> tuple[str, bool, str]:
    normalized_rel_path = rel_path.replace("\\", "/")
    if normalized_rel_path in {"logs/metrics.json", "manifests/manifest.json"}:
        old_payload = _normalized_json_payload(old_file, old_root, volatile_keys, normalization, rel_path)
        new_payload = _normalized_json_payload(new_file, new_root, volatile_keys, normalization, rel_path)
        publish_omission = _expected_publish_metric_omission(old_payload, new_payload)
        if publish_omission:
            return publish_omission
        return "unresolved_metadata", True, "run metadata differs outside an accepted correction rule"
    if normalized_rel_path == "logs/provenance/published_merged_dataset/dataset_manifest.json":
        old_payload = _normalized_json_payload(old_file, old_root, volatile_keys, normalization, rel_path)
        new_payload = _normalized_json_payload(new_file, new_root, volatile_keys, normalization, rel_path)
        if _is_expected_published_dataset_parent_omission(old_payload, new_payload):
            return (
                "expected_pre_fix_publish_parent_title_omission",
                False,
                "pre-fix publish finalization omitted a different final redaction parent title from each isolated run",
            )
        return "unresolved_metadata", True, "published dataset metadata differs outside an accepted correction rule"
    else:
        if _is_payload_path(rel_path):
            return "payload_mismatch", True, "materialized payload differs"
        if _is_metadata_path(rel_path):
            return "unresolved_metadata", True, "metadata differs outside an accepted correction rule"
        return "unresolved", True, "file differs outside an accepted correction rule"


def _normalized_json_payload(
    path: Path,
    root: Path,
    volatile_keys: set[str],
    normalization: ParityNormalization,
    rel_path: str,
) -> Any:
    return _normalize_value(
        json.loads(path.read_text(encoding="utf-8")),
        root=root,
        volatile_keys=volatile_keys,
        normalization=normalization,
        rel_path=rel_path,
    )


def _expected_publish_metric_omission(old_payload: Any, new_payload: Any) -> tuple[str, bool, str] | None:
    old_publish = _metrics_publish(old_payload)
    new_publish = _metrics_publish(new_payload)
    if not isinstance(old_publish, Mapping) or not isinstance(new_publish, Mapping):
        return None
    old_keys = {str(key) for key in old_publish}
    new_keys = {str(key) for key in new_publish}
    missing_from_old = new_keys - old_keys
    missing_from_new = old_keys - new_keys
    if not missing_from_old and not missing_from_new:
        return None
    if missing_from_old and not missing_from_new:
        trimmed_new = _remove_metrics_publish_keys(new_payload, missing_from_old)
        if old_payload == trimmed_new:
            return (
                "expected_old_baseline_publish_metadata_omission",
                False,
                "old baseline omits publish metrics that the refactored candidate now records after publish finalization",
            )
    if missing_from_old and missing_from_new:
        trimmed_old = _remove_metrics_publish_keys(old_payload, missing_from_new)
        trimmed_new = _remove_metrics_publish_keys(new_payload, missing_from_old)
        if trimmed_old == trimmed_new:
            return (
                "expected_pre_fix_publish_metadata_omission",
                False,
                "pre-fix publish finalization omitted a different final publish entity from each isolated run",
            )
    return None


def _metrics_publish(payload: Any) -> Any:
    if not isinstance(payload, Mapping):
        return None
    metrics = payload.get("metrics")
    if not isinstance(metrics, Mapping):
        return None
    return metrics.get("publish")


def _remove_metrics_publish_keys(payload: Any, keys: set[str]) -> Any:
    copied = json.loads(json.dumps(payload))
    publish = copied.get("metrics", {}).get("publish", {})
    if isinstance(publish, dict):
        for key in keys:
            publish.pop(key, None)
    return copied


def _is_expected_published_dataset_parent_omission(old_payload: Any, new_payload: Any) -> bool:
    if not isinstance(old_payload, Mapping) or not isinstance(new_payload, Mapping):
        return False
    old_parents = old_payload.get("parents")
    new_parents = new_payload.get("parents")
    if not isinstance(old_parents, list) or not isinstance(new_parents, list):
        return False
    old_titles = _redaction_parent_titles(old_parents)
    new_titles = _redaction_parent_titles(new_parents)
    old_only = old_titles - new_titles
    new_only = new_titles - old_titles
    if not old_only or not new_only:
        return False
    affected_entities = _entity_ids_from_redaction_titles(old_only | new_only)
    trimmed_old = _strip_pre_fix_redaction_parent_metadata(old_payload, old_only, affected_entities)
    trimmed_new = _strip_pre_fix_redaction_parent_metadata(new_payload, new_only, affected_entities)
    return _canonicalize_parents(trimmed_old) == _canonicalize_parents(trimmed_new)


def _redaction_parent_titles(parents: list[Any]) -> set[str]:
    titles: set[str] = set()
    for parent in parents:
        if not isinstance(parent, Mapping):
            continue
        title = parent.get("title")
        if isinstance(title, str) and title.startswith("Applied redaction rules for "):
            titles.add(title)
    return titles


def _entity_ids_from_redaction_titles(titles: set[str]) -> set[str]:
    prefix = "Applied redaction rules for "
    return {title[len(prefix) :] for title in titles if title.startswith(prefix)}


def _strip_pre_fix_redaction_parent_metadata(
    payload: Any,
    titles_to_remove: set[str],
    affected_entities: set[str],
) -> Any:
    copied = json.loads(json.dumps(payload))
    parents = copied.get("parents")
    if isinstance(parents, list):
        for parent in parents:
            if not isinstance(parent, dict):
                continue
            title = parent.get("title")
            locator = parent.get("locator")
            if isinstance(title, str) and title in titles_to_remove:
                parent.pop("title", None)
            haystack = " ".join(str(item) for item in (title, locator) if item)
            if any(entity_id in haystack for entity_id in affected_entities):
                parent.pop("dataset_id", None)
    return copied


def _canonicalize_parents(payload: Any) -> Any:
    copied = json.loads(json.dumps(payload))
    parents = copied.get("parents")
    if isinstance(parents, list):
        copied["parents"] = sorted(
            parents,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )
    return copied


def _is_payload_path(rel_path: str) -> bool:
    path = rel_path.replace("\\", "/")
    return (
        "/summary-data/" in f"/{path}/"
        or "/merged-data/" in f"/{path}/"
        or path.startswith("summary-data/")
        or path.startswith("merged-data/")
        or path.endswith(".csv")
        or path.endswith(".csv.gz")
    )


def _is_metadata_path(rel_path: str) -> bool:
    path = rel_path.replace("\\", "/")
    return path.endswith((".json", ".jsonl")) or "/logs/" in f"/{path}/" or "/manifests/" in f"/{path}/"


def _changed_count_by_classification(changes: Sequence[ParityChangedFile]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in changes:
        counts[item.classification] = counts.get(item.classification, 0) + 1
    return dict(sorted(counts.items()))


def _sha256_gzip_payload(path: Path) -> str:
    digest = sha256()
    with gzip.open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
