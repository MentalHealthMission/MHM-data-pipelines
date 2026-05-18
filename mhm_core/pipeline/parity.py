"""Production-spec parity comparison helpers.

The refactor parity invariant is that an unchanged CONNECT production spec can
run through the current CONNECT entrypoint and the refactored implementation
with equivalent materialized outputs. This module compares two already
materialized run/output directories and normalizes known volatile metadata.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from fnmatch import fnmatch
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

DEFAULT_IGNORE_PATTERNS = (
    ".DS_Store",
    "logs/pipeline.log",
)

DEFAULT_VOLATILE_KEYS = {
    "created_at",
    "duration_seconds",
    "elapsed_seconds",
    "last_run_id",
    "run_id",
    "started_at",
    "start_time",
    "timestamp",
    "updated_at",
}


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
) -> PipelineParityReport:
    """Compare two materialized run/output directories."""

    old_path = Path(old_root).expanduser().resolve()
    new_path = Path(new_root).expanduser().resolve()
    volatile_key_set = {str(key) for key in volatile_keys}
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
        )
        new_digest, new_kind = parity_digest(
            new_file,
            root=new_path,
            volatile_keys=volatile_key_set,
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
) -> tuple[str, str]:
    """Return a parity digest and content kind for one file."""

    suffixes = [suffix.lower() for suffix in path.suffixes]
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        normalized = _normalize_value(payload, root=root, volatile_keys=set(volatile_keys))
        body = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return sha256(body).hexdigest(), "json"
    if path.suffix.lower() == ".jsonl":
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rows.append(_normalize_value(json.loads(line), root=root, volatile_keys=set(volatile_keys)))
        body = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return sha256(body).hexdigest(), "jsonl"
    if ".gz" not in suffixes and _looks_like_utf8(path):
        text = path.read_text(encoding="utf-8")
        text = text.replace(str(root), "<RUN_ROOT>")
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


def _normalize_value(value: Any, *, root: Path, volatile_keys: set[str]) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): "<VOLATILE>" if str(key) in volatile_keys else _normalize_value(
                child,
                root=root,
                volatile_keys=volatile_keys,
            )
            for key, child in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list):
        return [_normalize_value(item, root=root, volatile_keys=volatile_keys) for item in value]
    if isinstance(value, str):
        return value.replace(str(root), "<RUN_ROOT>")
    return value


def _looks_like_utf8(path: Path) -> bool:
    try:
        path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return False
    return True


__all__ = [
    "DEFAULT_IGNORE_PATTERNS",
    "DEFAULT_VOLATILE_KEYS",
    "ParityChangedFile",
    "PipelineParityReport",
    "compare_run_directories",
    "parity_digest",
    "write_report",
]
