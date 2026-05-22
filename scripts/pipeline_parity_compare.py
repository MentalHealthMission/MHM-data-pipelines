#!/usr/bin/env python3
"""Compare two materialized pipeline run/output directories for parity."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mhm_core.pipeline.parity import DEFAULT_IGNORE_PATTERNS, ParityNormalization, compare_run_directories, write_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare baseline and candidate pipeline outputs")
    parser.add_argument("--old", required=True, help="Directory containing baseline outputs")
    parser.add_argument("--new", required=True, help="Directory containing candidate outputs")
    parser.add_argument(
        "--ignore",
        action="append",
        default=[],
        help="Additional relative glob to ignore; may be supplied more than once",
    )
    parser.add_argument("--report", help="Optional path for a JSON parity report")
    parser.add_argument(
        "--normalize-pair",
        action="append",
        default=[],
        metavar="OLD=NEW",
        help=(
            "Normalize an old/new value pair to the same token before hashing. "
            "Use for intentional isolation differences such as run ids, workspace roots, and output prefixes."
        ),
    )
    parser.add_argument(
        "--provenance-alias-normalization",
        action="store_true",
        help=(
            "Enable the explicit provenance schema-alias lens: accept additive neutral entity/group aliases "
            "where participant/site fields remain, and normalize provenance-derived ids/hashes."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    normalization = ParityNormalization.from_pairs(
        _parse_normalize_pairs(args.normalize_pair),
        provenance_alias_normalization=bool(args.provenance_alias_normalization),
    )
    report = compare_run_directories(
        args.old,
        args.new,
        ignore_patterns=tuple(DEFAULT_IGNORE_PATTERNS) + tuple(args.ignore),
        normalization=normalization,
    )
    payload = report.to_dict()
    if args.report:
        write_report(report, args.report)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if report.equivalent else 1


def _parse_normalize_pairs(values: list[str]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for value in values:
        if "=" not in value:
            raise SystemExit(f"--normalize-pair must be OLD=NEW, got: {value}")
        old_value, new_value = value.split("=", 1)
        if not old_value or not new_value:
            raise SystemExit(f"--normalize-pair requires non-empty OLD and NEW values, got: {value}")
        pairs.append((old_value, new_value))
    return pairs


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
