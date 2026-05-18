#!/usr/bin/env python3
"""Check the rehearsed MHM pipeline package import contract."""

from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mhm_core.pipeline.package_contract import check_import_contract, minimal_pipeline_contract


def main() -> int:
    contract = minimal_pipeline_contract(Path(__file__).resolve().parents[1])
    violations = check_import_contract(contract)
    payload = {
        "contract": contract.name,
        "status": "ok" if not violations else "failed",
        "violations": violations,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not violations else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
