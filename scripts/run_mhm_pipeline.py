#!/usr/bin/env python3
"""Run the MHM-core pipeline runner."""

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mhm_core.pipeline.runner import main


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
