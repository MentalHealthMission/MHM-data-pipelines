#!/usr/bin/env python3
"""Smoke-check the minimal MHM pipeline kernel import/build surface."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main() -> int:
    from mhm_core.pipeline.context import create_run_context
    from mhm_core.pipeline.plugins import load_profile_plugins
    from mhm_core.pipeline.spec import RunSpec, validate_spec
    from mhm_core.pipeline.steps import build_steps

    with tempfile.TemporaryDirectory() as tmp_dir:
        spec = RunSpec.from_dict(
            {
                "run_id": "minimal-package-check",
                "profile": "minimal",
                "created_by": "mhm-minimal-check",
                "created_at": "2026-05-18T00:00:00Z",
                "priority": "medium",
                "source": {
                    "entities": ["entity-alpha"],
                    "groups": ["group-a"],
                    "entity_group_map": {"entity-alpha": "group-a"},
                },
                "workspace": {"root": tmp_dir, "run_subdir": "minimal-package-check/{run_id}"},
                "outputs": {
                    "manifest_key": "memory://manifests/{run_id}.json",
                    "logs_prefix": "memory://logs/{run_id}/",
                },
                "processing": {"steps": [{"type": "noop"}, {"type": "publish"}]},
                "publishing": {"delete_local_workspace": False},
            }
        )
        errors = validate_spec(spec)
        plugins = load_profile_plugins(spec.profile)
        steps = build_steps(spec)
        context = create_run_context(spec, spec_locator="memory://minimal-spec")

    forbidden_modules = sorted(
        name
        for name in sys.modules
        if name == "boto3"
        or name.startswith("botocore")
        or name == "connect_summary"
        or name.startswith("connect_summary.")
        or name == "pandas"
        or name.startswith("pandas.")
        or name == "rdflib"
        or name.startswith("rdflib.")
    )
    payload = {
        "status": "ok" if not errors and not forbidden_modules else "failed",
        "errors": errors,
        "plugin_types": [type(plugin).__name__ for plugin in plugins],
        "steps": [step.name for step in steps],
        "object_store_client": type(context.s3_client).__name__,
        "forbidden_modules": forbidden_modules,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
