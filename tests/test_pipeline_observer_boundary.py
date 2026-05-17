from __future__ import annotations

import ast
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class PipelineObserverBoundaryTests(unittest.TestCase):
    def test_mhm_core_pipeline_has_no_connect_summary_imports(self) -> None:
        offenders: list[str] = []
        for path in sorted(Path("mhm_core/pipeline").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith("connect_summary"):
                            offenders.append(f"{path}:{node.lineno}: import {alias.name}")
                elif isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    if module.startswith("connect_summary"):
                        offenders.append(f"{path}:{node.lineno}: from {module}")
        self.assertEqual([], offenders)

    def test_importing_core_runner_does_not_load_connect_provenance(self) -> None:
        code = "\n".join(
            [
                "import sys",
                "import mhm_core.pipeline.runner",
                "loaded = sorted(name for name in sys.modules if name.startswith('connect_summary.provenance'))",
                "print('\\n'.join(loaded))",
                "raise SystemExit(1 if loaded else 0)",
            ]
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)

    def test_core_pipeline_runs_noop_without_connect_summary(self) -> None:
        code = r"""
import sys
import tempfile
from pathlib import Path

from mhm_core.pipeline.context import RunContext
from mhm_core.pipeline.spec import RunSpec
from mhm_core.pipeline.steps import build_steps

spec = RunSpec.from_dict(
    {
        "run_id": "core-noop-smoke",
        "created_by": "tests",
        "created_at": "2026-05-17T00:00:00Z",
        "priority": "medium",
        "source": {
            "bucket": "example-bucket",
            "prefix": "example-prefix",
            "participants": ["00000000-0000-0000-0000-000000000001"],
        },
        "workspace": {"root": "/tmp", "run_subdir": "core-noop-smoke"},
        "outputs": {
            "merged_prefix": "s3://example-bucket/merged/{site}/{participant_id}/",
            "summary_prefix": "s3://example-bucket/summary/{site}/{participant_id}/",
            "manifest_key": "s3://example-bucket/manifests/{run_id}.json",
            "logs_prefix": "s3://example-bucket/logs/{run_id}/",
        },
        "processing": {"steps": [{"type": "noop"}]},
        "publishing": {},
    }
)
assert spec.profile == "base", spec.profile
with tempfile.TemporaryDirectory() as tmp_dir:
    root = Path(tmp_dir)
    context = RunContext(
        spec=spec,
        run_id=spec.run_id,
        workspace_dir=root,
        raw_dir=root / "raw",
        merged_dir=root / "merged",
        summary_dir=root / "summary",
        latest_measurement_dir=root / "latest",
        logs_dir=root / "logs",
        s3_client=object(),
    )
    context.ensure_directories()
    steps = build_steps(spec)
    assert [step.name for step in steps] == ["noop"]
    metrics = steps[0].run(context)
    assert metrics["status"] == "ok", metrics
loaded = sorted(name for name in sys.modules if name.startswith("connect_summary"))
print("\n".join(loaded))
raise SystemExit(1 if loaded else 0)
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)

    def test_core_and_connect_spec_defaults_are_separate(self) -> None:
        from connect_summary.pipeline.spec import load_spec as load_connect_spec
        from mhm_core.pipeline.spec import load_spec as load_core_spec

        spec_text = "\n".join(
            [
                "run_id: default-profile-smoke",
                "created_by: tests",
                'created_at: "2026-05-17T00:00:00Z"',
                "priority: medium",
                "source:",
                "  bucket: example-bucket",
                "  prefix: example-prefix",
                "  participants:",
                "    - 00000000-0000-0000-0000-000000000001",
                "workspace:",
                "  root: /tmp",
                "outputs:",
                "  merged_prefix: s3://example-bucket/merged/{site}/{participant_id}/",
                "  summary_prefix: s3://example-bucket/summary/{site}/{participant_id}/",
                "  manifest_key: s3://example-bucket/manifests/{run_id}.json",
                "  logs_prefix: s3://example-bucket/logs/{run_id}/",
                "processing:",
                "  steps:",
                "    - type: noop",
                "publishing: {}",
                "",
            ]
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "spec.yaml"
            path.write_text(spec_text, encoding="utf-8")
            self.assertEqual(load_core_spec(str(path)).profile, "base")
            self.assertEqual(load_connect_spec(str(path)).profile, "connect")

    def test_connect_profile_supplies_connect_provenance_observer(self) -> None:
        from connect_summary.pipeline.bootstrap import register_connect_pipeline_profile
        from connect_summary.pipeline.provenance_observer import ConnectProvenanceObserver
        from mhm_core.pipeline.observers import NoOpPipelineObserver
        from mhm_core.pipeline.plugins import load_pipeline_observer

        register_connect_pipeline_profile()
        self.assertIsInstance(load_pipeline_observer("base"), NoOpPipelineObserver)
        self.assertIsInstance(load_pipeline_observer("connect"), ConnectProvenanceObserver)


if __name__ == "__main__":
    unittest.main()
