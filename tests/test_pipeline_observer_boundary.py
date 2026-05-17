from __future__ import annotations

import ast
import logging
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


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
        from connect_summary.pipeline.publish_observer import ConnectPublishObserver
        from connect_summary.pipeline.provenance_observer import ConnectProvenanceObserver
        from mhm_core.pipeline.observers import CompositePipelineObserver, NoOpPipelineObserver
        from mhm_core.pipeline.plugins import load_pipeline_observer

        register_connect_pipeline_profile()
        self.assertIsInstance(load_pipeline_observer("base"), NoOpPipelineObserver)
        observer = load_pipeline_observer("connect")
        self.assertIsInstance(observer, CompositePipelineObserver)
        inner = observer.observers
        self.assertTrue(any(isinstance(item, ConnectProvenanceObserver) for item in inner))
        self.assertTrue(any(isinstance(item, ConnectPublishObserver) for item in inner))

    def test_refresh_plan_uses_generic_step_capabilities(self) -> None:
        from mhm_core.pipeline.capabilities import (
            CacheRefreshCapability,
            PipelineStepCapabilities,
            RefreshSourceCapability,
        )
        from mhm_core.pipeline.refresh_plan import build_refresh_plan

        class ArbitrarySourceStep:
            def describe_capabilities(self, spec) -> PipelineStepCapabilities:
                return PipelineStepCapabilities(
                    refresh_source=RefreshSourceCapability(
                        refresh_options={
                            "mode": "relative",
                            "relative_days": 3,
                            "per_metric": {
                                "heart_rate": {
                                    "mode": "full",
                                }
                            },
                            "per_participant": {
                                "participant-1": {
                                    "mode": "relative",
                                    "relative_days": 10,
                                }
                            },
                        }
                    )
                )

        class ArbitraryCacheStep:
            def describe_capabilities(self, spec) -> PipelineStepCapabilities:
                return PipelineStepCapabilities(
                    cache_refresh=CacheRefreshCapability(
                        cache_policy_options={
                            "manifest_prefix": "s3://example/summary-manifests/",
                            "reuse": True,
                            "refresh": {
                                "mode": "relative",
                                "relative_days": 14,
                            },
                        },
                        metric_names={"sleep", "heart_rate"},
                    )
                )

        plan, cache_policy = build_refresh_plan(
            object(),
            steps=[ArbitrarySourceStep(), ArbitraryCacheStep()],
        )

        self.assertEqual(plan.default_rule.mode, "relative")
        self.assertEqual(plan.default_rule.relative_days, 3)
        self.assertEqual(plan.participant_rules["participant-1"].relative_days, 10)
        self.assertEqual(plan.metric_rules["heart_rate"].mode, "full")
        self.assertEqual(plan.metric_rules["sleep"].relative_days, 14)
        self.assertEqual(cache_policy.manifest_prefix, "s3://example/summary-manifests/")
        self.assertTrue(cache_policy.reuse_enabled)

    def test_participant_selection_uses_generic_capabilities(self) -> None:
        from mhm_core.pipeline.capabilities import ParticipantSelectionCapability
        from mhm_core.pipeline.runner import (
            _filter_completed_participants,
            _filter_participants_for_required_source_metrics,
        )

        class FakeS3:
            def list_objects_v2(self, *, Bucket: str, Prefix: str, MaxKeys: int = 1000):
                if Bucket == "example-source" and Prefix == "output/SiteA/participant-1/sleep/":
                    return {"KeyCount": 1, "Contents": [{"Key": f"{Prefix}part-000.csv.gz"}]}
                return {"KeyCount": 0}

        context = SimpleNamespace(
            s3_client=FakeS3(),
            participant_sites={
                "participant-1": "SiteA",
                "participant-2": "SiteA",
            },
            logger=logging.getLogger("test.participant_selection"),
            spec=SimpleNamespace(
                source=SimpleNamespace(bucket="example-source", prefix="output"),
                batching=SimpleNamespace(resume_completed=True),
            ),
        )
        capability = ParticipantSelectionCapability(
            required_source_metrics={"sleep"},
            skip_completed_resume=True,
            label="example",
        )

        selected = _filter_participants_for_required_source_metrics(
            context,
            ["participant-1", "participant-2"],
            [capability],
        )
        resumed = _filter_completed_participants(
            context,
            selected,
            skip_completed_resume=capability.skip_completed_resume,
        )

        self.assertEqual(selected, ["participant-1"])
        self.assertEqual(resumed, ["participant-1"])


if __name__ == "__main__":
    unittest.main()
