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
        from connect_summary.pipeline.publisher import ConnectS3PipelinePublisher
        from connect_summary.pipeline.provenance_observer import ConnectProvenanceObserver
        from mhm_core.pipeline.observers import CompositePipelineObserver, NoOpPipelineObserver
        from mhm_core.pipeline.plugins import load_pipeline_observer, load_pipeline_publisher
        from mhm_core.pipeline.publishing import NoOpPipelinePublisher

        register_connect_pipeline_profile()
        self.assertIsInstance(load_pipeline_observer("base"), NoOpPipelineObserver)
        self.assertIsInstance(load_pipeline_publisher("base"), NoOpPipelinePublisher)
        observer = load_pipeline_observer("connect")
        self.assertIsInstance(observer, CompositePipelineObserver)
        inner = observer.observers
        self.assertTrue(any(isinstance(item, ConnectProvenanceObserver) for item in inner))
        self.assertTrue(any(isinstance(item, ConnectPublishObserver) for item in inner))
        self.assertIsInstance(load_pipeline_publisher("connect"), ConnectS3PipelinePublisher)

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

    def test_core_publish_step_uses_generic_publisher_without_connect_summary(self) -> None:
        code = r"""
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from mhm_core.pipeline.observers import NoOpPipelineObserver
from mhm_core.pipeline.publishing import PipelinePublisher, PublishedArtifact, PublishResult
from mhm_core.pipeline.steps.publish import PublishStep


class RecordingPublisher(PipelinePublisher):
    def __init__(self):
        self.trees = []
        self.files = []
        self.participant_manifests = []

    def publish_tree(
        self,
        context,
        *,
        local_root,
        destination,
        filter_prefix="",
        collect_keys=False,
        collect_uploads=False,
        include_top_level_dirs=None,
    ):
        self.trees.append((str(local_root), destination, filter_prefix))
        result = PublishResult()
        if not local_root.exists():
            return result
        for file_path in sorted(local_root.rglob("*")):
            if not file_path.is_file():
                continue
            if filter_prefix and not file_path.name.startswith(filter_prefix):
                continue
            rel = file_path.relative_to(local_root).as_posix()
            if include_top_level_dirs is not None and rel.split("/", 1)[0] not in include_top_level_dirs:
                continue
            locator = f"{destination.rstrip('/')}/{rel}"
            result.files += 1
            result.bytes += file_path.stat().st_size
            if collect_keys:
                result.keys.append(locator)
            if collect_uploads:
                result.uploads.append(PublishedArtifact(file_path=file_path, locator=locator))
        return result

    def publish_file(self, context, *, file_path, destination):
        self.files.append((str(file_path), destination))
        return PublishResult(files=1, bytes=file_path.stat().st_size)

    def publish_participant_manifest(self, context, *, participant_id, site, merged_local, merged_uploads, should_publish):
        self.participant_manifests.append((participant_id, site, should_publish, len(merged_uploads)))
        return should_publish


with tempfile.TemporaryDirectory() as tmp_dir:
    root = Path(tmp_dir)
    participant_id = "participant-1"
    merged_file = root / "merged" / "SiteA" / participant_id / "sleep" / "part-000.csv.gz"
    merged_file.parent.mkdir(parents=True, exist_ok=True)
    merged_file.write_text("value\n1\n", encoding="utf-8")
    summary_file = root / "summary" / f"{participant_id}_summary.json"
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file.write_text("{}", encoding="utf-8")
    for directory in ("raw", "latest", "logs"):
        (root / directory).mkdir(parents=True, exist_ok=True)

    publisher = RecordingPublisher()
    context = SimpleNamespace(
        spec=SimpleNamespace(
            source=SimpleNamespace(prefix="source"),
            outputs=SimpleNamespace(
                merged_prefix="memory://merged/{site}/{participant_id}/",
                summary_prefix="memory://summary/{site}/{participant_id}/",
                logs_prefix="memory://logs/{run_id}/",
                manifest_key="memory://manifests/{run_id}.json",
            ),
            publishing=SimpleNamespace(
                delete_local_workspace=False,
                remove_local_raw_after_publish=False,
                remove_local_merged_after_publish=False,
                remove_local_summary_after_publish=False,
                remove_local_latest_measurement_after_publish=False,
            ),
            provenance=SimpleNamespace(upload_run_provenance=False),
        ),
        run_id="core-publish-smoke",
        start_time=datetime(2026, 5, 17, 0, 0, 0),
        current_participant=None,
        batch_participants=None,
        participant_sites={participant_id: "SiteA"},
        summary_outputs={},
        latest_measurement_outputs={},
        merged_metrics_to_publish={},
        raw_dir=root / "raw",
        merged_dir=root / "merged",
        summary_dir=root / "summary",
        latest_measurement_dir=root / "latest",
        logs_dir=root / "logs",
        workspace_dir=root,
        metrics={},
        logger=SimpleNamespace(
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
            debug=lambda *args, **kwargs: None,
            error=lambda *args, **kwargs: None,
        ),
        pipeline_observer=NoOpPipelineObserver(),
        pipeline_publisher=publisher,
    )
    result = PublishStep({}).run(context)
    assert result["merged_files"] == 1, result
    assert result["summary_files"] == 1, result
    assert publisher.participant_manifests == [(participant_id, "SiteA", True, 1)], publisher.participant_manifests
    assert any(destination == "memory://manifests/core-publish-smoke.json" for _, destination in publisher.files), publisher.files

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


if __name__ == "__main__":
    unittest.main()
