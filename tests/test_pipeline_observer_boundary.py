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

    def test_minimal_profile_is_small_package_rehearsal_surface(self) -> None:
        code = r"""
import sys

from mhm_core.pipeline.plugins import load_profile_plugins
from mhm_core.pipeline.spec import RunSpec
from mhm_core.pipeline.steps import build_step_registry, build_steps

spec = RunSpec.from_dict(
    {
        "run_id": "minimal-profile-smoke",
        "profile": "minimal",
        "created_by": "tests",
        "created_at": "2026-05-17T00:00:00Z",
        "priority": "medium",
        "source": {
            "entities": ["entity-a"],
            "groups": ["group-a"],
            "entity_group_map": {"entity-a": "group-a"},
        },
        "workspace": {"root": "/tmp"},
        "outputs": {
            "manifest_key": "memory://manifests/{run_id}.json",
            "logs_prefix": "memory://logs/{run_id}/",
        },
        "processing": {"steps": [{"type": "noop"}]},
        "publishing": {},
    }
)
plugins = load_profile_plugins(spec.profile)
assert [plugin.profile_id for plugin in plugins] == ["minimal"], [plugin.profile_id for plugin in plugins]
registry = build_step_registry(spec)
assert sorted(registry) == ["minimal.noop", "minimal.publish", "noop", "publish"], sorted(registry)
steps = build_steps(spec)
assert [step.name for step in steps] == ["noop"], [step.name for step in steps]
for forbidden_prefix in ("connect_summary", "pandas", "rdflib"):
    loaded = sorted(name for name in sys.modules if name == forbidden_prefix or name.startswith(forbidden_prefix + "."))
    if loaded:
        print("\n".join(loaded))
        raise SystemExit(1)
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)

    def test_core_validation_allows_neutral_entity_specs(self) -> None:
        from mhm_core.pipeline.spec import RunSpec, validate_spec

        spec = RunSpec.from_dict(
            {
                "run_id": "neutral-entity-validation",
                "profile": "base",
                "created_by": "tests",
                "created_at": "2026-05-17T00:00:00Z",
                "priority": "medium",
                "source": {
                    "entities": ["document-alpha"],
                    "groups": ["collection-a"],
                    "entity_group_map": {"document-alpha": "collection-a"},
                },
                "workspace": {"root": "/tmp"},
                "outputs": {
                    "manifest_key": "memory://manifests/{run_id}.json",
                    "logs_prefix": "memory://logs/{run_id}/",
                },
                "processing": {"steps": [{"type": "noop"}]},
                "publishing": {},
            }
        )

        self.assertEqual(validate_spec(spec), [])

    def test_core_batching_prefers_entity_count_with_participant_alias_compatibility(self) -> None:
        from mhm_core.pipeline.spec import BatchingConfig, RunSpec, validate_spec
        from mhm_core.pipeline.runner import _build_entity_batches

        batching = BatchingConfig.from_dict(
            {
                "strategy": "participant_count",
                "max_participants": 2,
            }
        )
        self.assertEqual(batching.strategy, "entity_count")
        self.assertEqual(batching.max_entities, 2)
        self.assertEqual(batching.max_participants, 2)

        spec = RunSpec.from_dict(
            {
                "run_id": "entity-count-batching",
                "profile": "base",
                "created_by": "tests",
                "created_at": "2026-05-18T00:00:00Z",
                "priority": "medium",
                "source": {"entities": ["entity-a", "entity-b", "entity-c"]},
                "workspace": {"root": "/tmp"},
                "batching": {"strategy": "entity_count", "max_entities": 2},
                "outputs": {
                    "manifest_key": "memory://manifests/{run_id}.json",
                    "logs_prefix": "memory://logs/{run_id}/",
                },
                "processing": {"steps": [{"type": "noop"}]},
                "publishing": {},
            }
        )

        self.assertEqual(validate_spec(spec), [])
        self.assertEqual(
            _build_entity_batches(spec, list(spec.iter_entities()), {}),
            [["entity-a", "entity-b"], ["entity-c"]],
        )

    def test_manifest_native_source_loading_prefers_neutral_coverage_fields(self) -> None:
        from mhm_core.pipeline.spec import load_spec

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            coverage_path = root / "coverage.json"
            coverage_path.write_text(
                """
                {
                  "coverage": {
                    "group_summary": [
                      {
                        "group": "collection-a",
                        "entities": ["entity-a", "entity-b"]
                      }
                    ]
                  }
                }
                """,
                encoding="utf-8",
            )
            source_manifest = root / "source_state.json"
            source_manifest.write_text(
                """
                {
                  "data_root_binding": {
                    "locator": "s3://example-source/prefix"
                  },
                  "documents": {
                    "coverage_summary": {
                      "locator": "coverage.json"
                    }
                  }
                }
                """,
                encoding="utf-8",
            )
            spec_path = root / "spec.yaml"
            spec_path.write_text(
                "\n".join(
                    [
                        "run_id: manifest-native-neutral",
                        "profile: base",
                        "created_by: tests",
                        'created_at: "2026-05-18T00:00:00Z"',
                        "priority: medium",
                        "source:",
                        f"  source_state_manifest: {source_manifest}",
                        "workspace:",
                        "  root: /tmp",
                        "outputs:",
                        "  manifest_key: memory://manifests/{run_id}.json",
                        "  logs_prefix: memory://logs/{run_id}/",
                        "processing:",
                        "  steps:",
                        "    - type: noop",
                        "publishing: {}",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            spec = load_spec(str(spec_path))

        self.assertEqual(spec.source.bucket, "example-source")
        self.assertEqual(spec.source.prefix, "prefix")
        self.assertEqual(spec.source.groups, ["collection-a"])
        self.assertEqual(spec.source.entities, ["entity-a", "entity-b"])

    def test_connect_validation_keeps_uuid_and_s3_shaped_requirements(self) -> None:
        from connect_summary.pipeline.spec import validate_spec
        from mhm_core.pipeline.spec import RunSpec

        spec = RunSpec.from_dict(
            {
                "run_id": "connect-validation",
                "profile": "connect",
                "created_by": "tests",
                "created_at": "2026-05-17T00:00:00Z",
                "priority": "medium",
                "source": {
                    "participants": ["document-alpha"],
                    "sites": ["SiteA"],
                },
                "workspace": {"root": "/tmp"},
                "outputs": {
                    "manifest_key": "s3://example/manifests/{run_id}.json",
                    "logs_prefix": "s3://example/logs/{run_id}/",
                },
                "processing": {"steps": [{"type": "publish"}]},
                "publishing": {},
            }
        )

        errors = validate_spec(spec)
        self.assertIn("source.bucket must be provided for CONNECT pipeline specs", errors)
        self.assertIn("source.prefix must be provided for CONNECT pipeline specs", errors)
        self.assertIn("outputs.summary_prefix must be configured for CONNECT pipeline specs", errors)
        self.assertIn("CONNECT participant IDs must be valid UUIDs: ['document-alpha']", errors)

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
            EntitySelectionCapability,
            ParticipantSelectionCapability,
            PipelineStepCapabilities,
            RefreshSourceCapability,
        )
        from mhm_core.pipeline.refresh_plan import CacheRefreshPolicy
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
        self.assertIsInstance(cache_policy, CacheRefreshPolicy)
        self.assertEqual(cache_policy.manifest_prefix, "s3://example/summary-manifests/")
        self.assertTrue(cache_policy.reuse_enabled)
        self.assertIs(ParticipantSelectionCapability, EntitySelectionCapability)

    def test_entity_selection_uses_generic_capabilities(self) -> None:
        from mhm_core.pipeline.capabilities import EntitySelectionCapability
        from mhm_core.pipeline.runner import (
            _filter_completed_entities,
            _filter_entities_for_required_source_metrics,
        )

        class FakeS3:
            def list_objects_v2(self, *, Bucket: str, Prefix: str, MaxKeys: int = 1000):
                if Bucket == "example-source" and Prefix == "output/SiteA/participant-1/sleep/":
                    return {"KeyCount": 1, "Contents": [{"Key": f"{Prefix}part-000.csv.gz"}]}
                return {"KeyCount": 0}

        context = SimpleNamespace(
            s3_client=FakeS3(),
            entity_groups={
                "participant-1": "SiteA",
                "participant-2": "SiteA",
            },
            logger=logging.getLogger("test.participant_selection"),
            spec=SimpleNamespace(
                source=SimpleNamespace(bucket="example-source", prefix="output"),
                batching=SimpleNamespace(resume_completed=True),
            ),
        )
        capability = EntitySelectionCapability(
            required_source_metrics={"sleep"},
            skip_completed_resume=True,
            label="example",
        )

        selected = _filter_entities_for_required_source_metrics(
            context,
            ["participant-1", "participant-2"],
            [capability],
        )
        resumed = _filter_completed_entities(
            context,
            selected,
            skip_completed_resume=capability.skip_completed_resume,
        )

        self.assertEqual(selected, ["participant-1"])
        self.assertEqual(resumed, ["participant-1"])

    def test_queue_selection_is_backend_neutral_with_s3_adapter_compatibility(self) -> None:
        from datetime import datetime, timezone

        from mhm_core.pipeline.backends.s3 import S3QueueBackend
        from mhm_core.pipeline.queue import QueueEntry, select_next_queue_spec

        class MemoryQueue:
            def list_specs(self, *, states):
                return [
                    QueueEntry("pending", "slow.yaml", "low", datetime(2026, 5, 19, tzinfo=timezone.utc)),
                    QueueEntry("pending", "fast.yaml", "urgent", datetime(2026, 5, 19, tzinfo=timezone.utc)),
                ]

        class Body:
            def __init__(self, payload: bytes):
                self.payload = payload

            def read(self):
                return self.payload

        class Paginator:
            def paginate(self, *, Bucket, Prefix):
                return [
                    {
                        "Contents": [
                            {
                                "Key": f"{Prefix}queued.yaml",
                                "LastModified": datetime(2026, 5, 19, tzinfo=timezone.utc),
                            }
                        ]
                    }
                ]

        class FakeS3:
            def get_paginator(self, operation):
                self.operation = operation
                return Paginator()

            def get_object(self, *, Bucket, Key):
                return {"Body": Body(b"priority: high\n")}

        selected = select_next_queue_spec(MemoryQueue(), states=("pending",))
        self.assertEqual(selected.key, "fast.yaml")

        s3_selected = select_next_queue_spec(S3QueueBackend(FakeS3(), "s3://queue-root/specs"), states=("pending",))
        self.assertEqual(s3_selected.priority, "high")
        self.assertEqual(s3_selected.key, "queued.yaml")

    def test_source_discovery_prefers_entity_group_language(self) -> None:
        from mhm_core.pipeline.discovery import discover_entities, discover_participants

        class Paginator:
            def paginate(self, *, Bucket, Prefix, Delimiter):
                if Prefix == "output/" and Delimiter == "/":
                    return [{"CommonPrefixes": [{"Prefix": "output/group-a/"}]}]
                if Prefix == "output/group-a/" and Delimiter == "/":
                    return [{"CommonPrefixes": [{"Prefix": "output/group-a/entity-1/"}]}]
                return [{"CommonPrefixes": []}]

        class FakeS3:
            def get_paginator(self, operation):
                return Paginator()

        entities, entity_group_map = discover_entities(
            FakeS3(),
            bucket="example",
            prefix="output",
            logger=logging.getLogger("test.discovery"),
        )
        self.assertEqual(entities, ["entity-1"])
        self.assertEqual(entity_group_map, {"entity-1": "group-a"})

        participants, site_map = discover_participants(
            FakeS3(),
            bucket="example",
            prefix="output",
            logger=logging.getLogger("test.discovery"),
        )
        self.assertEqual(participants, entities)
        self.assertEqual(site_map, entity_group_map)

    def test_feature_steps_prefer_entity_options_with_participant_aliases(self) -> None:
        import mhm_core.pipeline.integrations.derived_features as derived_module
        from mhm_core.pipeline.integrations.derived_features import DerivedFeaturesStep
        from mhm_core.pipeline.integrations.rapids import CombineFeaturesStep

        context = SimpleNamespace(
            run_id="entity-step-smoke",
            workspace_dir=Path("/tmp/entity-step-smoke"),
            entity_groups={"entity-1": "group-a"},
            logger=logging.getLogger("test.entity_steps"),
        )

        captured: list[list[str]] = []

        class RecordingCombine(CombineFeaturesStep):
            def run_command(self, context, cmd):
                captured.append(list(cmd))

        RecordingCombine({"entities": ["entity-1"]}).run(context)
        self.assertIn("--entities", captured[0])
        self.assertNotIn("--participants", captured[0])

        with tempfile.TemporaryDirectory() as tmp_dir:
            spec_path = Path(tmp_dir) / "derived.yaml"
            spec_path.write_text("features: []\n", encoding="utf-8")
            calls: list[str] = []
            original = derived_module.run_derived_features_for_entity

            def fake_runner(context, entity_id, *, spec_path, output_dir, input_dir=None):
                calls.append(entity_id)
                return {"status": "ok"}

            try:
                derived_module.run_derived_features_for_entity = fake_runner
                result = DerivedFeaturesStep({"spec": str(spec_path), "entities": ["entity-1"]}).run(context)
            finally:
                derived_module.run_derived_features_for_entity = original

        self.assertEqual(calls, ["entity-1"])
        self.assertEqual(result["entities"], 1)
        self.assertEqual(result["participants"], 1)

    def test_merge_manifest_has_neutral_entity_schema_with_participant_aliases(self) -> None:
        from mhm_core.pipeline.latest_measurement_manifest import (
            EntityLatestMeasurementManifest,
            LatestMeasurementManifest,
            entity_latest_measurement_manifest_object_key,
            latest_measurement_manifest_s3_key,
        )
        from mhm_core.pipeline.manifest import EntityManifest, ParticipantManifest
        from mhm_core.pipeline.summary_manifest import (
            EntitySummaryManifest,
            SummaryManifest,
            entity_summary_manifest_object_key,
            summary_manifest_s3_key,
        )

        manifest = EntityManifest(entity_id="entity-1", group="group-a")
        payload = manifest.to_dict()

        self.assertEqual(payload["entity_id"], "entity-1")
        self.assertEqual(payload["group"], "group-a")
        self.assertEqual(payload["participant_id"], "entity-1")
        self.assertEqual(payload["site"], "group-a")

        compat = ParticipantManifest.from_dict("participant-1", "site-a", payload)
        self.assertEqual(compat.participant_id, "entity-1")
        self.assertEqual(compat.site, "group-a")

        summary = EntitySummaryManifest(entity_id="entity-1", group="group-a").to_dict()
        latest = EntityLatestMeasurementManifest(entity_id="entity-1", group="group-a").to_dict()
        self.assertEqual(summary["entity_id"], "entity-1")
        self.assertEqual(summary["group"], "group-a")
        self.assertEqual(latest["entity_id"], "entity-1")
        self.assertEqual(latest["group"], "group-a")

        summary_compat = SummaryManifest(participant_id="entity-1", site="group-a")
        latest_compat = LatestMeasurementManifest(participant_id="entity-1", site="group-a")
        self.assertEqual(summary_compat.entity_id, "entity-1")
        self.assertEqual(latest_compat.group, "group-a")
        self.assertEqual(
            entity_summary_manifest_object_key("group-a", "entity-1", manifest_prefix="s3://bucket/manifests"),
            summary_manifest_s3_key("group-a", "entity-1", manifest_prefix="s3://bucket/manifests"),
        )
        self.assertEqual(
            entity_latest_measurement_manifest_object_key("group-a", "entity-1", manifest_prefix="s3://bucket/latest"),
            latest_measurement_manifest_s3_key("group-a", "entity-1", manifest_prefix="s3://bucket/latest"),
        )

    def test_core_publish_step_has_no_project_output_assumptions(self) -> None:
        source = Path("mhm_core/pipeline/steps/publish.py").read_text(encoding="utf-8")
        forbidden = [
            "summary_outputs",
            "latest_measurement_outputs",
            "summary_prefix",
            "latest_measurement_output_prefix",
            "latest_measurement_files",
            "summary_files",
        ]
        offenders = [term for term in forbidden if term in source]
        self.assertEqual([], offenders)

    def test_core_publish_step_uses_generic_publisher_without_connect_summary(self) -> None:
        code = r"""
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from mhm_core.pipeline.observers import PipelineObserver
from mhm_core.pipeline.publishing import (
    EntityPublishTarget,
    PipelinePublisher,
    PublishedArtifact,
    PublishResult,
    RunPublishTarget,
)
from mhm_core.pipeline.steps.publish import PublishStep


class RecordingPublisher(PipelinePublisher):
    def __init__(self):
        self.trees = []
        self.files = []
        self.entity_manifests = []

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

    def publish_entity_manifest(self, context, *, entity_id, group, output_local, published_artifacts, should_publish, target_name):
        self.entity_manifests.append((entity_id, group, should_publish, len(published_artifacts), target_name))
        return should_publish


class TargetObserver(PipelineObserver):
    def run_publish_targets(self, context, *, logs_prefix):
        extra_path = context.logs_dir / "profile_run_artifact.json"
        extra_path.write_text("{}", encoding="utf-8")
        return [
            RunPublishTarget(
                name="profile_run_artifact",
                file_path=extra_path,
                destination=f"{logs_prefix.rstrip('/')}/profile_run_artifact.json",
                required=True,
            )
        ]

    def entity_publish_targets(self, context, *, entity_id, group):
        return [
            EntityPublishTarget(
                name="artifact",
                local_root=context.merged_dir / group / entity_id,
                destination=f"memory://artifacts/{group}/{entity_id}/",
                collect_uploads=True,
                publish_entity_manifest=True,
                primary_output=True,
            ),
            EntityPublishTarget(
                name="analysis",
                local_root=context.summary_dir,
                destination=f"memory://analysis/{group}/{entity_id}/",
                filter_prefix=f"{entity_id}_",
                collect_keys=True,
            )
        ]


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
        current_entity=None,
        batch_entities=None,
        current_participant=None,
        batch_participants=None,
        entity_groups={participant_id: "SiteA"},
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
        pipeline_observer=TargetObserver(),
        pipeline_publisher=publisher,
    )
    result = PublishStep({}).run(context)
    assert result["artifact_files"] == 1, result
    assert result["analysis_files"] == 1, result
    assert result["published_target_files"] == {"analysis": 1, "artifact": 1}, result
    assert publisher.entity_manifests == [(participant_id, "SiteA", True, 1, "artifact")], publisher.entity_manifests
    assert any(destination == "memory://manifests/core-publish-smoke.json" for _, destination in publisher.files), publisher.files
    assert any(destination == "memory://logs/core-publish-smoke/profile_run_artifact.json" for _, destination in publisher.files), publisher.files

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

    def test_module_adoption_bridge_models_rapids_connect_binding(self) -> None:
        from mhm_core.pipeline.adoption import (
            ModuleAdoptionBridge,
            PipelineModuleBinding,
            PipelineModuleContract,
        )

        rapids_contract = PipelineModuleContract(
            module_id="mhm.rapids",
            display_name="MHM RAPIDS",
            capabilities=("passive_feature_extraction", "feature_reduction"),
            required_inputs=("entity_metric_tree", "entity_group_map"),
            produced_outputs=("rapids_features", "rapids_manifest"),
            config_keys=("provider_map", "external_engine"),
            optional_dependencies=("rapids-engine",),
        )
        connect_binding = PipelineModuleBinding(
            module_id="mhm.rapids",
            project_id="connect",
            profile_id="connect",
            input_bindings={
                "entity_metric_tree": "CONNECT raw/merged passive-data tree",
                "entity_group_map": "CONNECT participant/site map",
            },
            output_bindings={
                "rapids_features": "CONNECT run workspace RAPIDS feature outputs",
                "rapids_manifest": "CONNECT run-level rapids_manifest.json",
            },
            config_bindings={
                "provider_map": "CONNECT RAPIDS provider mapping assets",
                "external_engine": "operator-provided external/rapids checkout",
            },
            asset_bindings={
                "feature_map": "RAPIDS-to-MHM/ODIM feature map",
            },
            provenance_bindings=("rapids.stage", "rapids.run", "rapids.reduce"),
        )

        bridge = ModuleAdoptionBridge(rapids_contract, connect_binding)
        self.assertEqual(bridge.validate(), [])

        incomplete = ModuleAdoptionBridge(
            rapids_contract,
            PipelineModuleBinding(
                module_id="mhm.rapids",
                project_id="connect",
                profile_id="connect",
            ),
        )
        self.assertEqual(
            incomplete.validate(),
            [
                "module binding missing required inputs: ['entity_group_map', 'entity_metric_tree']",
                "module binding missing produced outputs: ['rapids_features', 'rapids_manifest']",
                "module binding missing config keys: ['external_engine', 'provider_map']",
            ],
        )

    def test_connect_rapids_adoption_bridge_is_valid(self) -> None:
        from connect_summary.rapids.adoption import (
            RAPIDS_MODULE_ID,
            connect_rapids_bridge,
            rapids_module_contract,
        )

        contract = rapids_module_contract()
        self.assertEqual(contract.module_id, RAPIDS_MODULE_ID)
        self.assertIn("passive_feature_extraction", contract.capabilities)
        self.assertIn("entity_metric_tree", contract.required_inputs)
        self.assertIn("external_engine", contract.config_keys)

        bridge = connect_rapids_bridge(
            rapids_dir="/opt/rapids",
            provider_map="ontology/mappings/rapids-provider-map.yaml",
            deployment="docker image or external checkout",
        )
        self.assertEqual(bridge.validate(), [])
        self.assertEqual(bridge.binding.project_id, "connect")
        self.assertEqual(bridge.binding.config_bindings["external_engine"], "/opt/rapids")
        self.assertEqual(
            bridge.binding.input_bindings["entity_group_map"],
            "CONNECT participant/site map exposed as entity_group_map",
        )


if __name__ == "__main__":
    unittest.main()
