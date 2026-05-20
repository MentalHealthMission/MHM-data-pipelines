from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


class MemoryObjectStore:
    scheme = "memory"

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})

    def read_bytes(self, locator: str) -> bytes:
        return self.objects[locator]

    def write_bytes(self, locator: str, payload: bytes, *, content_type: str | None = None) -> None:
        self.objects[locator] = payload

    def upload_file(self, source_path: str | Path, locator: str) -> None:
        self.objects[locator] = Path(source_path).read_bytes()

    def download_file(self, locator: str, destination_path: str | Path) -> None:
        Path(destination_path).write_bytes(self.objects[locator])

    def iter_object_locators(self, prefix_locator: str):
        prefix = _prefix(prefix_locator)
        for locator in sorted(self.objects):
            if locator.startswith(prefix):
                yield locator

    def iter_child_prefix_locators(self, prefix_locator: str):
        prefix = _prefix(prefix_locator)
        children: set[str] = set()
        for locator in self.objects:
            if not locator.startswith(prefix):
                continue
            rel = locator[len(prefix) :]
            child = rel.split("/", 1)[0]
            if child:
                children.add(f"{prefix}{child}")
        yield from sorted(children)

    def prefix_has_objects(self, prefix_locator: str) -> bool:
        prefix = _prefix(prefix_locator)
        return any(locator.startswith(prefix) for locator in self.objects)


def _prefix(locator: str) -> str:
    return f"{locator.rstrip('/')}/"


class PipelineBackendBoundaryTests(unittest.TestCase):
    def test_direct_s3_client_operations_stay_inside_s3_backend_adapter(self) -> None:
        allowed = {Path("mhm_core/pipeline/backends/s3.py")}
        forbidden_patterns = [
            'client("s3")',
            "client('s3')",
            "get_object(Bucket=",
            "put_object(Bucket=",
            "list_objects_v2(Bucket=",
            'get_paginator("list_objects_v2")',
            "get_paginator('list_objects_v2')",
        ]
        offenders: list[str] = []
        for path in sorted(Path("mhm_core/pipeline").rglob("*.py")):
            if path in allowed:
                continue
            source = path.read_text(encoding="utf-8")
            for pattern in forbidden_patterns:
                if pattern in source:
                    offenders.append(f"{path}: {pattern}")
        self.assertEqual([], offenders)

    def test_manifest_helpers_accept_backend_neutral_store(self) -> None:
        from mhm_core.pipeline.latest_measurement_manifest import (
            EntityLatestMeasurementManifest,
            load_entity_latest_measurement_manifest_from_store,
            save_entity_latest_measurement_manifest_to_store,
        )
        from mhm_core.pipeline.manifest import (
            EntityManifest,
            MetricWatermark,
            load_entity_manifest_from_store,
            save_entity_manifest_to_store,
        )
        from mhm_core.pipeline.summary_manifest import (
            EntitySummaryManifest,
            load_entity_summary_manifest_from_store,
            save_entity_summary_manifest_to_store,
        )

        store = MemoryObjectStore()
        save_entity_manifest_to_store(
            store,
            EntityManifest(entity_id="entity-1", group="group-a", metrics={"sleep": MetricWatermark(files_merged=2)}),
            run_id="run-1",
            base_prefix="s3://bucket/merged",
        )
        save_entity_summary_manifest_to_store(
            store,
            EntitySummaryManifest(entity_id="entity-1", group="group-a", summary_files=["summary.csv"]),
            run_id="run-1",
            manifest_prefix="s3://bucket/summary-manifests",
        )
        save_entity_latest_measurement_manifest_to_store(
            store,
            EntityLatestMeasurementManifest(entity_id="entity-1", group="group-a", results={"sleep": "2026-05-20"}),
            run_id="run-1",
            manifest_prefix="s3://bucket/latest-manifests",
        )

        self.assertEqual(
            load_entity_manifest_from_store(
                store,
                group="group-a",
                entity_id="entity-1",
                base_prefix="s3://bucket/merged",
            ).metrics["sleep"].files_merged,
            2,
        )
        self.assertEqual(
            load_entity_summary_manifest_from_store(
                store,
                group="group-a",
                entity_id="entity-1",
                manifest_prefix="s3://bucket/summary-manifests",
            ).summary_files,
            ["summary.csv"],
        )
        self.assertEqual(
            load_entity_latest_measurement_manifest_from_store(
                store,
                group="group-a",
                entity_id="entity-1",
                manifest_prefix="s3://bucket/latest-manifests",
            ).results["sleep"],
            "2026-05-20",
        )

    def test_spec_loader_uses_object_store_for_manifest_native_inputs(self) -> None:
        from mhm_core.pipeline.spec import load_spec

        store = MemoryObjectStore(
            {
                "s3://bucket/manifests/dataset_manifest.json": json.dumps(
                    {
                        "data_root_binding": {"locator": "s3://source-bucket/output"},
                        "documents": {"coverage_summary": {"locator": "coverage.json"}},
                    }
                ).encode("utf-8"),
                "s3://bucket/manifests/coverage.json": json.dumps(
                    {
                        "coverage": {
                            "group_summary": [
                                {"group": "group-a", "entities": ["entity-1", "entity-2"]},
                            ]
                        }
                    }
                ).encode("utf-8"),
            }
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            spec_path = Path(tmp_dir) / "spec.yaml"
            spec_path.write_text(
                "\n".join(
                    [
                        "run_id: object-store-spec",
                        "created_by: tests",
                        'created_at: "2026-05-20T00:00:00Z"',
                        "priority: medium",
                        "source:",
                        "  source_state_manifest: s3://bucket/manifests/dataset_manifest.json",
                        "workspace:",
                        "  root: /tmp",
                        "outputs:",
                        "  manifest_key: /tmp/manifest.json",
                        "processing:",
                        "  steps:",
                        "    - type: noop",
                        "publishing: {}",
                    ]
                ),
                encoding="utf-8",
            )

            spec = load_spec(str(spec_path), object_store=store)

        self.assertEqual(spec.source.bucket, "source-bucket")
        self.assertEqual(spec.source.prefix, "output")
        self.assertEqual(spec.source.groups, ["group-a"])
        self.assertEqual(spec.source.entities, ["entity-1", "entity-2"])

    def test_runner_filters_use_context_object_store(self) -> None:
        from mhm_core.pipeline.capabilities import EntitySelectionCapability
        from mhm_core.pipeline.runner import _filter_completed_entities, _filter_entities_for_required_source_metrics

        store = MemoryObjectStore(
            {
                "s3://source/output/group-a/entity-1/sleep/part-000.csv.gz": b"",
                "s3://published/merged/group-a/entity-1/manifest.json": b"{}",
            }
        )
        context = SimpleNamespace(
            object_store=store,
            s3_client=object(),
            merged_base_prefix="s3://published/merged",
            entity_groups={"entity-1": "group-a", "entity-2": "group-a"},
            logger=SimpleNamespace(info=lambda *args, **kwargs: None, debug=lambda *args, **kwargs: None),
            spec=SimpleNamespace(
                source=SimpleNamespace(bucket="source", prefix="output"),
                batching=SimpleNamespace(resume_completed=True),
            ),
        )
        capability = EntitySelectionCapability(required_source_metrics={"sleep"}, label="example")

        selected = _filter_entities_for_required_source_metrics(context, ["entity-1", "entity-2"], [capability])
        remaining = _filter_completed_entities(context, selected)

        self.assertEqual(selected, ["entity-1"])
        self.assertEqual(remaining, [])


if __name__ == "__main__":
    unittest.main()
