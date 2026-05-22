from __future__ import annotations

import gzip
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from mhm_core.pipeline.parity import ParityNormalization, compare_run_directories


class PipelineParityHarnessTests(unittest.TestCase):
    def test_normalizes_known_volatile_run_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old"
            new = root / "new"
            (old / "logs").mkdir(parents=True)
            (new / "logs").mkdir(parents=True)
            old_payload = {
                "run_id": "production-run-old",
                "created_at": "2026-05-18T08:00:00Z",
                "updated_at": "2026-05-18T08:05:00Z",
                "metrics": {"publish": {"duration_seconds": 1.2, "files": 2}},
                "outputs": ["s3://bucket/merged/site-a/entity-a/manifest.json"],
            }
            new_payload = {
                "run_id": "production-run-new",
                "created_at": "2026-05-18T09:00:00Z",
                "updated_at": "2026-05-18T09:05:00Z",
                "metrics": {"publish": {"duration_seconds": 2.9, "files": 2}},
                "outputs": ["s3://bucket/merged/site-a/entity-a/manifest.json"],
            }
            (old / "logs" / "manifest.json").write_text(json.dumps(old_payload), encoding="utf-8")
            (new / "logs" / "manifest.json").write_text(json.dumps(new_payload), encoding="utf-8")

            report = compare_run_directories(old, new)

            self.assertTrue(report.equivalent, report.to_dict())
            self.assertEqual(report.matched, ["logs/manifest.json"])

    def test_compares_gzip_files_by_decompressed_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old"
            new = root / "new"
            old.mkdir()
            new.mkdir()
            payload = b"entity,value\nalpha,1\n"
            (old / "summary.csv.gz").write_bytes(gzip.compress(payload, mtime=1))
            (new / "summary.csv.gz").write_bytes(gzip.compress(payload, mtime=2))

            report = compare_run_directories(old, new)

            self.assertTrue(report.equivalent, report.to_dict())
            self.assertEqual(report.matched, ["summary.csv.gz"])

    def test_reports_changed_deterministic_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old"
            new = root / "new"
            old.mkdir()
            new.mkdir()
            (old / "summary.csv").write_text("entity,value\nalpha,1\n", encoding="utf-8")
            (new / "summary.csv").write_text("entity,value\nalpha,2\n", encoding="utf-8")

            report = compare_run_directories(old, new)

            self.assertFalse(report.equivalent)
            self.assertEqual([item.path for item in report.changed], ["summary.csv"])
            self.assertEqual(report.changed[0].classification, "payload_mismatch")
            self.assertTrue(report.changed[0].blocking)

    def test_expected_old_baseline_publish_metadata_omission_is_non_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs"
            new = root / "new" / "logs"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            old_payload = {
                "metrics": {
                    "download": {"entity-a": {"status": "ok"}, "entity-b": {"status": "ok"}},
                    "publish": {"entity-a": {"status": "ok", "duration_seconds": 1.0}},
                }
            }
            new_payload = {
                "metrics": {
                    "download": {"entity-a": {"status": "ok"}, "entity-b": {"status": "ok"}},
                    "publish": {
                        "entity-a": {"status": "ok", "duration_seconds": 2.0},
                        "entity-b": {"status": "ok", "duration_seconds": 3.0},
                    },
                }
            }
            (old / "metrics.json").write_text(json.dumps(old_payload), encoding="utf-8")
            (new / "metrics.json").write_text(json.dumps(new_payload), encoding="utf-8")

            report = compare_run_directories(root / "old", root / "new")

            self.assertFalse(report.equivalent)
            self.assertTrue(report.blocking_equivalent, report.to_dict())
            self.assertEqual(
                report.changed[0].classification,
                "expected_old_baseline_publish_metadata_omission",
            )
            self.assertFalse(report.changed[0].blocking)

    def test_non_omission_publish_metadata_change_remains_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs"
            new = root / "new" / "logs"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            old_payload = {"metrics": {"publish": {"entity-a": {"status": "ok", "files": 1}}}}
            new_payload = {"metrics": {"publish": {"entity-a": {"status": "ok", "files": 2}}}}
            (old / "metrics.json").write_text(json.dumps(old_payload), encoding="utf-8")
            (new / "metrics.json").write_text(json.dumps(new_payload), encoding="utf-8")

            report = compare_run_directories(root / "old", root / "new")

            self.assertFalse(report.blocking_equivalent)
            self.assertEqual(report.changed[0].classification, "unresolved_metadata")
            self.assertTrue(report.changed[0].blocking)

    def test_pre_fix_publish_metadata_omission_on_both_sides_is_non_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs"
            new = root / "new" / "logs"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            old_payload = {
                "metrics": {
                    "summary": {
                        "entity-a": {"status": "ok"},
                        "entity-b": {"status": "ok"},
                        "entity-c": {"status": "ok"},
                    },
                    "publish": {
                        "entity-a": {"status": "ok", "duration_seconds": 1.0},
                        "entity-b": {"status": "ok", "duration_seconds": 2.0},
                    },
                }
            }
            new_payload = {
                "metrics": {
                    "summary": {
                        "entity-a": {"status": "ok"},
                        "entity-b": {"status": "ok"},
                        "entity-c": {"status": "ok"},
                    },
                    "publish": {
                        "entity-a": {"status": "ok", "duration_seconds": 3.0},
                        "entity-c": {"status": "ok", "duration_seconds": 4.0},
                    },
                }
            }
            (old / "metrics.json").write_text(json.dumps(old_payload), encoding="utf-8")
            (new / "metrics.json").write_text(json.dumps(new_payload), encoding="utf-8")

            report = compare_run_directories(root / "old", root / "new")

            self.assertFalse(report.equivalent)
            self.assertTrue(report.blocking_equivalent, report.to_dict())
            self.assertEqual(report.changed[0].classification, "expected_pre_fix_publish_metadata_omission")

    def test_pre_fix_published_dataset_parent_title_omission_is_non_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs" / "provenance" / "published_merged_dataset"
            new = root / "new" / "logs" / "provenance" / "published_merged_dataset"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            old_payload = {
                "dataset_kind": "published_merged_dataset",
                "parents": [
                    {"locator": "merged-entity-a", "dataset_id": "same-a"},
                    {"locator": "merged-entity-b", "title": "Applied redaction rules for entity-b", "dataset_id": "old-b"},
                    {"locator": "merged-entity-c"},
                ],
            }
            new_payload = {
                "dataset_kind": "published_merged_dataset",
                "parents": [
                    {"locator": "merged-entity-a", "dataset_id": "same-a"},
                    {"locator": "merged-entity-b"},
                    {"locator": "merged-entity-c", "title": "Applied redaction rules for entity-c", "dataset_id": "new-c"},
                ],
            }
            (old / "dataset_manifest.json").write_text(json.dumps(old_payload), encoding="utf-8")
            (new / "dataset_manifest.json").write_text(json.dumps(new_payload), encoding="utf-8")

            report = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization(provenance_alias_normalization=True),
            )

            self.assertFalse(report.equivalent)
            self.assertTrue(report.blocking_equivalent, report.to_dict())
            self.assertEqual(
                report.changed[0].classification,
                "expected_pre_fix_publish_parent_title_omission",
            )

    def test_normalize_pairs_accept_intentional_isolation_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old"
            new = root / "new"
            old.mkdir()
            new.mkdir()
            (old / "manifest.json").write_text(
                json.dumps({"output": "s3://bucket/parity/old/run-old/output.csv"}),
                encoding="utf-8",
            )
            (new / "manifest.json").write_text(
                json.dumps({"output": "s3://bucket/parity/new/run-new/output.csv"}),
                encoding="utf-8",
            )

            strict = compare_run_directories(old, new)
            normalized = compare_run_directories(
                old,
                new,
                normalization=ParityNormalization.from_pairs(
                    [
                        ("s3://bucket/parity/old/", "s3://bucket/parity/new/"),
                        ("run-old", "run-new"),
                    ]
                ),
            )

            self.assertFalse(strict.equivalent)
            self.assertTrue(normalized.equivalent, normalized.to_dict())

    def test_normalize_pairs_handle_overlapping_path_prefixes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old"
            new = root / "new"
            old.mkdir()
            new.mkdir()
            (old / "manifest.json").write_text(
                json.dumps({"script": "/opt/mhm/pipeline/connect_summary/merge-data.py"}),
                encoding="utf-8",
            )
            (new / "manifest.json").write_text(
                json.dumps({"script": "/opt/mhm/pipeline-candidate/connect_summary/merge-data.py"}),
                encoding="utf-8",
            )

            report = compare_run_directories(
                old,
                new,
                normalization=ParityNormalization.from_pairs(
                    [("/opt/mhm/pipeline", "/opt/mhm/pipeline-candidate")]
                ),
            )

            self.assertTrue(report.equivalent, report.to_dict())

    def test_provenance_alias_normalization_mode_treats_run_manifest_identity_rows_as_sets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "manifests"
            new = root / "new" / "manifests"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            old_payload = {
                "participants": [
                    {"participant_id": "entity-b", "site": "site-b"},
                    {"participant_id": "entity-a", "site": "site-a"},
                ],
                "entities": [
                    {"entity_id": "entity-b", "group": "site-b"},
                    {"entity_id": "entity-a", "group": "site-a"},
                ],
            }
            new_payload = {
                "participants": [
                    {"participant_id": "entity-a", "site": "site-a"},
                    {"participant_id": "entity-b", "site": "site-b"},
                ],
                "entities": [
                    {"entity_id": "entity-a", "group": "site-a"},
                    {"entity_id": "entity-b", "group": "site-b"},
                ],
            }
            (old / "manifest.json").write_text(json.dumps(old_payload), encoding="utf-8")
            (new / "manifest.json").write_text(json.dumps(new_payload), encoding="utf-8")

            strict = compare_run_directories(root / "old", root / "new")
            normalized = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization(provenance_alias_normalization=True),
            )

            self.assertFalse(strict.equivalent)
            self.assertTrue(normalized.equivalent, normalized.to_dict())

    def test_provenance_alias_normalization_mode_accepts_neutral_aliases_and_derived_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs" / "provenance" / "step_states" / "state" / "01-download"
            new = root / "new" / "logs" / "provenance" / "step_states" / "state" / "01-download"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            old_payload = {
                "manifest_type": "coverage_summary",
                "dataset_id": "old-dataset",
                "document_hash": "old-hash",
                "coverage": {
                    "content_summary": {
                        "participant_count": 1,
                        "site_count": 1,
                        "file_count": 2,
                    },
                    "site_summary": {"test": {"participant_count": 1}},
                },
            }
            new_payload = {
                "manifest_type": "coverage_summary",
                "dataset_id": "new-dataset",
                "document_hash": "new-hash",
                "coverage": {
                    "content_summary": {
                        "participant_count": 1,
                        "entity_count": 1,
                        "site_count": 1,
                        "group_count": 1,
                        "file_count": 2,
                    },
                    "site_summary": {"test": {"participant_count": 1}},
                    "group_summary": {"test": {"entity_count": 1}},
                },
            }
            (old / "coverage_summary.json").write_text(json.dumps(old_payload), encoding="utf-8")
            (new / "coverage_summary.json").write_text(json.dumps(new_payload), encoding="utf-8")

            strict = compare_run_directories(root / "old", root / "new")
            normalized = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization(provenance_alias_normalization=True),
            )

            self.assertFalse(strict.equivalent)
            self.assertTrue(normalized.equivalent, normalized.to_dict())

    def test_provenance_alias_normalization_mode_accepts_redundant_inventory_coordinates_and_labels(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs" / "provenance"
            new = root / "new" / "logs" / "provenance"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            old_record = {
                "artifact_type": "file",
                "artifact_hash": "old-derived-hash",
                "logical_address": {
                    "surface": "ec2_workspace",
                    "domain": "passive-data",
                    "stage": "raw",
                    "site": "test",
                    "participant_id": "entity-1",
                    "stream": "steps",
                    "artifact": "20260101_0000.csv.gz",
                    "dataset_id": "old-dataset",
                },
                "logical_address_display": "ec2_workspace:passive-data:raw:test:entity-1:steps:20260101_0000.csv.gz",
                "observed": {"fingerprint_mode": "metadata", "modified_at": "old-time", "size_bytes": 12},
                "physical_bindings": [{"binding_type": "posix_path", "locator": "/old/root/file.csv.gz"}],
            }
            new_record = {
                "artifact_type": "file",
                "artifact_hash": "new-derived-hash",
                "logical_address": {
                    "surface": "ec2_workspace",
                    "domain": "passive-data",
                    "stage": "raw",
                    "site": "test",
                    "group": "test",
                    "participant_id": "entity-1",
                    "entity_id": "entity-1",
                    "stream": "steps",
                    "artifact": "20260101_0000.csv.gz",
                    "dataset_id": "new-dataset",
                    "coordinates": {"group": "test", "entity_id": "entity-1", "stream": "steps"},
                    "labels": {
                        "group": "test",
                        "site": "test",
                        "entity_id": "entity-1",
                        "participant_id": "entity-1",
                        "stream": "steps",
                    },
                },
                "logical_address_display": "ec2_workspace:passive-data:raw:test:entity-1:steps:20260101_0000.csv.gz",
                "observed": {"fingerprint_mode": "metadata", "modified_at": "new-time", "size_bytes": 12},
                "physical_bindings": [{"binding_type": "posix_path", "locator": "/new/root/file.csv.gz"}],
            }
            (old / "artifact_inventory.jsonl").write_text(json.dumps(old_record) + "\n", encoding="utf-8")
            (new / "artifact_inventory.jsonl").write_text(json.dumps(new_record) + "\n", encoding="utf-8")

            strict = compare_run_directories(root / "old", root / "new")
            normalized = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization.from_pairs(
                    [("/old/root", "/new/root")],
                    provenance_alias_normalization=True,
                ),
            )

            self.assertFalse(strict.equivalent)
            self.assertTrue(normalized.equivalent, normalized.to_dict())

    def test_provenance_alias_normalization_mode_reports_non_redundant_inventory_labels(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs" / "provenance"
            new = root / "new" / "logs" / "provenance"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            base_record = {
                "artifact_type": "file",
                "artifact_hash": "derived-hash",
                "logical_address": {
                    "surface": "ec2_workspace",
                    "domain": "passive-data",
                    "stage": "raw",
                    "site": "test",
                    "participant_id": "entity-1",
                    "stream": "steps",
                    "artifact": "20260101_0000.csv.gz",
                    "dataset_id": "dataset",
                },
                "logical_address_display": "ec2_workspace:passive-data:raw:test:entity-1:steps:20260101_0000.csv.gz",
                "observed": {"fingerprint_mode": "metadata", "modified_at": "time", "size_bytes": 12},
                "physical_bindings": [{"binding_type": "posix_path", "locator": "/root/file.csv.gz"}],
            }
            changed_record = json.loads(json.dumps(base_record))
            changed_record["logical_address"]["labels"] = {
                "group": "test",
                "site": "test",
                "entity_id": "entity-1",
                "participant_id": "entity-1",
                "stream": "steps",
                "non_redundant_label": "should-not-be-normalized",
            }
            (old / "artifact_inventory.jsonl").write_text(json.dumps(base_record) + "\n", encoding="utf-8")
            (new / "artifact_inventory.jsonl").write_text(json.dumps(changed_record) + "\n", encoding="utf-8")

            report = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization(provenance_alias_normalization=True),
            )

            self.assertFalse(report.equivalent)
            self.assertEqual([item.path for item in report.changed], ["logs/provenance/artifact_inventory.jsonl"])

    def test_provenance_alias_normalization_mode_accepts_redundant_source_locator_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs" / "provenance"
            new = root / "new" / "logs" / "provenance"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            old_payload = {
                "manifest_type": "pipeline_spec_manifest",
                "document_hash": "old-hash",
                "spec_node_hash": "old-spec-node-hash",
                "source": {
                    "bucket": "connect-dev-output",
                    "prefix": "output",
                    "sites": ["test"],
                    "participants": [],
                },
                "spec_node": {
                    "source": {
                        "bucket": "connect-dev-output",
                        "prefix": "output",
                        "sites": ["test"],
                        "participants": [],
                    }
                },
            }
            new_payload = {
                "manifest_type": "pipeline_spec_manifest",
                "document_hash": "new-hash",
                "spec_node_hash": "new-spec-node-hash",
                "source": {
                    "bucket": "connect-dev-output",
                    "prefix": "output",
                    "locator": "s3://connect-dev-output/output",
                    "sites": ["test"],
                    "groups": ["test"],
                    "participants": [],
                    "entities": [],
                    "entity_group_map": {},
                },
                "spec_node": {
                    "source": {
                        "bucket": "connect-dev-output",
                        "prefix": "output",
                        "locator": "s3://connect-dev-output/output",
                        "sites": ["test"],
                        "groups": ["test"],
                        "participants": [],
                        "entities": [],
                        "entity_group_map": {},
                    }
                },
            }
            (old / "pipeline_spec_manifest.json").write_text(json.dumps(old_payload), encoding="utf-8")
            (new / "pipeline_spec_manifest.json").write_text(json.dumps(new_payload), encoding="utf-8")

            strict = compare_run_directories(root / "old", root / "new")
            normalized = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization(provenance_alias_normalization=True),
            )

            self.assertFalse(strict.equivalent)
            self.assertTrue(normalized.equivalent, normalized.to_dict())

    def test_provenance_alias_normalization_mode_accepts_entity_group_map_matching_site_map(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs" / "provenance"
            new = root / "new" / "logs" / "provenance"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            old_payload = {
                "manifest_type": "pipeline_spec_manifest",
                "source": {
                    "sites": ["test"],
                    "participants": ["entity-1"],
                    "site_map": {"entity-1": "test"},
                },
            }
            new_payload = {
                "manifest_type": "pipeline_spec_manifest",
                "source": {
                    "sites": ["test"],
                    "groups": ["test"],
                    "participants": ["entity-1"],
                    "entities": ["entity-1"],
                    "site_map": {"entity-1": "test"},
                    "entity_group_map": {"entity-1": "test"},
                },
            }
            (old / "pipeline_spec_manifest.json").write_text(json.dumps(old_payload), encoding="utf-8")
            (new / "pipeline_spec_manifest.json").write_text(json.dumps(new_payload), encoding="utf-8")

            normalized = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization(provenance_alias_normalization=True),
            )

            self.assertTrue(normalized.equivalent, normalized.to_dict())

    def test_provenance_alias_normalization_mode_reports_non_redundant_entity_group_map(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs" / "provenance"
            new = root / "new" / "logs" / "provenance"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            old_payload = {
                "manifest_type": "pipeline_spec_manifest",
                "source": {
                    "sites": ["test"],
                    "participants": ["entity-1"],
                    "site_map": {"entity-1": "test"},
                },
            }
            new_payload = {
                "manifest_type": "pipeline_spec_manifest",
                "source": {
                    "sites": ["test"],
                    "groups": ["test"],
                    "participants": ["entity-1"],
                    "entities": ["entity-1"],
                    "site_map": {"entity-1": "test"},
                    "entity_group_map": {"entity-1": "other"},
                },
            }
            (old / "pipeline_spec_manifest.json").write_text(json.dumps(old_payload), encoding="utf-8")
            (new / "pipeline_spec_manifest.json").write_text(json.dumps(new_payload), encoding="utf-8")

            report = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization(provenance_alias_normalization=True),
            )

            self.assertFalse(report.equivalent)
            self.assertEqual([item.path for item in report.changed], ["logs/provenance/pipeline_spec_manifest.json"])

    def test_provenance_alias_normalization_mode_reports_non_redundant_source_locator(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs" / "provenance"
            new = root / "new" / "logs" / "provenance"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            (old / "pipeline_spec_manifest.json").write_text(
                json.dumps(
                    {
                        "manifest_type": "pipeline_spec_manifest",
                        "source": {"bucket": "connect-dev-output", "prefix": "output"},
                    }
                ),
                encoding="utf-8",
            )
            (new / "pipeline_spec_manifest.json").write_text(
                json.dumps(
                    {
                        "manifest_type": "pipeline_spec_manifest",
                        "source": {
                            "bucket": "connect-dev-output",
                            "prefix": "output",
                            "locator": "s3://different-bucket/output",
                        },
                    }
                ),
                encoding="utf-8",
            )

            report = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization(provenance_alias_normalization=True),
            )

            self.assertFalse(report.equivalent)
            self.assertEqual([item.path for item in report.changed], ["logs/provenance/pipeline_spec_manifest.json"])

    def test_provenance_alias_normalization_mode_accepts_derivable_document_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs" / "provenance"
            new = root / "new" / "logs" / "provenance"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            old_payload = {
                "control_documents": [
                    {
                        "role": "pipeline_spec_manifest",
                        "locator": "s3://bucket/path/pipeline_spec_manifest.json",
                        "document_hash": "same",
                    }
                ]
            }
            new_payload = {
                "control_documents": [
                    {
                        "role": "pipeline_spec_manifest",
                        "locator": "s3://bucket/path/pipeline_spec_manifest.json",
                        "document_hash": "same",
                        "document_type": "json",
                    }
                ]
            }
            (old / "operation_event.json").write_text(json.dumps(old_payload), encoding="utf-8")
            (new / "operation_event.json").write_text(json.dumps(new_payload), encoding="utf-8")

            strict = compare_run_directories(root / "old", root / "new")
            normalized = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization(provenance_alias_normalization=True),
            )

            self.assertFalse(strict.equivalent)
            self.assertTrue(normalized.equivalent, normalized.to_dict())

    def test_provenance_alias_normalization_mode_reports_non_derivable_document_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs" / "provenance"
            new = root / "new" / "logs" / "provenance"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            (old / "operation_event.json").write_text(
                json.dumps(
                    {
                        "control_documents": [
                            {
                                "role": "pipeline_spec_manifest",
                                "locator": "s3://bucket/path/pipeline_spec_manifest.json",
                                "document_hash": "same",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (new / "operation_event.json").write_text(
                json.dumps(
                    {
                        "control_documents": [
                            {
                                "role": "pipeline_spec_manifest",
                                "locator": "s3://bucket/path/pipeline_spec_manifest.json",
                                "document_hash": "same",
                                "document_type": "yaml",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            report = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization(provenance_alias_normalization=True),
            )

            self.assertFalse(report.equivalent)
            self.assertEqual([item.path for item in report.changed], ["logs/provenance/operation_event.json"])

    def test_provenance_alias_normalization_mode_still_reports_semantic_coverage_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old" / "logs" / "provenance"
            new = root / "new" / "logs" / "provenance"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            (old / "coverage_summary.json").write_text(
                json.dumps(
                    {
                        "manifest_type": "coverage_summary",
                        "dataset_id": "old-dataset",
                        "document_hash": "old-hash",
                        "coverage": {"content_summary": {"participant_count": 1, "site_count": 1, "file_count": 2}},
                    }
                ),
                encoding="utf-8",
            )
            (new / "coverage_summary.json").write_text(
                json.dumps(
                    {
                        "manifest_type": "coverage_summary",
                        "dataset_id": "new-dataset",
                        "document_hash": "new-hash",
                        "coverage": {
                            "content_summary": {
                                "participant_count": 2,
                                "entity_count": 2,
                                "site_count": 1,
                                "group_count": 1,
                                "file_count": 2,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            report = compare_run_directories(
                root / "old",
                root / "new",
                normalization=ParityNormalization(provenance_alias_normalization=True),
            )

            self.assertFalse(report.equivalent)
            self.assertEqual([item.path for item in report.changed], ["logs/provenance/coverage_summary.json"])

    def test_cli_exits_nonzero_for_differences(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old"
            new = root / "new"
            old.mkdir()
            new.mkdir()
            (old / "out.txt").write_text("old\n", encoding="utf-8")
            (new / "out.txt").write_text("new\n", encoding="utf-8")
            report_path = root / "report.json"

            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/pipeline_parity_compare.py",
                    "--old",
                    str(old),
                    "--new",
                    str(new),
                    "--report",
                    str(report_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 1)
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertFalse(payload["equivalent"])
            self.assertEqual(payload["changed"][0]["path"], "out.txt")

    def test_cli_accepts_normalization_options(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old"
            new = root / "new"
            old.mkdir()
            new.mkdir()
            (old / "manifest.json").write_text(
                json.dumps({"run_id": "old-run", "output": "s3://bucket/old/out.csv"}),
                encoding="utf-8",
            )
            (new / "manifest.json").write_text(
                json.dumps({"run_id": "new-run", "output": "s3://bucket/new/out.csv"}),
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/pipeline_parity_compare.py",
                    "--old",
                    str(old),
                    "--new",
                    str(new),
                    "--normalize-pair",
                    "s3://bucket/old/=s3://bucket/new/",
                    "--provenance-alias-normalization",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
