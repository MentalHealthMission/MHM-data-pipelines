from __future__ import annotations

import gzip
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from mhm_core.pipeline.parity import compare_run_directories


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


if __name__ == "__main__":
    unittest.main()
