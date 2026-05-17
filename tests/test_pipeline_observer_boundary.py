from __future__ import annotations

import ast
import subprocess
import sys
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

    def test_connect_profile_supplies_connect_provenance_observer(self) -> None:
        from connect_summary.pipeline.provenance_observer import ConnectProvenanceObserver
        from mhm_core.pipeline.observers import NoOpPipelineObserver
        from mhm_core.pipeline.plugins import load_pipeline_observer

        self.assertIsInstance(load_pipeline_observer("base"), NoOpPipelineObserver)
        self.assertIsInstance(load_pipeline_observer("connect"), ConnectProvenanceObserver)


if __name__ == "__main__":
    unittest.main()
