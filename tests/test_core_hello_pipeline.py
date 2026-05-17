from __future__ import annotations

import ast
import json
import subprocess
import sys
import unittest
from pathlib import Path


class CoreHelloPipelineHarnessTests(unittest.TestCase):
    def test_hello_pipeline_example_has_no_project_imports(self) -> None:
        offenders: list[str] = []
        for path in sorted(Path("examples/hello_pipeline").rglob("*.py")):
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

    def test_hello_pipeline_runs_through_core_without_project_package(self) -> None:
        code = r"""
import json
import sys
import tempfile
from pathlib import Path

from examples.hello_pipeline.pipeline_plugin import HELLO_NAMESPACE, HelloPipelineProfile
from mhm_core.pipeline.context import create_run_context, extension_state
from mhm_core.pipeline.plugins import (
    load_pipeline_observer,
    load_pipeline_publisher,
    register_profile_plugin,
)
from mhm_core.pipeline.runner import _build_batches, _run_step_with_timing
from mhm_core.pipeline.spec import RunSpec, validate_spec
from mhm_core.pipeline.steps import build_steps


class FakeSession:
    def client(self, service_name):
        if service_name != "s3":
            raise AssertionError(service_name)
        return object()


entities = [
    "00000000-0000-0000-0000-000000000101",
    "00000000-0000-0000-0000-000000000102",
]
groups = {
    entities[0]: "alpha-group",
    entities[1]: "beta-group",
}

register_profile_plugin("hello", HelloPipelineProfile)
spec = RunSpec.from_dict(
    {
        "run_id": "hello-core-harness",
        "profile": "hello",
        "created_by": "tests",
        "created_at": "2026-05-17T00:00:00Z",
        "priority": "medium",
        "source": {
            "bucket": "hello-source",
            "prefix": "collections",
            "participants": entities,
            "sites": sorted(set(groups.values())),
        },
        "workspace": {
            "root": "/tmp",
            "run_subdir": "hello-core-harness/{run_id}",
        },
        "outputs": {
            "merged_prefix": "hello://merged/{site}/{participant_id}/",
            "summary_prefix": "hello://unused-required-output/{site}/{participant_id}/",
            "manifest_key": "hello://run-manifests/{run_id}.json",
            "logs_prefix": "hello://run-logs/{run_id}/",
        },
        "processing": {
            "steps": [
                {"type": "hello.collect", "label": "package rehearsal"},
                {"type": "hello.render"},
                {"type": "publish"},
            ]
        },
        "publishing": {
            "delete_local_workspace": False,
            "remove_local_raw_after_publish": False,
            "remove_local_merged_after_publish": False,
            "remove_local_summary_after_publish": False,
        },
    }
)
spec.source.site_map = groups
errors = validate_spec(spec)
if errors:
    raise AssertionError(errors)

with tempfile.TemporaryDirectory() as tmp_dir:
    spec.workspace.root = Path(tmp_dir)
    context = create_run_context(spec, boto3_session=FakeSession(), spec_locator="hello://spec")
    context.pipeline_observer = load_pipeline_observer(spec.profile)
    context.pipeline_publisher = load_pipeline_publisher(spec.profile)
    context.pipeline_observer.on_run_start(context)

    steps = build_steps(spec)
    context.metrics = {step.name: {} for step in steps}
    batches = _build_batches(spec, list(spec.iter_participants()), context.participant_sites)

    for batch in batches:
        context.batch_participants = list(batch)
        for entity_id in batch:
            context.current_participant = entity_id
            for step_index, step in enumerate(steps, start=1):
                pre_step_state = context.pipeline_observer.before_step(
                    context,
                    step=step,
                    step_index=step_index,
                )
                metrics = _run_step_with_timing(context, step)
                context.metrics[step.name][entity_id] = metrics
                context.pipeline_observer.after_step(
                    context,
                    step=step,
                    step_index=step_index,
                    metrics=metrics,
                    pre_step_state=pre_step_state,
                )

    state = extension_state(context, HELLO_NAMESPACE)
    published_root = context.workspace_dir / "published"
    published_files = sorted(
        path.relative_to(published_root).as_posix()
        for path in published_root.rglob("*")
        if path.is_file()
    )
    payload = {
        "steps": [step.name for step in steps],
        "batches": batches,
        "record_count": len(state.get("records", {})),
        "report_count": len(state.get("reports", {})),
        "published_artifact_count": len(state.get("published_artifacts", [])),
        "publish_results": state.get("publish_results", {}),
        "publish_metrics": context.metrics["publish"],
        "published_files": published_files,
        "loaded_project_modules": sorted(
            name for name in sys.modules if name.startswith("connect_summary")
        ),
    }

if payload["loaded_project_modules"]:
    raise AssertionError(payload["loaded_project_modules"])
print(json.dumps(payload, sort_keys=True))
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
        payload = json.loads(result.stdout)

        self.assertEqual(payload["loaded_project_modules"], [])
        self.assertEqual(payload["steps"], ["hello_collect", "hello_render", "publish"])
        self.assertEqual(payload["batches"], [["00000000-0000-0000-0000-000000000101", "00000000-0000-0000-0000-000000000102"]])
        self.assertEqual(payload["record_count"], 2)
        self.assertEqual(payload["report_count"], 2)
        self.assertEqual(payload["published_artifact_count"], 2)
        for entity_id, publish_result in payload["publish_results"].items():
            self.assertTrue(entity_id.startswith("00000000-0000-0000-0000-00000000010"))
            self.assertEqual(len(publish_result["report_keys"]), 1)
            self.assertTrue(publish_result["participant_manifest_published"])
        for metrics in payload["publish_metrics"].values():
            self.assertEqual(metrics["merged_files"], 1)
            self.assertEqual(metrics["hello_report_files"], 1)
            self.assertEqual(metrics["published_target_files"], {"hello_report": 1})
        self.assertTrue(
            any(path.endswith("/greeting_artifacts/greeting.txt") for path in payload["published_files"]),
            payload["published_files"],
        )
        self.assertTrue(
            any(path.endswith("_hello_report.json") for path in payload["published_files"]),
            payload["published_files"],
        )


if __name__ == "__main__":
    unittest.main()
