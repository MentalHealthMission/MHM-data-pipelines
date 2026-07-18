from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import yaml

from mhm_core.pipeline.context import RunContext
from mhm_core.pipeline.integrations.ontology_select import OntologySelectStep


class OntologyRuntimePathTests(unittest.TestCase):
    def test_derived_feature_root_is_bound_by_the_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            unification = root / "unification.yaml"
            unification.write_text(
                yaml.safe_dump(
                    {
                        "features": [
                            {
                                "id": "daily_steps",
                                "odim_feature": "odim:DailyStepCount",
                                "method": "column",
                                "output_column": "total_steps",
                                "inputs": [
                                    {
                                        "path": "{derived_features_dir}/{participant_id}/steps.csv",
                                        "value_column": "total_steps",
                                    }
                                ],
                            }
                        ]
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            output_plan = root / "feature-plan.yaml"
            derived_root = root / "slice-derived-features"
            context = RunContext(
                spec=SimpleNamespace(iter_entities=lambda: [], iter_participants=lambda: []),
                run_id="runtime-path-test",
                workspace_dir=root / "workspace",
                raw_dir=root / "raw",
                merged_dir=root / "merged",
                summary_dir=root / "summary",
                latest_measurement_dir=root / "latest",
                logs_dir=root / "logs",
                s3_client=object(),
            )

            OntologySelectStep(
                {
                    "unification_spec": str(unification),
                    "request": {"features": ["daily_steps"]},
                    "derived_features_dir": str(derived_root),
                    "output_plan": str(output_plan),
                }
            ).run(context)

            plan = yaml.safe_load(output_plan.read_text(encoding="utf-8"))
            self.assertEqual(
                plan["features"][0]["inputs"][0]["path"],
                f"{derived_root}/{{participant_id}}/steps.csv",
            )


if __name__ == "__main__":
    unittest.main()
