# MHM Pipelines

Python tools for defining and running file-backed data pipelines.

Use this package to load pipeline specifications, execute pipeline steps,
record run manifests, publish outputs, and compare runs without inspecting
participant-level data.

## What You Can Do

- load and validate pipeline specifications
- build execution contexts and refresh plans
- read and write run manifests
- plug in object-store and queue backends
- publish outputs through explicit targets and observer hooks
- generate parity digests for no-data regression checks
- run the included hello-world pipeline example

## Install

```sh
python -m venv .venv
. .venv/bin/activate
pip install -e .
python -m mhm_core.pipeline.runner --help
```

For optional object-store/S3 support:

```sh
pip install -e ".[object-store]"
```

## Development

Run the lightweight checks:

```sh
python scripts/check_minimal_pipeline_kernel.py
python scripts/check_pipeline_package_contract.py
python -m unittest tests.test_core_hello_pipeline tests.test_pipeline_parity
```
