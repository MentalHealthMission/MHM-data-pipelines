# MHM Pipelines

Reusable Python pipeline runtime for Mental Health Mission data processing.

This repository contains the reusable pipeline kernel, profile/plugin contracts,
object-store and queue abstractions, publishing contracts, and parity digest
helpers. It is intended to support project-specific pipeline profiles without
baking those profiles into the core runtime.

## What This Package Owns

- pipeline specification loading and validation
- execution contexts, run manifests, and refresh plans
- object-store and queue interfaces
- publish targets and observer hooks
- parity/digest helpers for no-data regression checks
- a small "hello world" pipeline used as a neutral contract test

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

Run the lightweight checks with:

```sh
python scripts/check_minimal_pipeline_kernel.py
python scripts/check_pipeline_package_contract.py
python -m unittest tests.test_core_hello_pipeline tests.test_pipeline_parity
```
