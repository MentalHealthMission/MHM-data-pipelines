# MHM Pipelines

Generic Mental Health Mission pipeline runtime extracted from the CONNECT
pipeline refactor.

This repository contains the reusable pipeline kernel, profile/plugin contracts,
object-store and queue abstractions, publishing contracts, and parity digest
helpers. CONNECT-specific profiles, steps, deployment scripts, and production
specs live in CONNECT repositories.

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

This branch is an extraction-review branch generated from
`connect-summary@011391223d0acaa28eb4c19ad5cd3e8f3e022d0b`.

Run the lightweight checks with:

```sh
python scripts/check_minimal_pipeline_kernel.py
python scripts/check_pipeline_package_contract.py
python -m unittest tests.test_core_hello_pipeline tests.test_pipeline_parity
```

