# MHM Data Pipelines

## Overview

This repository contains scripts and utilities developed as part of the Mental Health Mission (MHM) programme for managing and processing wearable and smartphone data. The current implementation supports data collected for the CONNECT study and is structured to evolve into a general-purpose pipeline for handling digital phenotyping data in mental health research.

The codebase includes tools for:

- Extracting participant-level data summaries  
- Merging raw data from structured directories  
- Interacting with S3 buckets used for storage and replication  
- Generating heatmaps of data availability  
- Managing AWS authentication for secure access  

The scripts assume a hierarchical directory structure based on site, participant ID, and metric type. Data files are typically in `.csv.gz` format and may be merged (longitudinal) or disaggregated by timestamped filenames.

Future versions will generalise the input data structure and extend support for additional studies and data sources under the MHM programme.

## Installation

It is recommended to use a Python virtual environment. To get started:

```bash
# Clone the repository
git clone https://github.com/MentalHealthMission/MHM-data-pipelines
cd MHM-data-piplines

# Create a virtual environment
python -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

## CONNECT scripts

The scripts listed below were developed to support data processing tasks for the CONNECT study, including data download, merging, summarisation, and metadata extraction. Each script is focused on a specific function within the data pipeline.

### `collect_data_metadata.py`

Traverses local CONNECT data directories, parsing file names and content to extract basic statistics (row counts, date ranges, unique days with data) per participant, per metric. Merges device-level metrics where applicable. Outputs statistics grouped by site, including a combined summary across all sites.

### `data_collection.py`

Extracts a list of participant-days for metrics matching a given prefix (e.g. `sensorkit_`) in a merged data structure. Can optionally render a pivoted CSV heatmap showing days of data per participant and metric.

### `download_data.py`

Downloads CONNECT study data from a specified S3 bucket path into a local directory (e.g. mapped RDS drive). Supports filtering by site name using `--include-sites` and `--exclude-sites`. Uses concurrency for parallel downloads and skips files already present locally.

### `extract_patient_summary.py`

Processes merged participant-level directories to extract summary statistics (per time period) across features, sleep, and questionnaire data. Outputs a JSON summary per participant per time period. Supports flexible `--feature`, `--questionnaire-slider`, and `--questionnaire-histogram` flags to define data extraction logic.

### `main.py`

Command-line interface for querying a cached summary of CONNECT data in an S3 bucket. Uses `summary_data.pkl` to reduce repeated API calls. Can list users, measurements, schemas, and generate summary reports.

### `merge-data.py`

Merges per-day CONNECT data files into a single monthly file per participant and metric. Used to create a structure suitable for downstream summarisation.

### `process-overview.py`

Summarises CONNECT data processing state, checking for presence of expected merged files. Lists missing or incomplete data for participants across metrics.

### `regenerate_aws_session_token.py`

Helper script to refresh temporary AWS credentials in the `default` profile using a long-term credential profile (`long-term`) and an MFA code. Reads the MFA ARN from `config/config.ini`.

### `set_aws_mfa_env.py`

Initialises AWS credentials with MFA support. Copies long-term credentials to a named profile (`long-term`) if not already present, prompts for an MFA code, and writes temporary credentials to the default profile. Requires `config/config.ini` to be configured with a valid MFA ARN.

### `summary.py`

Provides interactive CLI commands for analysing CONNECT S3 bucket structure. Tracks file counts, date ranges, and schema presence per measurement per participant. Allows schema viewing and summary generation. Supports caching via `summary_data.pkl`.
