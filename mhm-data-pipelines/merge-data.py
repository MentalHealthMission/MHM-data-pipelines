#!/usr/bin/env python3

import argparse
import os
import sys
import logging
import re
from datetime import datetime
import pandas as pd
import gzip
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_file_path(file_path: str, input_dir: str):
    """
    Parses the file path to extract site, participant ID, metric, and timestamp.
    """
    relative_path = os.path.relpath(file_path, input_dir)
    path_parts = relative_path.strip(os.sep).split(os.sep)

    if len(path_parts) < 4:
        logger.debug(f"File path '{file_path}' does not have enough parts to parse.")
        return None

    site = path_parts[1]
    participant_id = path_parts[2]
    metric = path_parts[3]

    filename = path_parts[-1]
    match = re.search(r'(\d{8}_\d{4})(?:_\d+)?\.csv\.gz$', filename)
    
    if match:
        timestamp_str = match.group(1)
        try:
            timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M')
        except ValueError:
            logger.warning(f"Invalid timestamp format in file: {file_path}")
            return None
    else:
        logger.debug(f"Filename does not match expected pattern: {filename}")
        return None

    return {
        'site': site,
        'participant_id': participant_id,
        'metric': metric,
        'timestamp': timestamp,
        'file_path': file_path
    }

def process_metric(metric_files, output_dir, site, participant_id, metric, output_format='csv', update=False):
    """
    Processes and merges data per metric.
    """
    logger.info(f"Processing {site}/{participant_id}/{metric}")

    if output_format == 'csv':
        output_file = os.path.join(output_dir, f"{metric}.csv.gz")
    elif output_format == 'parquet':
        output_file = os.path.join(output_dir, f"{metric}.parquet")
    else:
        logger.error(f"Unsupported output format: {output_format}")
        return

    if not update and os.path.exists(output_file):
        logger.info(f"Output file '{output_file}' already exists. Skipping.")
        return

    data_frames = []
    for file_info in metric_files:
        timestamp = file_info['timestamp']
        file_path = file_info['file_path']

        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as gz:
                df = pd.read_csv(gz)

            df['file_timestamp'] = timestamp.isoformat()
            df['site'] = site
            df['participant_id'] = participant_id

            data_frames.append(df)
        except Exception as e:
            logger.error(f"Error processing '{file_path}': {e}")

    if data_frames:
        merged_df = pd.concat(data_frames, ignore_index=True)
        os.makedirs(output_dir, exist_ok=True)
89
        if output_format == 'csv':
            merged_df.to_csv(output_file, index=False, compression='gzip')
        elif output_format == 'parquet':
            merged_df.to_parquet(output_file, index=False)

        logger.info(f"Wrote file '{output_file}'")
    else:
        logger.info(f"No data to merge for {site}/{participant_id}/{metric}")

def main():
    parser = argparse.ArgumentParser(description="Process and merge metric data from local directories.")
    parser.add_argument('--input-dir', type=str, required=True, help='Input directory containing data files')
    parser.add_argument('--output-dir', type=str, required=True, help='Output directory')
    parser.add_argument('--exclude', type=str, help='Comma-separated list of directory names to exclude')
    parser.add_argument('--include', type=str, help='Comma-separated list of directory names to include')
    parser.add_argument('--output-format', type=str, choices=['csv', 'parquet'], default='csv', help='Output file format')
    parser.add_argument('--update', action='store_true', help='Process all directories, even if merged output already exists')
    args = parser.parse_args()

    input_dir = args.input_dir

    exclude_list = args.exclude.split(',') if args.exclude else []
    include_list = args.include.split(',') if args.include else []
    exclude_list = [item.strip() for item in exclude_list]
    include_list = [item.strip() for item in include_list]

    files_by_site_participant_metric = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    # Walk the input directory
    for root, dirs, files in os.walk(input_dir):
        relative_dir = os.path.relpath(root, input_dir)
        dir_parts = relative_dir.strip(os.sep).split(os.sep)

        # Exclude directories at all levels
        if any(part in exclude_list for part in dir_parts):
            logger.info(f"Skipping directory '{root}' due to exclude list")
            dirs[:] = []  # Prevent recursion
            continue

        # If this directory contains files, then apply inclusion check
        if files and include_list:
            if not any(part in include_list for part in dir_parts):
                logger.info(f"Skipping directory '{root}' as it does not match include list")
                continue

        # Process files
        for filename in files:
            if filename.endswith('.csv.gz'):
                file_path = os.path.join(root, filename)
                file_info = parse_file_path(file_path, input_dir)
                if not file_info:
                    logger.info(f"Skipping file '{file_path}' as it could not be parsed")
                    continue

                site = file_info['site']
                participant_id = file_info['participant_id']
                metric = file_info['metric']
                files_by_site_participant_metric[site][participant_id][metric].append(file_info)

    logger.info("Starting to process metrics...")
    for site, participants in files_by_site_participant_metric.items():
        for participant_id, metrics in participants.items():
            for metric, files_info in metrics.items():
                metric_output_dir = os.path.join(args.output_dir, site, participant_id, metric)
                process_metric(
                    metric_files=files_info,
                    output_dir=metric_output_dir,
                    site=site,
                    participant_id=participant_id,
                    metric=metric,
                    output_format=args.output_format,
                    update=args.update
                )

    logger.info("Data processing complete.")

if __name__ == '__main__':
    main()
