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
    Expected file path format:
    [input_dir]/[some-top-level-directories]/SITE/Participant-id/metric/[possibly-intermediate-directories]/data-file--timestamp[_i].csv.gz
    """
    # Remove the input_dir part from the path
    relative_path = os.path.relpath(file_path, input_dir)
    path_parts = relative_path.strip(os.sep).split(os.sep)

    # Ensure there are enough parts to parse
    if len(path_parts) < 4:
        logger.debug(f"File path '{file_path}' does not have enough parts to parse.")
        return None  # Path does not have enough parts

    site = path_parts[1]  # Assuming site is always at index 1
    participant_id = path_parts[2]
    metric = path_parts[3]  # Metric is at index 3

    # Extract the filename
    filename = path_parts[-1]

    # Use regex to match the timestamp in the filename, handling optional '_i' index
    match = re.search(r'(\d{8}_\d{4})(?:_\d+)?\.csv\.gz$', filename)
    if match:
        timestamp_str = match.group(1)
        # Parse the timestamp string into a datetime object
        try:
            timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M')
            logger.debug(f"Parsed timestamp '{timestamp}' from file '{file_path}'")
        except ValueError:
            logger.warning(f"Invalid timestamp format in file: {file_path}")
            return None
    else:
        # If filename doesn't match the expected pattern, skip
        logger.debug(f"Filename does not match expected pattern: {filename}")
        return None

    logger.info(f"parsed: '{site}' : '{participant_id}' : '{metric}' : '{timestamp}'")
    return {
        'site': site,
        'participant_id': participant_id,
        'metric': metric,
        'timestamp': timestamp,
        'file_path': file_path,
        'path_parts': path_parts  # Include path parts for matching
    }

def process_metric(metric_files, output_dir, site, participant_id, metric, output_format='csv'):
    """
    Processes all files for a given metric.
    Unzips and merges data into a single file.
    Adds file_timestamp, site, and participant_id columns.
    """
    logger.info(f"Processing {site}/{participant_id}/{metric}")
    data_frames = []
    for file_info in metric_files:
        timestamp = file_info['timestamp']
        file_path = file_info['file_path']

        logger.info(f"Processing file '{file_path}'")

        try:
            # Open the compressed file
            with gzip.open(file_path, 'rt', encoding='utf-8') as gz:
                # Read CSV data into DataFrame
                df = pd.read_csv(gz)

            # Add additional columns
            df['file_timestamp'] = timestamp.isoformat()
            df['site'] = site
            df['participant_id'] = participant_id

            data_frames.append(df)

        except Exception as e:
            logger.error(f"Error processing '{file_path}': {e}")

    if data_frames:
        # Concatenate all DataFrames
        merged_df = pd.concat(data_frames, ignore_index=True)

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Write merged data to file
        if output_format == 'csv':
            output_file = os.path.join(output_dir, f"{metric}.csv.gz")
            logger.info(f"Writing compressed CSV to '{output_file}'")
            merged_df.to_csv(output_file, index=False, compression='gzip')
        elif output_format == 'parquet':
            output_file = os.path.join(output_dir, f"{metric}.parquet")
            logger.info(f"Writing Parquet file to '{output_file}'")
            merged_df.to_parquet(output_file, index=False)
        else:
            logger.error(f"Unsupported output format: {output_format}")
            return

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
    args = parser.parse_args()

    input_dir = args.input_dir

    # Process exclude and include arguments
    exclude_list = args.exclude.split(',') if args.exclude else []
    include_list = args.include.split(',') if args.include else []
    exclude_list = [item.strip() for item in exclude_list]
    include_list = [item.strip() for item in include_list]

    files_by_site_participant_metric = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    # Walk through the input directory
    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith('.csv.gz'):
                file_path = os.path.join(root, filename)
                relative_dir = os.path.relpath(root, input_dir)
                relative_dir_parts = relative_dir.strip(os.sep).split(os.sep)

                file_info = parse_file_path(file_path, input_dir)
                if not file_info:
                    logger.info(f"Skipping file '{file_path}' as it could not be parsed")
                    continue  # Skip if parsing failed

                site = file_info['site']
                participant_id = file_info['participant_id']
                metric = file_info['metric']
                path_parts = file_info['path_parts']

                # Exclude or include if specified
                if exclude_list:
                    if any(part in exclude_list for part in path_parts):
                        logger.info(f"Excluding file '{file_path}' due to exclude list")
                        continue
                if include_list:
                    if not any(part in include_list for part in path_parts):
                        logger.info(f"Excluding file '{file_path}' as it does not match include list")
                        continue

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
                    output_format=args.output_format
                )

    logger.info("Data processing complete.")

if __name__ == '__main__':
    main()
