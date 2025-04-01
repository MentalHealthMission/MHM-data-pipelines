#!/usr/bin/env python3

import os
import gzip
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from collections import defaultdict
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Extract data presence heatmap from merged CONNECT directories.")
    parser.add_argument('--input-dir', required=True, help='Path to merged data')
    parser.add_argument('--output-csv', required=True, help='Path to output CSV file')
    parser.add_argument('--data-prefix', required=True, help='Prefix for metrics to match (e.g., sensorkit_)')
    parser.add_argument('--include', type=str, help='Comma-separated list of participant IDs to include')
    parser.add_argument('--exclude', type=str, help='Comma-separated list of participant IDs to exclude')
    parser.add_argument('--heatmap-file', type=str, help='Optional path to save heatmap image')
    parser.add_argument('--from-merged', action='store_true', help='If set, extract days from file contents instead of filenames')

    return parser.parse_args()

def collect_days_from_filename(file_path):
    """
    Extract the date from a filename formatted as yyyymmdd_hhmm.csv.gz
    Returns a set with a single date string (YYYY-MM-DD) if matched, else empty set.
    """
    logger.info(f"Processing file: {file_path}")
    filename = os.path.basename(file_path)
    match = re.match(r'(\d{8})_\d{4}\.csv\.gz$', filename)
    if match:
        try:
            date_str = match.group(1)
            date_obj = datetime.strptime(date_str, "%Y%m%d").date()
            return {date_obj.isoformat()}
        except Exception as e:
            logger.warning(f"Failed to parse date from filename '{filename}': {e}")
    else:
        logger.debug(f"Filename does not match expected pattern: {filename}")
    return set()

def collect_days(file_path, time_fields):
    try:
        logger.info(f"Processing file: {file_path}")
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            df = pd.read_csv(f, low_memory=False)  # Read all columns
            for col in time_fields:
                if col in df.columns:
                    try:
                        dt_series = pd.to_datetime(df[col], unit='s', errors='coerce')
                        return set(dt_series.dropna().dt.date.astype(str))
                    except Exception:
                        continue
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return set()

def render_heatmap(df, heatmap_file):
    pivot = df.pivot_table(index='participant_id', columns='date', values='metric', aggfunc='count', fill_value=0)
    plt.figure(figsize=(20, 10))
    sns.heatmap(pivot, cmap="YlGnBu", linewidths=0.5, linecolor='gray')
    plt.title('Data Availability Heatmap')
    plt.xlabel('Date')
    plt.ylabel('Participant')
    plt.tight_layout()
    plt.savefig(heatmap_file)
    logger.info(f"Saved heatmap to {heatmap_file}")

def main():
    args = parse_args()
    include = set(s.strip() for s in args.include.split(",")) if args.include else None
    exclude = set(s.strip() for s in args.exclude.split(",")) if args.exclude else set()

    rows = []

    for root, dirs, files in os.walk(args.input_dir):
        if not files:
            continue

        parts = root.split(os.sep)
        if len(parts) < 3:
            continue

        site = parts[-3]
        participant_id = parts[-2]
        metric = parts[-1]

        if include and participant_id not in include:
            continue
        if participant_id in exclude:
            continue
        if not metric.startswith(args.data_prefix):
            continue

        for file in files:
            if not file.endswith('.csv.gz'):
                continue

            file_path = os.path.join(root, file)

            if args.from_merged:
                time_fields = ['timestamp', 'value.time', 'value.startTime', 'value.timeCompleted', 'time', 'timeReceived']
                dates = collect_days(file_path, time_fields)
            else:
                dates = collect_days_from_filename(file)

            for date in dates:
                rows.append({
                    'site': site,
                    'participant_id': participant_id,
                    'metric': metric,
                    'date': date
                })

    df = pd.DataFrame(rows)
    df.to_csv(args.output_csv, index=False)
    print(f"Wrote {len(df)} rows to {args.output_csv}")

    if args.heatmap_file:
        render_heatmap(df, args.heatmap_file)
        
if __name__ == "__main__":
    main()
