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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def file_passes_include_exclude(path_parts, include_list, exclude_list):
    """
    Returns True if this file path (split into path_parts) should be included,
    based on the include_list and exclude_list rules:
      - If exclude_list is non-empty, skip if any path_part is in exclude_list.
      - If include_list is non-empty, only include if at least one path_part is in include_list.
    """
    # Exclude check
    if exclude_list:
        if any(part in exclude_list for part in path_parts):
            return False

    # Include check
    if include_list:
        if not any(part in include_list for part in path_parts):
            return False

    return True

def parse_file_path(file_path: str, input_dir: str):
    """
    Parses the file path to extract site, participant, metric from the directories, e.g.:
      [input_dir]/.../SITE/Participant-ID/metric/.../filename.csv.gz
    Also returns path_parts for further checks and possibly a parsed timestamp from the filename.
    """
    relative_path = os.path.relpath(file_path, input_dir)
    path_parts = relative_path.strip(os.sep).split(os.sep)

    if len(path_parts) < 4:
        logger.debug(f"File path '{file_path}' does not have enough parts.")
        return None

    site = path_parts[1]
    participant = path_parts[2]
    metric = path_parts[3]

    filename = path_parts[-1]
    match = re.search(r'(\d{8}_\d{4})(?:_\d+)?\.csv\.gz$', filename)
    parsed_timestamp = None
    if match:
        ts_str = match.group(1)
        try:
            parsed_timestamp = datetime.strptime(ts_str, '%Y%m%d_%H%M')
        except ValueError:
            logger.warning(f"Timestamp format invalid in filename: {file_path}")

    return {
        'site': site,
        'participant': participant,
        'metric': metric,
        'parsed_timestamp': parsed_timestamp,  # optional
        'file_path': file_path,
        'path_parts': path_parts
    }

def parse_time_col_as_s(series: pd.Series) -> pd.Series:
    """
    For timestamps like 1729146600.70951 in 'value.time',
    interpret as float seconds since epoch (with fractional part).
    """
    numeric = pd.to_numeric(series, errors='coerce')
    dt_series = pd.to_datetime(numeric, unit='s', errors='coerce')
    return dt_series

def gather_file_stats(file_info):
    """
    Reads a .csv.gz file, looks for 'value.time',
    interprets it as seconds since epoch, groups by device if a 'device' column is present,
    returns a list of stats dicts:
      [
        {
          'site': site,
          'participant': participant,
          'metric': metric (or metric/device),
          'row_count': int,
          'start_date': str or None,
          'end_date': str or None,
          'day_set': set of distinct day strings (e.g. '2023-08-14')
        },
        ...
      ]
    """
    site = file_info['site']
    participant = file_info['participant']
    metric_base = file_info['metric']
    file_path = file_info['file_path']

    results = []
    logger.info(f"Reading file '{file_path}'")

    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as gz:
            df = pd.read_csv(gz)

        # Attempt to interpret 'value.time' as date/time
        if 'value.time' in df.columns:
            dt_series = parse_time_col_as_s(df['value.time'])
        else:
            dt_series = pd.Series([], dtype='datetime64[ns]')

        dt_series = dt_series.dropna()
        if not dt_series.empty:
            start_dt = dt_series.min().isoformat()
            end_dt = dt_series.max().isoformat()
            # day_set => store normalized day strings
            normalized_days = dt_series.dt.normalize().dt.strftime('%Y-%m-%d')
            day_set = set(normalized_days)
        else:
            start_dt = None
            end_dt = None
            day_set = set()

        # Check for device column
        device_col = None
        for col in df.columns:
            if 'device' in col.lower():
                device_col = col
                break

        if device_col:
            grouped = df.groupby(device_col)
            for dev_val, sub_df in grouped:
                row_count = len(sub_df)
                full_metric = f"{metric_base}/{dev_val}"
                # The date range is the same for the entire file,
                # but if you prefer device-level date ranges, you'd parse sub_df again.
                results.append({
                    'site': site,
                    'participant': participant,
                    'metric': full_metric,
                    'row_count': row_count,
                    'start_date': start_dt,
                    'end_date': end_dt,
                    'day_set': day_set  # The same day_set for all device groups in this file
                })
        else:
            row_count = len(df)
            results.append({
                'site': site,
                'participant': participant,
                'metric': metric_base,
                'row_count': row_count,
                'start_date': start_dt,
                'end_date': end_dt,
                'day_set': day_set
            })

    except Exception as e:
        logger.error(f"Error reading file '{file_path}': {e}")

    return results

def accumulate_stats(global_stats, file_stats):
    """
    Merges file-level stats (a list of dicts) into global_stats, keyed by (site, participant, metric).
    We sum row_count, unify date ranges, and union day sets.
    """
    for row in file_stats:
        site = row['site']
        participant = row['participant']
        metric = row['metric']
        key = (site, participant, metric)
        acc = global_stats[key]

        acc['row_count'] += row['row_count']

        sd = row['start_date']
        ed = row['end_date']
        if sd:
            if acc['start_date'] is None or sd < acc['start_date']:
                acc['start_date'] = sd
        if ed:
            if acc['end_date'] is None or ed > acc['end_date']:
                acc['end_date'] = ed

        # union day sets
        if 'day_set' not in acc:
            acc['day_set'] = set()
        acc['day_set'].update(row['day_set'])

def write_stats_per_site_and_all(global_stats, output_dir, output_format):
    """
    Writes a separate file per site plus an all_sites file, from the global_stats structure,
    only if global_stats is non-empty.
    day_count is computed as len of the day_set
    """
    if not global_stats:
        logger.info("Global stats is empty, nothing to write.")
        return

    rows = []
    for (site, participant, metric), acc in global_stats.items():
        day_count = 0
        if 'day_set' in acc and acc['day_set']:
            day_count = len(acc['day_set'])
        rows.append({
            'site': site,
            'participant': participant,
            'metric': metric,
            'row_count': acc['row_count'],
            'start_date': acc['start_date'],
            'end_date': acc['end_date'],
            'day_count': day_count
        })

    if not rows:
        logger.info("No rows to write after building from global_stats.")
        return

    df_all = pd.DataFrame(rows)
    df_all.sort_values(by=['site','participant','metric'], inplace=True)
    os.makedirs(output_dir, exist_ok=True)

    # Write one file per site
    for site_name in df_all['site'].unique():
        df_site = df_all[df_all['site'] == site_name].copy()
        if output_format == 'csv':
            out_file = os.path.join(output_dir, f"{site_name}_stats.csv.gz")
            logger.info(f"Writing stats for site='{site_name}' => {out_file}")
            df_site.to_csv(out_file, index=False, compression='gzip')
        else:
            out_file = os.path.join(output_dir, f"{site_name}_stats.parquet")
            logger.info(f"Writing stats for site='{site_name}' => {out_file}")
            df_site.to_parquet(out_file, index=False)

    # Write combined file
    if output_format == 'csv':
        combined_path = os.path.join(output_dir, "all_sites.csv.gz")
        logger.info(f"Writing combined stats => {combined_path}")
        df_all.to_csv(combined_path, index=False, compression='gzip')
    else:
        combined_path = os.path.join(output_dir, "all_sites.parquet")
        logger.info(f"Writing combined stats => {combined_path}")
        df_all.to_parquet(combined_path, index=False)

def main():
    parser = argparse.ArgumentParser(
        description="Gather stats from local CSV.GZ files, interpret 'value.time' as seconds, merge device metrics, partial writes, and exact day counts.")
    parser.add_argument('--input-dir', required=True, help='Root directory of input files')
    parser.add_argument('--output-dir', required=True, help='Directory where stats are written')
    parser.add_argument('--include', help='Comma-separated directory names to include (match any path part).')
    parser.add_argument('--exclude', help='Comma-separated directory names to exclude (match any path part).')
    parser.add_argument('--output-format', choices=['csv','parquet'], default='csv', help='Output file format')
    args = parser.parse_args()

    include_list = args.include.split(',') if args.include else []
    exclude_list = args.exclude.split(',') if args.exclude else []
    include_list = [x.strip() for x in include_list]
    exclude_list = [x.strip() for x in exclude_list]

    # We'll keep a global stats dictionary, keyed by (site, participant, metric).
    # Each entry is a dict with row_count, start_date, end_date, day_set
    global_stats = defaultdict(lambda: {
        'row_count': 0,
        'start_date': None,
        'end_date': None,
        'day_set': set()
    })

    # We'll walk directories one by one. After finishing each directory, if we actually processed any files,
    # we update the global stats and write partial stats.
    for root, dirs, files in os.walk(args.input_dir):
        logger.info(f"Processing directory: {root}")
        directory_changed = False
        directory_stats = []

        for filename in files:
            if filename.endswith('.csv.gz'):
                file_path = os.path.join(root, filename)
                f_info = parse_file_path(file_path, args.input_dir)
                if not f_info:
                    logger.debug(f"Skipping '{file_path}', parse_file_path returned None")
                    continue

                # Check the file path parts for include/exclude
                path_parts = f_info['path_parts']
                if not file_passes_include_exclude(path_parts, include_list, exclude_list):
                    logger.debug(f"Excluding file '{file_path}' due to include/exclude logic.")
                    continue

                # gather stats from file
                file_stats = gather_file_stats(f_info)
                if file_stats:
                    directory_stats.extend(file_stats)

        if directory_stats:
            logger.info(f"Directory '{root}' produced {len(directory_stats)} new stats entries.")
            directory_changed = True
            # Accumulate into global_stats
            for row in directory_stats:
                site = row['site']
                participant = row['participant']
                metric = row['metric']
                key = (site, participant, metric)
                acc = global_stats[key]

                acc['row_count'] += row['row_count']

                sd = row['start_date']
                ed = row['end_date']
                if sd:
                    if acc['start_date'] is None or sd < acc['start_date']:
                        acc['start_date'] = sd
                if ed:
                    if acc['end_date'] is None or ed > acc['end_date']:
                        acc['end_date'] = ed

                # union day sets
                acc['day_set'].update(row['day_set'])
        else:
            logger.info(f"No new stats from directory '{root}'")

        if directory_changed:
            logger.info(f"Finished directory '{root}'. Updating stats files.")
            write_stats_per_site_and_all(global_stats, args.output_dir, args.output_format)
        else:
            logger.info(f"Directory '{root}' had no changes, skipping partial write.")

    logger.info("All directories processed. Final stats are in the last partial write.")

if __name__ == '__main__':
    main()
