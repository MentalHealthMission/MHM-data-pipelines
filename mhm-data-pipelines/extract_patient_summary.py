#!/usr/bin/env python3
"""
Example usage:
python extract_patient_summary.py \
  --input-dir /data/merged \
  --output-dir /data/summaries \
  --include xxx-yyy-zzz \
  --time-resolution month \
  --feature "steps:android_health_connect_typed_data:value.time:value.key:Steps:value.intVal" \
  --feature "heart_rate:android_health_connect_typed_data:value.time:value.key:HeartRate:value.intVal" \
  --feature "screen_usage:device_app_log:timestamp:usage_duration:hours" \
  --feature "sleep_period:wearable_sleep_tracker:value.time:value.duration:hours" \
  --questionnaire "questionnaire_response:value.timeCompleted" \
  --questionnaire-slider "negative_emotions:questionnaire_response:value.answers:negative_emotions_:value:StartTime" \
  --questionnaire-histogram "sleep:questionnaire_response:value.answers:sleep_5:value:StartTime"
"""

import argparse
import os
import gzip
import json
import logging
import datetime
from datetime import date, datetime, timedelta
from collections import defaultdict
import pandas as pd
import numpy as np

# Set logging to DEBUG (minimal output for processing measures)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------- Feature Processing Functions ---------------------- #
def parse_feature_flag(flag_value):
    parts = flag_value.split(":")
    if len(parts) == 6:
        return {
            "feature": parts[0],
            "source": parts[1],
            "time_field": parts[2],
            "filter_field": parts[3],
            "filter_value": parts[4],
            "extraction_field": parts[5],
            "unit": None
        }
    elif len(parts) == 5:
        return {
            "feature": parts[0],
            "source": parts[1],
            "time_field": parts[2],
            "extraction_field": parts[3],
            "unit": parts[4],
            "filter_field": None,
            "filter_value": None
        }
    else:
        raise ValueError(f"Invalid feature flag format: {flag_value}")

def parse_questionnaire_flag(flag_value):
    parts = flag_value.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid questionnaire flag format: {flag_value}")
    return {"file_filter": parts[0], "time_field": parts[1]}

def parse_questionnaire_slider(flag_value):
    # Expected format: DOMAIN:FILE_IDENTIFIER:ANSWERS_BASE:TARGET_PREFIX:VALUE_SUFFIX:TIME_SUFFIX
    parts = flag_value.split(":")
    if len(parts) != 6:
        raise ValueError(f"Invalid questionnaire slider flag format: {flag_value}")
    return {
        "domain": parts[0],
        "file_filter": parts[1],
        "answers_base": parts[2],
        "target_prefix": parts[3],
        "value_suffix": parts[4],
        "time_suffix": parts[5]
    }

def parse_questionnaire_histogram(flag_value):
    # Expected format: DOMAIN:FILE_IDENTIFIER:ANSWERS_BASE:TARGET_QUESTIONID:VALUE_SUFFIX:TIME_SUFFIX
    parts = flag_value.split(":")
    if len(parts) != 6:
        raise ValueError(f"Invalid questionnaire histogram flag format: {flag_value}")
    return {
        "domain": parts[0],
        "file_filter": parts[1],
        "answers_base": parts[2],
        "target_questionid": parts[3],
        "value_suffix": parts[4],
        "time_suffix": parts[5]
    }

def get_time_key(dt, resolution):
    if resolution.lower() == "month":
        return dt.strftime("%Y-%m")
    elif resolution.lower() == "week":
        iso = dt.isocalendar()  # (year, week, weekday)
        return f"{iso.year}-W{iso.week:02d}"
    elif resolution.lower() == "year":
        return dt.strftime("%Y")
    else:
        raise ValueError(f"Unsupported time resolution: {resolution}")

def update_summary(summary, dt, value):
    summary["total_entries"] += 1
    try:
        numeric_val = float(value)
    except Exception:
        logger.warning(f"Non-numeric value encountered: {value}")
        return
    summary["values"].append(numeric_val)
    summary["days"].add(dt.date())

def compute_stats(values):
    if not values:
        return None, None, None, None, None
    arr = np.array(values)
    return float(np.mean(arr)), float(np.median(arr)), float(np.std(arr)), float(np.min(arr)), float(np.max(arr))

def process_csv_file(file_path, time_field, extraction_field, filter_field=None, filter_value=None):
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as gz:
            df = pd.read_csv(gz)
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return None

    if filter_field and filter_value:
        df = df[df[filter_field] == filter_value]
    if df.empty:
        return None

    try:
        df[time_field] = pd.to_datetime(df[time_field], unit='s', errors='coerce')
        df = df.dropna(subset=[time_field])
    except Exception as e:
        logger.error(f"Error parsing time field in {file_path}: {e}")
        return None

    return df


def convert_sets_to_lists(obj):
    if isinstance(obj, set):
        return [convert_sets_to_lists(x) for x in obj]
    elif isinstance(obj, tuple):
        return [convert_sets_to_lists(x) for x in obj]
    elif isinstance(obj, dict):
        # Convert dictionary keys to strings
        return {str(k): convert_sets_to_lists(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_sets_to_lists(x) for x in obj]
    elif isinstance(obj, (date, datetime)):
        return obj.isoformat()
    else:
        return obj
    
def parse_file_path(file_path, input_dir):
    # Assume structure: .../<...>/participant-id/device/file.csv.gz
    relative_path = os.path.relpath(file_path, input_dir)
    parts = relative_path.strip(os.sep).split(os.sep)
    if len(parts) < 3:
        return None
    return {
        "participant_id": parts[-3],
        "metric": parts[-2],
        "file_path": file_path,
        "path_parts": parts
    }

def matches_include(dir_parts, include_list):
    for part in dir_parts:
        for inc in include_list:
            if inc in part:
                return True
    return False

# ---------------------- Main Processing ---------------------- #
def main():
    parser = argparse.ArgumentParser(description="Extract patient summary data from merged directories.")
    parser.add_argument('--input-dir', type=str, required=True, help='Input directory containing merged data files')
    parser.add_argument('--output-dir', type=str, required=True, help='Output directory for JSON summaries')
    parser.add_argument('--include', type=str, help='Comma-separated list of directory names to include')
    parser.add_argument('--exclude', type=str, help='Comma-separated list of directory names to exclude')
    parser.add_argument('--time-resolution', type=str, default="month", help='Time resolution: month, week, or year')
    parser.add_argument('--feature', action='append', help='Feature flag in the form feature:source:time_field:(filter_field:filter_value:extraction_field OR extraction_field:unit)')
    parser.add_argument('--questionnaire', type=str, help='Simple questionnaire flag in the form file_identifier:time_field')
    parser.add_argument('--questionnaire-slider', action='append', help='Questionnaire slider flag in the form DOMAIN:FILE_IDENTIFIER:ANSWERS_BASE:TARGET_PREFIX:VALUE_SUFFIX:TIME_SUFFIX')
    parser.add_argument('--questionnaire-histogram', action='append', help='Questionnaire histogram flag in the form DOMAIN:FILE_IDENTIFIER:ANSWERS_BASE:TARGET_QUESTIONID:VALUE_SUFFIX:TIME_SUFFIX')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    exclude_list = [s.strip() for s in args.exclude.split(",")] if args.exclude else []
    include_list = [s.strip() for s in args.include.split(",")] if args.include else []
    logger.info(f"Include list: {include_list}")
    logger.info(f"Exclude list: {exclude_list}")

    # Parse feature definitions
    feature_defs = {}
    if args.feature:
        for feat in args.feature:
            feat_def = parse_feature_flag(feat)
            feature_defs[feat_def["feature"]] = feat_def

    questionnaire_def = parse_questionnaire_flag(args.questionnaire) if args.questionnaire else None

    questionnaire_slider_defs = []
    if args.questionnaire_slider:
        for flag in args.questionnaire_slider:
            questionnaire_slider_defs.append(parse_questionnaire_slider(flag))

    questionnaire_histogram_defs = []
    if args.questionnaire_histogram:
        for flag in args.questionnaire_histogram:
            questionnaire_histogram_defs.append(parse_questionnaire_histogram(flag))

    # Summary data structure keyed by (participant_id, time_key)
    summary_data = defaultdict(lambda: {
        "patient_id": None,
        "site": None,
        "data_summary": {
            "start_date": None,
            "end_date": None,
            "total_days_with_data": 0,
            "missing_days": None,
            "features_available": set()
        },
        "feature_statistics": {feat: {"total_entries": 0, "days": set(), "values": []} for feat in feature_defs.keys()},
        "questionnaire_responses": {
            "total_responses": 0,
            "days": set(),
            "slider": defaultdict(lambda: {"total_entries": 0, "days": set(), "values": []}),
            "histogram": defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        }
    })

    # Traverse input directory
    for root, dirs, files in os.walk(args.input_dir):
        relative_dir = os.path.relpath(root, args.input_dir)
        dir_parts = relative_dir.strip(os.sep).split(os.sep)

        if any(part in exclude_list for part in dir_parts):
            continue
        if files and include_list and not matches_include(dir_parts, include_list):
            continue

        for filename in files:
            if filename.endswith('.csv.gz'):
                file_path = os.path.join(root, filename)
                logger.info(f"Processing file: {file_path}")
                file_info = parse_file_path(file_path, args.input_dir)
                if not file_info:
                    continue

                parts = file_info["path_parts"]
                if len(parts) < 3:
                    continue
                participant_id = parts[-3]
                metric_dir = parts[-2]
                site = participant_id
                file_info["participant_id"] = participant_id
                file_info["metric"] = metric_dir

                # Process feature files
                for feat_name, feat_def in feature_defs.items():
                    if feat_def["source"] in file_path:
                        df = process_csv_file(
                            file_path,
                            feat_def["time_field"],
                            feat_def["extraction_field"],
                            filter_field=feat_def["filter_field"],
                            filter_value=feat_def["filter_value"]
                        )
                        if df is not None and not df.empty:
                            for _, row in df.iterrows():
                                dt = row[feat_def["time_field"]]
                                time_key = get_time_key(dt, args.time_resolution)
                                key = (participant_id, time_key)
                                summary_data[key]["patient_id"] = participant_id
                                summary_data[key]["site"] = site
                                summary_data[key]["data_summary"]["features_available"].add(feat_name)
                                update_summary(summary_data[key]["feature_statistics"][feat_name], dt, row[feat_def["extraction_field"]])
                            logger.info(f"Processed feature '{feat_name}' for participant {participant_id} for {time_key}")
                        break

                # Process simple questionnaire flag if provided
                if questionnaire_def and questionnaire_def["file_filter"] in file_path:
                    try:
                        with gzip.open(file_path, 'rt', encoding='utf-8') as gz:
                            df_q = pd.read_csv(gz)
                        df_q[questionnaire_def["time_field"]] = pd.to_datetime(df_q[questionnaire_def["time_field"]], unit='s', errors='coerce')
                        df_q = df_q.dropna(subset=[questionnaire_def["time_field"]])
                        if not df_q.empty:
                            for _, row in df_q.iterrows():
                                dt = row[questionnaire_def["time_field"]]
                                time_key = get_time_key(dt, args.time_resolution)
                                key = (participant_id, time_key)
                                summary_data[key]["patient_id"] = participant_id
                                summary_data[key]["site"] = site
                                summary_data[key]["questionnaire_responses"]["total_responses"] += 1
                                summary_data[key]["questionnaire_responses"]["days"].add(dt.date())
                            logger.info(f"Processed questionnaire responses for participant {participant_id} for {time_key}")
                    except Exception as e:
                        logger.error(f"Error processing questionnaire file '{file_path}': {e}")

                # Process questionnaire slider responses
                for qs_def in questionnaire_slider_defs:
                    if qs_def["file_filter"] in file_path:
                        try:
                            with gzip.open(file_path, 'rt', encoding='utf-8') as gz:
                                df_q = pd.read_csv(gz)
                            for _, row in df_q.iterrows():
                                for col in row.index:
                                    if col.startswith(qs_def["answers_base"]) and col.endswith(".questionId"):
                                        question_val = row[col]
                                        if isinstance(question_val, str) and question_val.startswith(qs_def["target_prefix"]):
                                            base = col.rsplit(".", 1)[0]
                                            value_col = f"{base}.{qs_def['value_suffix']}"
                                            time_col = qs_def['time_suffix']
                                            if value_col in row and time_col in row:
                                                try:
                                                    dt = pd.to_datetime(row[time_col], unit='s', errors='coerce')
                                                except Exception as e:
                                                    continue
                                                if pd.isna(dt):
                                                    continue
                                                slider_val = row[value_col]
                                                time_key = get_time_key(dt, args.time_resolution)
                                                key = (participant_id, time_key)
                                                summary_data[key]["patient_id"] = participant_id
                                                summary_data[key]["site"] = site
                                                slider_summary = summary_data[key]["questionnaire_responses"]["slider"][qs_def["domain"]]
                                                slider_summary["total_entries"] += 1
                                                try:
                                                    numeric_val = float(slider_val)
                                                except Exception:
                                                    continue
                                                slider_summary.setdefault("values", []).append(numeric_val)
                                                slider_summary.setdefault("days", set()).add(dt.date())
                            logger.info(f"Processed questionnaire slider for domain '{qs_def['domain']}' for participant {participant_id}")
                        except Exception as e:
                            logger.error(f"Error processing questionnaire slider in {file_path}: {e}")

                # Process questionnaire histogram responses (matching on target_questionid)
                for qh_def in questionnaire_histogram_defs:
                    if qh_def["file_filter"] in file_path:
                        logger.info(f"Processing histogram for domain '{qh_def['domain']}' in file {file_path}")
                        try:
                            with gzip.open(file_path, 'rt', encoding='utf-8') as gz:
                                df_q = pd.read_csv(gz)
                            for idx, row in df_q.iterrows():
                                for col in row.index:
                                    if col.startswith(qh_def["answers_base"]) and col.endswith(".questionId"):
                                        question_id = row[col]
                                        if question_id != qh_def["target_questionid"]:
                                            continue
                                        base = col.rsplit(".", 1)[0]
                                        value_col = f"{base}.{qh_def['value_suffix']}"
                                        time_col = qs_def['time_suffix']
                                        if value_col not in row or time_col not in row:
                                            continue
                                        response = row[value_col]
                                        try:
                                            dt = pd.to_datetime(row[time_col], unit='s', errors='coerce')
                                        except Exception:
                                            continue
                                        if pd.isna(dt):
                                            continue
                                        time_key = get_time_key(dt, args.time_resolution)
                                        key = (participant_id, time_key)
                                        hist = summary_data[key]["questionnaire_responses"]["histogram"]
                                        if qh_def["domain"] not in hist:
                                            hist[qh_def["domain"]] = defaultdict(lambda: defaultdict(int))
                                        hist[qh_def["domain"]][question_id][str(response)] += 1
                            logger.info(f"Processed questionnaire histogram for domain '{qh_def['domain']}' for participant {participant_id}")
                        except Exception as e:
                            logger.error(f"Error processing questionnaire histogram in {file_path}: {e}")

        # Post-process summary_data and write JSON files
    for (participant_id, time_key), data in summary_data.items():
        # Process feature statistics aggregation
        all_days = set()
        for stats in data["feature_statistics"].values():
            all_days.update(stats["days"])
        if all_days:
            data["data_summary"]["start_date"] = min(all_days).isoformat()
            data["data_summary"]["end_date"] = max(all_days).isoformat()
            data["data_summary"]["total_days_with_data"] = len(all_days)
        else:
            data["data_summary"]["start_date"] = None
            data["data_summary"]["end_date"] = None
            data["data_summary"]["total_days_with_data"] = 0
        data["data_summary"]["missing_days"] = None
        data["data_summary"]["features_available"] = sorted(list(data["data_summary"]["features_available"]))

        # Aggregate feature statistics into summaries (mean, median, etc.)
        final_stats = {}
        for feat, stats in data["feature_statistics"].items():
            mean, median, std_dev, min_val, max_val = compute_stats(stats["values"])
            final_stats[feat] = {
                "total_entries": stats["total_entries"],
                "days_with_data": len(stats["days"]),
                "mean": mean,
                "median": median,
                "std_dev": std_dev,
                "min": min_val,
                "max": max_val
            }
            if feature_defs.get(feat, {}).get("unit"):
                final_stats[feat]["unit"] = feature_defs[feat]["unit"]
        data["feature_statistics"] = final_stats

        # Process questionnaire responses (keeping the slider aggregation as before)
        data["questionnaire_responses"]["days_with_responses"] = len(data["questionnaire_responses"]["days"])
        data["questionnaire_responses"].pop("days", None)
        slider_final = {}
        for domain, slider_data in data["questionnaire_responses"]["slider"].items():
            mean, median, std_dev, min_val, max_val = compute_stats(slider_data.get("values", []))
            slider_final[domain] = {
                "total_entries": slider_data.get("total_entries", 0),
                "days_with_data": len(slider_data.get("days", set())),
                "mean": mean,
                "median": median,
                "std_dev": std_dev,
                "min": min_val,
                "max": max_val
            }
        data["questionnaire_responses"]["slider"] = slider_final

        data = convert_sets_to_lists(data)
        out_filename = f"{participant_id}_{time_key}.json"
        out_filepath = os.path.join(args.output_dir, out_filename)
        with open(out_filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Wrote summary file '{out_filepath}'")

    logger.info("Patient summary extraction complete.")

if __name__ == '__main__':
    main()
