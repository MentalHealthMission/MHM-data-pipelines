
#!/usr/bin/env python3

import argparse
import boto3
import os
import sys
import platform
import logging
import configparser
from botocore.exceptions import NoCredentialsError, ClientError
from botocore.config import Config as BotocoreConfig
from boto3.s3.transfer import TransferConfig
import urllib.parse
import psutil
from typing import List
import concurrent.futures

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def normalize_network_path(path: str) -> str:
    path = path.replace('\\', '/').replace('\\\\', '/').replace('\\\\', '/')
    path = path.replace('\\', '/').replace('\\\\', '/')
    path = urllib.parse.unquote(path)
    if '@' in path:
        path = path.split('@', 1)[1]
    if '://' in path:
        path = path.split('://', 1)[1]
    path = path.lstrip('/')
    return path.lower()

def is_mounted_correctly(mount_point: str, network_path: str) -> bool:
    system = platform.system()
    expected_path_normalized = normalize_network_path(network_path)
    if system == 'Windows':
        import subprocess
        try:
            command = 'wmic logicaldisk get deviceid, providername'
            result = subprocess.check_output(command, shell=True).decode()
            lines = result.strip().split('\n')
            for line in lines[1:]:
                if line.strip() == '':
                    continue
                parts = line.strip().split(None, 1)
                if len(parts) == 2:
                    device_id, provider_name = parts
                    if device_id.lower().rstrip('\\') == mount_point.lower().rstrip('\\'):
                        actual_path_normalized = normalize_network_path(provider_name)
                        if expected_path_normalized == actual_path_normalized:
                            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to execute command: {e}")
    else:
        for partition in psutil.disk_partitions(all=True):
            if partition.mountpoint == mount_point:
                actual_path_normalized = normalize_network_path(partition.device)
                if expected_path_normalized == actual_path_normalized:
                    return True
    return False

def should_exclude_key(key: str, exclude_sites: List[str], include_sites: List[str]) -> bool:
    path_parts = key.split('/')
    if exclude_sites and any(site in path_parts for site in exclude_sites):
        return True
    if include_sites and not any(site in path_parts for site in include_sites):
        return True
    return False

def download_file_wrapper(s3_client, s3_bucket, key, local_file_path, transfer_config):
    try:
        s3_client.download_file(
            Bucket=s3_bucket,
            Key=key,
            Filename=local_file_path,
            Config=transfer_config
        )
        logger.info(f"Downloaded '{key}'")
    except Exception as e:
        logger.error(f"Error downloading '{key}': {e}")

def download_s3_objects(s3_bucket: str, s3_prefix: str, output_dir: str,
                        exclude_sites: List[str], include_sites: List[str],
                        start_at_page: int = 1, skip_file_check: bool = False):
    s3 = boto3.client('s3', config=BotocoreConfig(max_pool_connections=50, connect_timeout=10, read_timeout=30))
    transfer_config = TransferConfig(
        max_concurrency=5,
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=True
    )
    paginator = s3.get_paginator('list_objects_v2')
    total_objects = 0
    total_tasks = 0
    current_page = 0
    created_directories = set()
    try:
        logger.info(f"Listing objects in bucket '{s3_bucket}' with prefix '{s3_prefix}'")
        page_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
        for page in page_iterator:
            current_page += 1
            if current_page < start_at_page:
                logger.info(f"Skipping page {current_page}")
                continue
            objects = page.get('Contents', [])
            logger.info(f"Page {current_page}: Retrieved {len(objects)} objects")
            total_objects += len(objects)
            tasks = []
            for obj in objects:
                key = obj['Key']
                if should_exclude_key(key, exclude_sites, include_sites):
                    continue
                local_file_path = os.path.join(output_dir, s3_bucket, key)
                local_dir = os.path.dirname(local_file_path)
                if local_dir not in created_directories:
                    try:
                        os.makedirs(local_dir, exist_ok=True)
                        created_directories.add(local_dir)
                    except Exception as e:
                        logger.error(f"Error creating directory '{local_dir}': {e}")
                        continue
                if skip_file_check or not os.path.exists(local_file_path):
                    tasks.append((key, local_file_path))
            total_tasks += len(tasks)
            if tasks:
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    futures = {
                        executor.submit(
                            download_file_wrapper,
                            s3,
                            s3_bucket,
                            key,
                            local_file_path,
                            transfer_config
                        ): key for key, local_file_path in tasks
                    }
                    for future in concurrent.futures.as_completed(futures):
                        pass
        logger.info(f"Total objects found: {total_objects}")
        logger.info(f"Total tasks queued for download: {total_tasks}")
    except ClientError as e:
        logger.error(f"AWS ClientError: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

def main():
    config = configparser.ConfigParser()
    config.read('config/config.ini')
    try:
        s3_bucket_path = config['AWS']['s3_bucket_path']
        bucket_name, s3_prefix = s3_bucket_path.split('/', 1)
    except KeyError as e:
        logger.error(f"Missing configuration in config.ini: {e}")
        sys.exit(1)
    except ValueError:
        logger.error("Invalid s3_bucket_path format in config.ini. Expected 'bucket_name/prefix'")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Download data from S3 to a mapped network drive.")
    parser.add_argument('--s3-bucket', type=str, default=bucket_name, help='S3 bucket name')
    parser.add_argument('--s3-prefix', type=str, default=s3_prefix, help='S3 prefix/path')
    parser.add_argument('--output-dir', type=str, help='Output directory')
    parser.add_argument('--mount-point', type=str, help='Expected local mount point if output-dir not provided')
    parser.add_argument('--network-path', type=str, help='Expected network path if output-dir not provided')
    parser.add_argument('--exclude-sites', type=str, help='Comma-separated list of site names to exclude')
    parser.add_argument('--include-sites', type=str, help='Comma-separated list of site names to include')
    parser.add_argument('--start-at-page', type=int, default=1, help='Page number to start pagination from')
    parser.add_argument('--skip-file-check', action='store_true', help='Skip checking if files already exist')
    args = parser.parse_args()

    exclude_sites = [s.strip() for s in args.exclude_sites.split(',')] if args.exclude_sites else []
    include_sites = [s.strip() for s in args.include_sites.split(',')] if args.include_sites else []

    output_dir = args.output_dir
    if not output_dir:
        if not args.mount_point or not args.network_path:
            logger.error("When --output-dir is not used, both --mount-point and --network-path must be provided.")
            sys.exit(1)
        if not is_mounted_correctly(args.mount_point, args.network_path):
            logger.error(f"The drive is not mounted correctly at '{args.mount_point}'.")
            sys.exit(1)
        output_dir = args.mount_point
        logger.info("Downloading to verified mount point.")

    download_s3_objects(
        args.s3_bucket,
        args.s3_prefix,
        output_dir,
        exclude_sites=exclude_sites,
        include_sites=include_sites,
        start_at_page=args.start_at_page,
        skip_file_check=args.skip_file_check
    )

if __name__ == '__main__':
    main()
