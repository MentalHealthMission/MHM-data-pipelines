"""Helpers to discover participants from the source bucket/prefix."""

from __future__ import annotations

from typing import Dict, List, Tuple
import logging

import boto3
from botocore.exceptions import ClientError


def discover_participants(
    s3_client: boto3.client,
    *,
    bucket: str,
    prefix: str,
    logger: logging.Logger,
    sites: List[str] | None = None,
) -> Tuple[List[str], Dict[str, str]]:
    """Return (participants, site_map) discovered under bucket/prefix.

    Expects layout: <prefix>/<site>/<participant_uuid>/...
    """
    participants: List[str] = []
    site_map: Dict[str, str] = {}

    base_prefix = prefix.strip("/")
    base_prefix = f"{base_prefix}/" if base_prefix else ""

    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        site_prefixes: List[str] = []
        if sites:
            for site in sites:
                site_prefixes.append(f"{base_prefix}{site}")
        else:
            for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix, Delimiter="/"):
                for site_prefix in page.get("CommonPrefixes", []):
                    site_path = site_prefix.get("Prefix", "").rstrip("/")
                    if site_path:
                        site_prefixes.append(site_path)
        for site_path in site_prefixes:
            site = site_path.split("/")[-1]
            logger.info("Discovered site prefix %s", site_path)

            for participant_page in paginator.paginate(
                Bucket=bucket,
                Prefix=f"{site_path}/",
                Delimiter="/",
            ):
                for participant_prefix in participant_page.get("CommonPrefixes", []):
                    p_path = participant_prefix.get("Prefix", "").rstrip("/")
                    if not p_path:
                        continue
                    pid = p_path.split("/")[-1]
                    participants.append(pid)
                    site_map[pid] = site
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code") if hasattr(exc, "response") else None
        if code == "ExpiredToken":
            logger.error(
                "AWS token expired while listing %s/%s. Refresh credentials (e.g. `aws sso login` or your MFA workflow) and rerun.",
                bucket,
                prefix,
            )
        else:
            logger.error("Failed to discover participants under %s/%s: %s", bucket, prefix, exc)
        raise

    participants = sorted({pid for pid in participants if pid})
    return participants, site_map


__all__ = ["discover_participants"]
