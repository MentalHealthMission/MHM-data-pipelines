"""Helpers to discover entities from grouped object-store prefixes."""

from __future__ import annotations

from typing import Dict, Iterable, List, Tuple
import logging

from .object_store import client_error_code


def discover_entities(
    s3_client,
    *,
    bucket: str,
    prefix: str,
    logger: logging.Logger,
    groups: Iterable[str] | None = None,
) -> Tuple[List[str], Dict[str, str]]:
    """Return (entities, entity_group_map) discovered under bucket/prefix.

    Expects layout: <prefix>/<group>/<entity>/...
    """
    entities: List[str] = []
    entity_group_map: Dict[str, str] = {}

    base_prefix = prefix.strip("/")
    base_prefix = f"{base_prefix}/" if base_prefix else ""

    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        group_prefixes: List[str] = []
        if groups:
            for group in groups:
                group_prefixes.append(f"{base_prefix}{group}")
        else:
            for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix, Delimiter="/"):
                for group_prefix in page.get("CommonPrefixes", []):
                    group_path = group_prefix.get("Prefix", "").rstrip("/")
                    if group_path:
                        group_prefixes.append(group_path)
        for group_path in group_prefixes:
            group = group_path.split("/")[-1]
            logger.info("Discovered group prefix %s", group_path)

            for entity_page in paginator.paginate(
                Bucket=bucket,
                Prefix=f"{group_path}/",
                Delimiter="/",
            ):
                for entity_prefix in entity_page.get("CommonPrefixes", []):
                    entity_path = entity_prefix.get("Prefix", "").rstrip("/")
                    if not entity_path:
                        continue
                    entity_id = entity_path.split("/")[-1]
                    entities.append(entity_id)
                    entity_group_map[entity_id] = group
    except Exception as exc:
        code = client_error_code(exc)
        if code == "ExpiredToken":
            logger.error(
                "AWS token expired while listing %s/%s. Refresh credentials (e.g. `aws sso login` or your MFA workflow) and rerun.",
                bucket,
                prefix,
            )
        else:
            logger.error("Failed to discover entities under %s/%s: %s", bucket, prefix, exc)
        raise

    entities = sorted({entity_id for entity_id in entities if entity_id})
    return entities, entity_group_map


def discover_participants(
    s3_client,
    *,
    bucket: str,
    prefix: str,
    logger: logging.Logger,
    sites: List[str] | None = None,
) -> Tuple[List[str], Dict[str, str]]:
    """Compatibility wrapper for the historical participant/site API."""

    return discover_entities(
        s3_client,
        bucket=bucket,
        prefix=prefix,
        logger=logger,
        groups=sites,
    )


__all__ = ["discover_entities", "discover_participants"]
