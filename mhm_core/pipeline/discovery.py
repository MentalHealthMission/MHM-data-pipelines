"""Helpers to discover entities from grouped object-store prefixes."""

from __future__ import annotations

from typing import Dict, Iterable, List, Tuple
import logging

from .object_store import ObjectStore, client_error_code, object_store_from_client


def discover_entities(
    s3_client,
    *,
    bucket: str,
    prefix: str,
    logger: logging.Logger,
    groups: Iterable[str] | None = None,
) -> Tuple[List[str], Dict[str, str]]:
    """Compatibility wrapper for S3-backed entity discovery."""

    return discover_entities_from_store(
        object_store_from_client(s3_client),
        source_locator=f"s3://{bucket}/{prefix.strip('/')}" if prefix.strip("/") else f"s3://{bucket}",
        logger=logger,
        groups=groups,
    )


def discover_entities_from_store(
    object_store: ObjectStore,
    *,
    source_locator: str,
    logger: logging.Logger,
    groups: Iterable[str] | None = None,
) -> Tuple[List[str], Dict[str, str]]:
    """Return (entities, entity_group_map) discovered under a source locator.

    Expects layout: <prefix>/<group>/<entity>/...
    """
    entities: List[str] = []
    entity_group_map: Dict[str, str] = {}

    source_locator = source_locator.rstrip("/")
    try:
        group_prefixes: List[str] = []
        if groups:
            for group in groups:
                group_prefixes.append(_join_locator(source_locator, str(group).strip()))
        else:
            group_prefixes.extend(object_store.iter_child_prefix_locators(source_locator))
        for group_path in group_prefixes:
            group = _locator_name(group_path)
            logger.info("Discovered group prefix %s", group_path)

            for entity_path in object_store.iter_child_prefix_locators(group_path):
                entity_id = _locator_name(entity_path)
                if not entity_id:
                    continue
                entities.append(entity_id)
                entity_group_map[entity_id] = group
    except Exception as exc:
        code = client_error_code(exc)
        if code == "ExpiredToken":
            logger.error(
                "AWS token expired while listing %s/%s. Refresh credentials (e.g. `aws sso login` or your MFA workflow) and rerun.",
                object_store.scheme,
                source_locator,
            )
        else:
            logger.error("Failed to discover entities under %s: %s", source_locator, exc)
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


def _join_locator(prefix_locator: str, child: str) -> str:
    child = child.strip("/")
    if not child:
        return prefix_locator.rstrip("/")
    return f"{prefix_locator.rstrip('/')}/{child}"


def _locator_name(locator: str) -> str:
    return str(locator).rstrip("/").rsplit("/", 1)[-1].strip()


__all__ = ["discover_entities", "discover_entities_from_store", "discover_participants"]
