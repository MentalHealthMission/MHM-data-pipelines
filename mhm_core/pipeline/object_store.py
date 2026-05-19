"""Object-store contracts and lazy backend helpers for the pipeline kernel.

The minimal pipeline package should be importable without AWS libraries. Keep
concrete backend imports inside functions that are only called by backend-backed
execution paths.
"""

from __future__ import annotations

from typing import Any, Protocol


class NoOpObjectStoreClient:
    """Placeholder client for local-only profiles that do not need object storage."""

    def __getattr__(self, name: str):
        raise RuntimeError(
            "No object-store client is configured for this pipeline run; "
            f"attempted to use client method or attribute '{name}'."
        )


class ObjectStoreClient(Protocol):
    """Minimal object-store client contract used by backend-specific adapters."""

    def get_paginator(self, operation_name: str) -> Any:
        """Return a paginator for a backend-specific list operation."""


class ObjectStoreBackend(Protocol):
    """Backend contract for object-store locator handling."""

    scheme: str

    def client(self) -> ObjectStoreClient:
        """Return a concrete client for this backend."""


def locator_scheme(locator: str) -> str:
    """Return the URI scheme for a storage locator, or an empty string for paths."""

    text = str(locator or "").strip()
    if "://" not in text:
        return ""
    scheme, _, _ = text.partition("://")
    return scheme.lower()


def locator_needs_object_store(locator: str) -> bool:
    """Return whether a locator needs an object-store client."""

    return locator_scheme(locator) in {"s3"}


def create_boto3_session(*, profile_name: str | None = None) -> Any:
    """Compatibility wrapper for the S3 backend session factory."""

    from .backends.s3 import create_boto3_session as _create_boto3_session

    return _create_boto3_session(profile_name=profile_name)


def create_s3_client(*, session: Any = None, profile_name: str | None = None) -> Any:
    """Compatibility wrapper for the S3 backend client factory."""

    from .backends.s3 import create_s3_client as _create_s3_client

    return _create_s3_client(session=session, profile_name=profile_name)


def client_error_code(exc: BaseException) -> str:
    """Return a botocore-style client error code without importing botocore."""

    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return ""
    error = response.get("Error", {})
    if not isinstance(error, dict):
        return ""
    return str(error.get("Code", "") or "")


def is_client_error(exc: BaseException) -> bool:
    """Return whether an exception looks like a botocore ClientError."""

    return exc.__class__.__name__ == "ClientError" or bool(client_error_code(exc))


def is_missing_key_error(exc: BaseException) -> bool:
    """Return whether an object-store exception means the key is absent."""

    return client_error_code(exc) in {"NoSuchKey", "404", "NotFound"}


def split_s3_uri(uri: str) -> tuple[str, str]:
    """Compatibility wrapper for S3 URI parsing."""

    from .backends.s3 import split_s3_uri as _split_s3_uri

    return _split_s3_uri(uri)


__all__ = [
    "NoOpObjectStoreClient",
    "ObjectStoreBackend",
    "ObjectStoreClient",
    "client_error_code",
    "create_boto3_session",
    "create_s3_client",
    "is_client_error",
    "is_missing_key_error",
    "locator_needs_object_store",
    "locator_scheme",
    "split_s3_uri",
]
