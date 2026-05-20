"""Object-store contracts and lazy backend helpers for the pipeline kernel.

The minimal pipeline package should be importable without AWS libraries. Keep
concrete backend imports inside functions that are only called by backend-backed
execution paths.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Protocol


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


class ObjectStore(Protocol):
    """Backend-neutral object-store operations used by the pipeline kernel."""

    scheme: str

    def read_bytes(self, locator: str) -> bytes:
        """Read an object payload by locator."""

    def write_bytes(self, locator: str, payload: bytes, *, content_type: str | None = None) -> None:
        """Write an object payload by locator."""

    def upload_file(self, source_path: str | Path, locator: str) -> None:
        """Upload a local file to an object locator."""

    def download_file(self, locator: str, destination_path: str | Path) -> None:
        """Download an object locator to a local file."""

    def iter_object_locators(self, prefix_locator: str) -> Iterable[str]:
        """Yield object locators under a prefix locator."""

    def iter_child_prefix_locators(self, prefix_locator: str) -> Iterable[str]:
        """Yield direct child prefix locators under a prefix locator."""

    def prefix_has_objects(self, prefix_locator: str) -> bool:
        """Return whether a prefix contains at least one object."""


class NoOpObjectStore:
    """Object-store implementation for local-only runs."""

    scheme = "none"

    def _raise(self, operation: str) -> None:
        raise RuntimeError(f"No object store is configured for this pipeline run; attempted {operation}.")

    def read_bytes(self, locator: str) -> bytes:
        self._raise(f"read_bytes({locator})")

    def write_bytes(self, locator: str, payload: bytes, *, content_type: str | None = None) -> None:
        self._raise(f"write_bytes({locator})")

    def upload_file(self, source_path: str | Path, locator: str) -> None:
        self._raise(f"upload_file({locator})")

    def download_file(self, locator: str, destination_path: str | Path) -> None:
        self._raise(f"download_file({locator})")

    def iter_object_locators(self, prefix_locator: str) -> Iterable[str]:
        self._raise(f"iter_object_locators({prefix_locator})")

    def iter_child_prefix_locators(self, prefix_locator: str) -> Iterable[str]:
        self._raise(f"iter_child_prefix_locators({prefix_locator})")

    def prefix_has_objects(self, prefix_locator: str) -> bool:
        self._raise(f"prefix_has_objects({prefix_locator})")


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


def create_object_store_for_locator(
    locator: str,
    *,
    session: Any = None,
    profile_name: str | None = None,
) -> ObjectStore:
    """Create an object-store adapter for a locator scheme."""

    scheme = locator_scheme(locator)
    if scheme == "s3":
        from .backends.s3 import S3ObjectStore, create_s3_client

        return S3ObjectStore(create_s3_client(session=session, profile_name=profile_name))
    if not scheme:
        return NoOpObjectStore()
    raise ValueError(f"Unsupported object-store locator scheme '{scheme}': {locator}")


def object_store_from_client(client: Any, *, scheme: str = "s3") -> ObjectStore:
    """Wrap an existing backend client in the matching object-store adapter."""

    if isinstance(client, NoOpObjectStoreClient):
        return NoOpObjectStore()
    if scheme == "s3":
        from .backends.s3 import S3ObjectStore

        return S3ObjectStore(client)
    raise ValueError(f"Unsupported object-store client scheme '{scheme}'")


def object_store_client(store: ObjectStore) -> Any:
    """Return a compatibility backend client when an adapter exposes one."""

    return getattr(store, "s3_client", None)


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
    "NoOpObjectStore",
    "ObjectStore",
    "ObjectStoreBackend",
    "ObjectStoreClient",
    "client_error_code",
    "create_boto3_session",
    "create_object_store_for_locator",
    "create_s3_client",
    "is_client_error",
    "is_missing_key_error",
    "locator_needs_object_store",
    "locator_scheme",
    "object_store_client",
    "object_store_from_client",
    "split_s3_uri",
]
