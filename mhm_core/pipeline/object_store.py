"""Lazy object-store helpers for the pipeline kernel.

The minimal pipeline package should be importable without AWS libraries. Keep
all boto3/botocore imports inside functions that are only called by S3-backed
execution paths.
"""

from __future__ import annotations

from typing import Any


class NoOpObjectStoreClient:
    """Placeholder client for local-only profiles that do not need object storage."""

    def __getattr__(self, name: str):
        raise RuntimeError(
            "No object-store client is configured for this pipeline run; "
            f"attempted to use client method or attribute '{name}'."
        )


def create_boto3_session(*, profile_name: str | None = None) -> Any:
    """Create a boto3 session only when an S3-backed path needs one."""

    import boto3

    return boto3.session.Session(profile_name=profile_name) if profile_name else boto3.session.Session()


def create_s3_client(*, session: Any = None, profile_name: str | None = None) -> Any:
    """Create an S3 client using a supplied session or a lazily-created session."""

    if session is None:
        session = create_boto3_session(profile_name=profile_name)
    return session.client("s3")


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
    """Split an S3 URI into bucket and key."""

    if not str(uri).startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got {uri}")
    remainder = str(uri)[len("s3://") :]
    bucket, _, key = remainder.partition("/")
    if not bucket:
        raise ValueError(f"Invalid S3 URI: {uri}")
    return bucket, key


def locator_needs_object_store(locator: str) -> bool:
    """Return whether a locator needs an object-store client."""

    return str(locator).strip().startswith("s3://")


__all__ = [
    "NoOpObjectStoreClient",
    "client_error_code",
    "create_boto3_session",
    "create_s3_client",
    "is_client_error",
    "is_missing_key_error",
    "locator_needs_object_store",
    "split_s3_uri",
]
