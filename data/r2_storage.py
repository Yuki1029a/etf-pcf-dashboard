"""Cloudflare R2 storage layer for PCF data persistence.

Provides a simple get/put interface using S3-compatible API.
Fails silently on any error so the app can fall back to local storage.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_client = None
_bucket: str = ""
_initialized: bool = False


def _init_client():
    """Lazy-initialize the R2 client from Streamlit secrets or env vars."""
    global _client, _bucket, _initialized

    if _initialized:
        return
    _initialized = True

    try:
        # Try Streamlit secrets first
        import streamlit as st
        r2_conf = st.secrets.get("r2", {})
        account_id = r2_conf.get("account_id", "")
        access_key = r2_conf.get("access_key_id", "")
        secret_key = r2_conf.get("secret_access_key", "")
        _bucket = r2_conf.get("bucket_name", "pcf-data")
    except Exception:
        # Fall back to environment variables
        import os
        account_id = os.environ.get("R2_ACCOUNT_ID", "")
        access_key = os.environ.get("R2_ACCESS_KEY_ID", "")
        secret_key = os.environ.get("R2_SECRET_ACCESS_KEY", "")
        _bucket = os.environ.get("R2_BUCKET_NAME", "pcf-data")

    if not (account_id and access_key and secret_key):
        logger.info("R2 credentials not configured; R2 storage disabled")
        return

    try:
        import boto3
        _client = boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )
        logger.info("R2 storage initialized: bucket=%s", _bucket)
    except Exception as e:
        logger.warning("R2 init failed: %s", e)


def r2_get(key: str) -> bytes | None:
    """Get an object from R2. Returns None on any failure."""
    _init_client()
    if _client is None:
        return None
    try:
        resp = _client.get_object(Bucket=_bucket, Key=key)
        return resp["Body"].read()
    except _client.exceptions.NoSuchKey:
        return None
    except Exception:
        return None


def r2_put(key: str, content: bytes) -> bool:
    """Put an object to R2. Returns True on success."""
    _init_client()
    if _client is None:
        return False
    try:
        _client.put_object(Bucket=_bucket, Key=key, Body=content)
        return True
    except Exception as e:
        logger.warning("R2 put failed for %s: %s", key, e)
        return False


def r2_available() -> bool:
    """Check if R2 is configured and available."""
    _init_client()
    return _client is not None
