#!/usr/bin/env python3
"""
Minimal helpers for Google Cloud Storage buckets.

Prerequisite:
    pip install --upgrade google-cloud-storage
"""

import os
from typing import List, Dict, Union

from google.cloud import storage


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _client(sa_key_path: str) -> storage.Client:
    """
    Build a storage.Client, given the path to a service-account JSON key file.
    The call is cheap; reuse the returned client when you issue many requests.
    """
    # Equivalent to:  export GOOGLE_APPLICATION_CREDENTIALS=/path/key.json
    return storage.Client.from_service_account_json(sa_key_path)


# ---------------------------------------------------------------------------
# Public bucket utilities
# ---------------------------------------------------------------------------

def list_gcs_objects(
    service_account_key_path: str,
    bucket_name: str
) -> List[Dict[str, Union[str, int]]]:
    """
    List objects in *bucket_name*.

    Returns:
        A list of dictionaries with at least keys 'name' and 'size'.
    """
    client = _client(service_account_key_path)
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs()
    return [{"name": b.name, "size": b.size} for b in blobs]


def delete_gcs_object(
    service_account_key_path: str,
    bucket_name: str,
    object_name: str
) -> None:
    """
    Delete *object_name* from *bucket_name*.
    No exception is raised if the object is missing.
    """
    client = _client(service_account_key_path)
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(object_name)
    try:
        blob.delete()
    except Exception as exc:
        # Ignore 404, bubble up anything else.
        if getattr(exc, "code", None) != 404:
            raise


def download_gcs_object(
    service_account_key_path: str,
    bucket_name: str,
    object_name: str
) -> bytes:
    """
    Download *object_name* from *bucket_name*.

    Returns:
        The object’s content as bytes.
    """
    client = _client(service_account_key_path)
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(object_name)
    return blob.download_as_bytes()  # raises NotFound for a missing object


def upload_gcs_object(
    service_account_key_path: str,
    bucket_name: str,
    object_name: str,
    file_path: str
) -> None:
    """
    Upload *file_path* to *bucket_name* under key *object_name*.
    """
    client = _client(service_account_key_path)
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(file_path)
