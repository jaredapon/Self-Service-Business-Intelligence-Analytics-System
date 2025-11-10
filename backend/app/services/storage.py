"""
Service for interacting with MinIO object storage.

Responsibilities:
- Ensure required buckets and logical "folders" exist.
- Upload files to the landing bucket.
- Create a '_complete' marker to signal batch completion.
- List uploaded data files.
- Remove objects / clear the landing bucket.

Used by:
- Async upload API (FastAPI)
- Pipeline / observer services.
"""

import logging
import time
from io import BytesIO
from typing import List, Any

import anyio
from minio import Minio
from minio.error import S3Error

from app.core.config import settings

logger = logging.getLogger(__name__)

# Initialize MinIO client (sync)
client = Minio(
    settings.minio_endpoint,
    access_key=settings.minio_access,
    secret_key=settings.minio_secret,
    secure=settings.minio_secure,
)


def _create_folder_marker(bucket: str, folder: str) -> None:
    """
    Create a small marker object to represent a folder/prefix in the bucket.

    Non-fatal: failure is logged but does not stop execution.
    """
    if not folder:
        return

    marker_name = folder.rstrip("/") + "/.keep"

    try:
        client.put_object(
            bucket_name=bucket,
            object_name=marker_name,
            data=BytesIO(b""),
            length=0,
            content_type="application/octet-stream",
        )
        logger.debug("Created folder marker: %s/%s", bucket, marker_name)
    except S3Error as e:
        logger.warning(
            "Could not create folder marker %s in %s: %s",
            marker_name,
            bucket,
            e,
        )


def ensure_buckets_exist() -> None:
    """
    Ensure landing + staging buckets and required prefixes exist.

    - Landing bucket:
        - created if missing
        - ensures raw_sales + raw_sales_by_product prefixes via .keep markers
    - Staging bucket:
        - created if configured and missing
    """
    try:
        # Landing bucket
        landing = settings.minio_landing_bucket
        if not client.bucket_exists(landing):
            client.make_bucket(landing)
            logger.info("✅ Created bucket: %s", landing)

        _create_folder_marker(landing, settings.minio_raw_sales_folder)
        _create_folder_marker(
            landing,
            settings.minio_raw_sales_by_product_folder,
        )

        # Staging bucket (for processed CSVs)
        staging = getattr(settings, "minio_staging_bucket", None)
        if staging:
            if not client.bucket_exists(staging):
                client.make_bucket(staging)
                logger.info("✅ Created bucket: %s", staging)

    except S3Error as e:
        logger.error("Failed to ensure MinIO buckets: %s", e)
        raise


def ensure_bucket_exists() -> None:
    """
    Backwards-compatible alias, if other code still calls ensure_bucket_exists().
    """
    ensure_buckets_exist()


async def upload_file_to_minio(
    file: Any, filename: str, target_folder: str
) -> int:
    """
    Async-friendly upload of a file to a specific folder in the landing bucket.

    - Reads file content using async I/O.
    - Offloads MinIO (blocking) calls to a worker thread.

    Args:
        file: FastAPI UploadFile or similar (must support .read()).
        filename: Original filename.
        target_folder: The folder/prefix within the landing bucket.

    Returns:
        Size of the uploaded file in bytes.
    """
    # Make sure buckets exist (in a worker thread so we don't block the event loop)
    await anyio.to_thread.run_sync(ensure_buckets_exist)

    # Unique object name to avoid collisions, placed in the target folder
    unique_filename = f"{int(time.time())}_{filename}"
    object_name = f"{target_folder}/{unique_filename}"

    # Async read from UploadFile
    content = await file.read()
    if content is None:
        content = b""

    file_size = len(content)
    content_type = getattr(file, "content_type", None) or "application/octet-stream"

    def _do_upload() -> None:
        try:
            client.put_object(
                bucket_name=settings.minio_landing_bucket,
                object_name=object_name,
                data=BytesIO(content),
                length=file_size,
                content_type=content_type,
            )
        except S3Error as e:
            logger.error("MinIO upload failed for %s: %s", object_name, e)
            raise

    # Offload to thread (non-blocking for the event loop)
    await anyio.to_thread.run_sync(_do_upload)

    logger.info(
        "Successfully uploaded to MinIO: %s (%d bytes)",
        object_name,
        file_size,
    )
    return file_size


async def create_complete_marker(marker_name: str = "_complete") -> None:
    """
    Async-friendly creation of a '_complete' marker in the landing bucket.

    Your observer/watchdog watches for this and runs ETL + MBA + PED + NLP + HW.

    Args:
        marker_name: marker object name (default: '_complete').
                     For single-tenant, root-level is fine.
    """
    # Ensure buckets exist without blocking event loop
    await anyio.to_thread.run_sync(ensure_buckets_exist)

    def _do_marker() -> None:
        try:
            client.put_object(
                bucket_name=settings.minio_landing_bucket,
                object_name=marker_name,
                data=BytesIO(b""),
                length=0,
                content_type="application/octet-stream",
            )
        except S3Error as e:
            logger.error("Failed to upload '%s' marker: %s", marker_name, e)
            raise

    await anyio.to_thread.run_sync(_do_marker)

    logger.info("Successfully uploaded '%s' marker to MinIO.", marker_name)


def list_uploaded_files() -> List[str]:
    """
    List all uploaded data files (.xlsx, .csv) in the landing bucket.
    (Primarily for debugging / admin / pipeline utilities.)
    """
    try:
        objects = client.list_objects(
            settings.minio_landing_bucket,
            recursive=True,
        )
        return [
            obj.object_name
            for obj in objects
            if obj.object_name.lower().endswith((".xlsx", ".csv"))
        ]
    except S3Error as e:
        logger.error("Failed to list files: %s", e)
        return []


def remove_object(object_name: str) -> None:
    """
    Remove a single object from the landing bucket.
    """
    try:
        client.remove_object(settings.minio_landing_bucket, object_name)
        logger.info("🗑️  Removed: %s", object_name)
    except S3Error as e:
        logger.error("Failed to remove object %s: %s", object_name, e)


def clear_bucket(preserve_markers: bool = True) -> None:
    """
    Clear the landing bucket (e.g., after successful processing).

    Args:
        preserve_markers:
            If True, keeps folder markers (/.keep) and '_complete' marker.
    """
    try:
        objects = client.list_objects(
            settings.minio_landing_bucket,
            recursive=True,
        )
        for obj in objects:
            name = obj.object_name
            if preserve_markers and (
                name.endswith("/.keep") or name == "_complete"
            ):
                continue
            remove_object(name)

        logger.info("🗑️  Bucket cleared")
    except S3Error as e:
        logger.error("Failed to clear bucket: %s", e)
