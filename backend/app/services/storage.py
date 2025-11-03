"""
This service provides an interface for interacting with the MinIO object storage.
It handles creating the bucket, uploading files, creating completion markers,
and cleaning up processed files. It abstracts the MinIO client logic away
from the API endpoints and pipeline steps.
"""
import logging
import time
from io import BytesIO
from minio import Minio
from minio.error import S3Error

from app.core.config import settings

logger = logging.getLogger(__name__)

# Initialize MinIO client
client = Minio(
    settings.minio_endpoint,
    access_key=settings.minio_access,
    secret_key=settings.minio_secret,
    secure=settings.minio_secure
)

def ensure_bucket_exists():
    """Create bucket if it doesn't exist."""
    try:
        if not client.bucket_exists(settings.minio_bucket):
            client.make_bucket(settings.minio_bucket)
            logger.info(f"✅ Created bucket: {settings.minio_bucket}")
    except S3Error as e:
        logger.error(f"Failed to create bucket: {e}")
        raise

async def upload_file_to_minio(file, filename: str) -> int:
    """
    Upload a file from a FastAPI UploadFile object to MinIO.

    Args:
        file: The FastAPI UploadFile object.
        filename: The original name of the file.

    Returns:
        The size of the uploaded file in bytes.
    """
    ensure_bucket_exists()

    # Generate a unique object name to avoid collisions
    object_name = f"{int(time.time())}_{filename}"

    # Read file content into a BytesIO stream
    content = await file.read()
    file_size = len(content)
    file_stream = BytesIO(content)

    try:
        client.put_object(
            bucket_name=settings.minio_bucket,
            object_name=object_name,
            data=file_stream,
            length=file_size,
            content_type=file.content_type or "application/octet-stream"
        )
        logger.info(f"Successfully uploaded to MinIO: {object_name} ({file_size} bytes)")
        return file_size
    except S3Error as e:
        logger.error(f"MinIO upload failed for {object_name}: {e}")
        raise

async def create_complete_marker():
    """
    Upload an empty '_complete' marker file to signal the end of a batch upload.
    This file is used by other services to know when it's safe to start processing.
    """
    ensure_bucket_exists()

    empty_stream = BytesIO(b"")

    try:
        client.put_object(
            bucket_name=settings.minio_bucket,
            object_name="_complete",
            data=empty_stream,
            length=0,
            content_type="application/octet-stream"
        )
        logger.info("Successfully uploaded '_complete' marker to MinIO.")
    except S3Error as e:
        logger.error(f"Failed to upload '_complete' marker: {e}")
        raise

def list_uploaded_files():
    """
    List all uploaded files in the bucket.
    """
    try:
        objects = client.list_objects(settings.minio_bucket, recursive=True)
        files = [
            obj.object_name 
            for obj in objects 
            if obj.object_name.endswith(('.xlsx', '.csv'))
        ]
        return files
    except S3Error as e:
        logger.error(f"Failed to list files: {e}")
        return []

def remove_object(object_name: str):
    """Remove an object from the bucket."""
    try:
        client.remove_object(settings.minio_bucket, object_name)
        logger.info(f"🗑️  Removed: {object_name}")
    except S3Error as e:
        logger.error(f"Failed to remove object: {e}")

def clear_bucket():
    """Remove all files from the bucket (after processing)."""
    try:
        objects = client.list_objects(settings.minio_bucket, recursive=True)
        for obj in objects:
            remove_object(obj.object_name)
        logger.info("🗑️  Bucket cleared")
    except S3Error as e:
        logger.error(f"Failed to clear bucket: {e}")