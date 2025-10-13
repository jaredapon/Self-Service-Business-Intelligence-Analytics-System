from minio import Minio
from core.config import settings
import time

# Initialize a MinIO client
client = Minio(
    settings.minio_endpoint,
    access_key=settings.minio_access,
    secret_key=settings.minio_secret,
    secure=settings.minio_secure,
)

def ensure_bucket_exists(bucket: str):
    """Creates the bucket if it doesn't exist yet."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

def presign_put(filename: str, expires: int = 3600):
    """
    Generate a presigned URL for uploading (PUT).
    Returns a dict with upload URL and object name.
    """
    ensure_bucket_exists(settings.minio_bucket)
    object_name = f"{int(time.time())}_{filename}"
    url = client.presigned_put_object(
        bucket_name=settings.minio_bucket,
        object_name=object_name,
        expires=expires,
    )
    return {"url": url, "object_name": object_name}

def presign_get(object_name: str, expires: int = 3600):
    """Generate a presigned URL for downloading (GET)."""
    ensure_bucket_exists(settings.minio_bucket)
    url = client.presigned_get_object(
        bucket_name=settings.minio_bucket,
        object_name=object_name,
        expires=expires,
    )
    return {"url": url}
