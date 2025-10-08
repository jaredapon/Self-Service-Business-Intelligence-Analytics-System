import secrets
from fastapi import HTTPException
from core.config import settings

def require_webhook_secret(secret: str | None):
    """
    Checks the webhook secret sent in the request header in a secure way.
    Raises HTTP 401 if missing or invalid.
    Used for MinIO → Backend webhook validation.
    """
    if not secret or not secrets.compare_digest(secret, settings.webhook_secret):
        raise HTTPException(status_code=401, detail="invalid webhook secret")