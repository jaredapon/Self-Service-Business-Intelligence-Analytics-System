"""
Centralized application configuration.

This module defines a `Settings` class that reads all configuration values
from environment variables. It is designed for a Docker-first workflow,
where Docker Compose injects the necessary variables at runtime.
"""

import os
from typing import List
from urllib.parse import quote_plus
from dotenv import load_dotenv
load_dotenv() 

def _str_to_bool(v: str | None, default: bool = False) -> bool:
    """
    Safely converts a string from an environment variable to a boolean value.
    Handles common string representations of 'true' like "1", "true", "yes", or "on".
    """
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")

class Settings:
    # API
    api_host: str = os.getenv("API_HOST", "0.0.0.0")
    api_port: int = int(os.getenv("API_PORT", "8000"))

    # Frontend / CORS
    _cors_origins_raw = os.getenv("CORS_ALLOW_ORIGINS", "http://localhost:5173")
    cors_origins: List[str] = [o.strip() for o in _cors_origins_raw.split(",") if o.strip()]

    # Database (components)
    db_host: str = os.getenv("POSTGRES_HOST", "localhost")
    db_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    db_user: str = os.getenv("POSTGRES_USER", "booklatte")
    db_password: str = os.getenv("POSTGRES_PASSWORD", "password")
    db_name: str = os.getenv("POSTGRES_DB", "booklatte")

    # Full SQLAlchemy URL (DATABASE_URL overrides components if provided)
    database_url: str = os.getenv("DATABASE_URL") or (
        f"postgresql+psycopg2://{quote_plus(db_user)}:{quote_plus(db_password)}"
        f"@{db_host}:{db_port}/{db_name}"
    )

    # DB Pool tuning
    db_pool_size: int = int(os.getenv("DB_POOL_SIZE", "5"))
    db_max_overflow: int = int(os.getenv("DB_MAX_OVERFLOW", "10"))
    db_pool_recycle: int = int(os.getenv("DB_POOL_RECYCLE", "1800"))
    db_pool_timeout: int = int(os.getenv("DB_POOL_TIMEOUT", "30"))

    # Object Storage (MinIO)
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access: str = os.getenv("MINIO_ROOT_USER", "booklatte")
    minio_secret: str = os.getenv("MINIO_ROOT_PASSWORD", "password")
    minio_secure: bool = _str_to_bool(os.getenv("MINIO_SECURE", "false"))
    minio_landing_bucket: str = os.getenv("MINIO_LANDING_BUCKET", "landing")
    minio_raw_sales_folder: str = os.getenv("MINIO_RAW_SALES_FOLDER", "raw_sales_by_transaction")
    minio_raw_sales_by_product_folder: str = os.getenv("MINIO_RAW_SALES_BY_PRODUCT_FOLDER", "raw_sales_by_product")
    minio_staging_bucket: str = os.getenv("MINIO_STAGING_BUCKET", "staging")
    minio_etl_folder: str = os.getenv("MINIO_ETL_FOLDER", "etl")
    
    # Pipeline Trigger
    # Use backend/trigger for local dev, /app/trigger for Docker
    import pathlib
    _backend_dir = pathlib.Path(__file__).parent.parent.parent  # Navigate to backend/
    _default_trigger = str(_backend_dir / "trigger") if not os.path.exists("/app") else "/app/trigger"
    _trigger_dir_raw: str = os.getenv("TRIGGER_DIR", _default_trigger)
    trigger_dir: str = os.path.normpath(os.path.abspath(_trigger_dir_raw))  # Normalize and make absolute

    # Authentication (Keycloak)
    keycloak_issuer: str = os.getenv("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/booklatte")
    keycloak_client_id: str = os.getenv("KEYCLOAK_CLIENT_ID", "frontend")

settings = Settings()