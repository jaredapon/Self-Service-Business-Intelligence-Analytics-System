import os

class Settings:
    """
    Centralized configuration for the backend.
    Reads all environment variables required for database, MinIO, and security.
    """

    #Frontend
    cors_origin = os.getenv("DASH_ALLOW_ORIGINS", "http://localhost:5173")

    #Database
    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_user = os.getenv("POSTGRES_USER", "booklatte")
    db_password = os.getenv("POSTGRES_PASSWORD", "password")
    db_name = os.getenv("POSTGRES_DB", "booklatte")

    #Object Storage
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_access = os.getenv("MINIO_ROOT_USER", "booklatte")
    minio_secret = os.getenv("MINIO_ROOT_PASSWORD", "password")
    minio_secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    minio_bucket = os.getenv("MINIO_UPLOAD_BUCKET", "uploads")

    #Webhook Secret
    webhook_secret = os.getenv("WEBHOOK_SECRET", "devsecret")

    #Authentication
    keycloak_issuer = os.getenv("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/booklatte")
    keycloak_client_id = os.getenv("KEYCLOAK_CLIENT_ID", "frontend")

settings = Settings()