import os
from dotenv import load_dotenv

# Find and load the .env file from the project root
# This allows the app to be run from the 'testy' subdirectory
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

class Settings:
    """
    Centralized configuration for the backend.
    Reads all environment variables required for the API, database, MinIO, and security.
    Finds and loads the .env file from the project root.
    """

    # API
    api_host = os.getenv("API_HOST", "0.0.0.0")
    api_port = int(os.getenv("API_PORT", "8000"))

    # Frontend
    cors_origin = os.getenv("DASH_ALLOW_ORIGINS", "http://localhost:5173")

    # Database
    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_user = os.getenv("POSTGRES_USER", "booklatte")
    db_password = os.getenv("POSTGRES_PASSWORD", "password")
    db_name = os.getenv("POSTGRES_DB", "booklatte")

    # Object Storage
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_access = os.getenv("MINIO_ROOT_USER", "booklatte")
    minio_secret = os.getenv("MINIO_ROOT_PASSWORD", "password")
    minio_secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    minio_bucket = os.getenv("MINIO_UPLOAD_BUCKET", "uploads")

    # Pipeline Trigger
    trigger_dir = os.getenv("TRIGGER_DIR", "/app/trigger")

    # Authentication
    keycloak_issuer = os.getenv("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/booklatte")
    keycloak_client_id = os.getenv("KEYCLOAK_CLIENT_ID", "frontend")

settings = Settings()