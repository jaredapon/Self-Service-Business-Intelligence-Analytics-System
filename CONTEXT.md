High-level components (from docker-compose.yml)
Postgres (postgres): Database. Exposes 5432, persists data to db_data. Provides health check for readiness.
MinIO (minio): S3-compatible storage. Exposes 9000 (API) and 9001 (console). Persists data to minio_data. Has a readiness health check.
Keycloak (keycloak): Auth server. Uses Postgres. Exposes 8080. Waits for Postgres readiness; has its own readiness check.
Backend API (backend-api): FastAPI app (served by Uvicorn) built from ./backend/Dockerfile. Exposes 8000. Depends on the healthy state of Postgres, MinIO, and Keycloak. Shares a trigger directory via a named volume.
Observer (observer): Sidecar-like service built from the same backend image; runs run_observer.py. Depends on the Backend API being healthy. Shares the same trigger directory volume.
Startup and dependency flow
Postgres and MinIO start first, each with health checks.
Keycloak starts after Postgres is healthy, with its own readiness health check.
Backend API starts after Postgres, MinIO, and Keycloak are healthy.
Observer starts only after the Backend API reports healthy.
Backend API runtime flow
Build/run: Uses the backend image, runs uvicorn app.main:app --host 0.0.0.0 --port 8000.
Configuration sources:
Loads defaults from ./backend/.env.
Overrides critical in-container endpoints:
POSTGRES_HOST=postgres
MINIO_ENDPOINT=minio:9000
KEYCLOAK_ISSUER=http://keycloak:${KEYCLOAK_PORT:-8080}/realms/booklatte
Optional DB pool tuning: DB_POOL_SIZE, DB_MAX_OVERFLOW, DB_POOL_RECYCLE, DB_POOL_TIMEOUT.
TRIGGER_DIR=/app/trigger is set for file-based signaling.
Networking/ports: Publishes ${API_PORT:-8000} on the host to container 8000. All services share the app-net network for service-name DNS.
Storage/IPC: Mounts the named volume trigger_volume at /app/trigger to coordinate with observer.
Health check: Periodically requests http://localhost:8000/openapi.json; container is considered healthy on HTTP 200.
Observer sidecar flow (interaction with Backend API)
Purpose: Watches the shared TRIGGER_DIR (/app/trigger) for file-based events and performs background/async work (e.g., interacting with MinIO), complementary to the Backend API’s synchronous requests.
Config: Uses the same .env, plus MINIO_ENDPOINT=minio:9000 and TRIGGER_DIR=/app/trigger.
Coordination: Only starts after the Backend API is healthy; both containers share trigger_volume.
Data and control paths
Client → Backend API: Clients call :8000 on the host; requests route to the API inside the backend-api container.
Backend API → Postgres: Uses service DNS postgres for DB connections.
Backend API → Keycloak: Uses http://keycloak:8080/... for auth/issuer discovery.
Backend API ↔ MinIO: Uses minio:9000 for object storage operations.
Backend API ↔ Observer via trigger_volume: API can emit files/signals into /app/trigger; Observer consumes them to execute background tasks.
Ensures reliable startup via chained health checks so the Backend API only runs once its dependencies are ready, and the Observer only runs once the API is ready.