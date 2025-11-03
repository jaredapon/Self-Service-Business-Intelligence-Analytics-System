# Business Analytics Platform - Backend

This backend service provides the core functionality for the Business Analytics Platform. It handles secure file uploads, stores them in a MinIO object storage, and triggers a data processing pipeline.

## Features

- **Secure File Upload:** Endpoint protected by JWT authentication via Keycloak.
- **File Validation:** Accepts only `.csv` and `.xlsx` files.
- **Object Storage:** Uploads files directly to a MinIO bucket.
- **Pipeline Trigger:** Creates a trigger file that a separate observer service watches to initiate ETL, MBA, and forecasting jobs.
- **Containerized:** Fully containerized with Docker for easy setup and deployment.

## Technology Stack

- **Framework:** FastAPI
- **Authentication:** Keycloak (JWT)
- **Object Storage:** MinIO
- **Database:** PostgreSQL (for Keycloak)
- **Containerization:** Docker & Docker Compose
- **Observer:** Watchdog

---

## Getting Started

### Prerequisites

- Docker
- Docker Compose

### Setup

1.  **Environment File:**
    The project uses an `.env` file for configuration. A template is provided in the root of the repository. Rename `.env.example` to `.env` and update the variables if necessary. The default values are configured to work with the provided `docker-compose.yml`.

2.  **Keycloak Configuration:**
    After starting the services, you will need to configure Keycloak:
    - Navigate to `http://localhost:8080`.
    - Log in with the admin credentials from your `.env` file (default: `admin`/`password`).
    - Create a new realm named `booklatte`.
    - Inside the `booklatte` realm, create a new client with `Client ID` set to `frontend`.
    - In the client settings, set `Valid Redirect URIs` to your frontend's URL (e.g., `http://localhost:5173/*`) and save.

### Running the Application with Docker

This is the recommended way to run the entire application stack.

From the `backend` directory, run:

```bash
docker-compose up --build
```

This command will:
- Build the Docker image for the backend application.
- Start containers for the API, the observer, PostgreSQL, MinIO, and Keycloak.
- The API will be available at `http://localhost:8000`.
- The MinIO console will be at `http://localhost:9001`.
- The Keycloak admin console will be at `http://localhost:8080`.

---

## API Endpoint

### `POST /upload/upload`

-   **Description:** Uploads one or more files to be processed.
-   **Authentication:** `Bearer Token` required.
-   **Body:** `multipart/form-data` with one or more `files`.
-   **Success Response (200):**
    ```json
    {
      "message": "Files uploaded successfully and pipeline triggered.",
      "uploaded_files": ["file1.csv", "file2.xlsx"]
    }
    ```
-   **Error Responses:**
    -   `400 Bad Request`: No valid files were provided.
    -   `401 Unauthorized`: Invalid or missing JWT.
    -   `500 Internal Server Error`: An issue occurred during upload or trigger creation.