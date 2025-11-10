"""
This script is the main entry point for launching the FastAPI web server.
It uses Uvicorn, an ASGI server, to run the application.
This script is typically the command executed when running the API in a Docker container
or directly for development.
"""
import uvicorn
from app.main import app
from app.core.config import settings

if __name__ == "__main__":
    # A simple print statement to confirm that the server is starting and on which address.
    print(f"Starting API server on {settings.api_host}:{settings.api_port}...")
    
    # This command starts the Uvicorn server.
    uvicorn.run(
        # "app.main:app" tells Uvicorn where to find the FastAPI app instance.
        # It means: in the 'app' package, inside the 'main' module, find the variable named 'app'.
        "app.main:app",
        host=settings.api_host,  # The host address to bind to, from config.
        port=settings.api_port,  # The port to listen on, from config.
        reload=False              # reload=True enables auto-reloading for development.
                                 # The server will restart automatically on code changes.
                                 # This should be set to False in production.
    )