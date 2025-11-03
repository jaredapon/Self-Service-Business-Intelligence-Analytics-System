import uvicorn
from app.main import app
from app.core.config import settings

if __name__ == "__main__":
    print(f"Starting API server on {settings.API_HOST}:{settings.API_PORT}...")
    
    # This runs the Uvicorn server, loading the 'app' object from 'app.main'
    uvicorn.run(
        "app.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=True  # reload=True is great for development
    )