"""
SAGE API - FastAPI Application
==============================
RESTful API for programmatic access to SAGE platform.

Features:
- Authentication with JWT tokens
- Data Factory endpoints
- Metadata Factory endpoints
- Project Tracker endpoints
- System management endpoints
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Add api directory to path for router imports
api_dir = Path(__file__).parent
sys.path.insert(0, str(api_dir))

# Import routers
from routers import auth, data, metadata, tracker, system, chat, dictionary, meddra


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    print("SAGE API starting up...")
    yield
    # Shutdown
    print("SAGE API shutting down...")


# Create FastAPI app
app = FastAPI(
    title="SAGE API",
    description="""
## Study Analytics Generative Engine - REST API

SAGE provides a comprehensive API for:
- **Authentication**: JWT-based user authentication
- **Data Factory**: SAS file processing, DuckDB queries
- **Metadata Factory**: Golden metadata management
- **Project Tracker**: Task and progress tracking
- **System**: Health checks, configuration

### Authentication
Most endpoints require a valid JWT token. Obtain one via `/api/v1/auth/login`.
Include the token in the Authorization header: `Bearer <token>`
""",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# Include Routers
# ============================================

app.include_router(
    auth.router,
    prefix="/api/v1/auth",
    tags=["Authentication"]
)

app.include_router(
    data.router,
    prefix="/api/v1/data",
    tags=["Data Factory"]
)

app.include_router(
    metadata.router,
    prefix="/api/v1/metadata",
    tags=["Metadata Factory"]
)

app.include_router(
    tracker.router,
    prefix="/api/v1/tracker",
    tags=["Project Tracker"]
)

app.include_router(
    system.router,
    prefix="/api/v1/system",
    tags=["System"]
)

app.include_router(
    chat.router,
    prefix="/api/v1/chat",
    tags=["Chat"]
)

app.include_router(
    dictionary.router,
    prefix="/api/v1/dictionary",
    tags=["Dictionary (Factory 3)"]
)

app.include_router(
    meddra.router,
    prefix="/api/v1/meddra",
    tags=["MedDRA Library"]
)


# ============================================
# Root Endpoints
# ============================================

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information."""
    return {
        "service": "SAGE API",
        "version": "1.0.0",
        "description": "Study Analytics Generative Engine - REST API",
        "docs": "/docs",
        "redoc": "/redoc",
        "health": "/api/v1/system/health",
        "api_base": "/api/v1"
    }


@app.get("/health", tags=["Root"])
async def health():
    """Simple health check endpoint at root level."""
    return {"status": "healthy", "service": "sage-api"}


@app.get("/api/v1", tags=["Root"])
async def api_root():
    """API v1 root endpoint."""
    return {
        "version": "1.0.0",
        "endpoints": {
            "auth": "/api/v1/auth",
            "chat": "/api/v1/chat",
            "data": "/api/v1/data",
            "metadata": "/api/v1/metadata",
            "dictionary": "/api/v1/dictionary",
            "meddra": "/api/v1/meddra",
            "tracker": "/api/v1/tracker",
            "system": "/api/v1/system"
        }
    }


# ============================================
# Global Exception Handler
# ============================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for unhandled errors."""
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": {
                "code": "INTERNAL_ERROR",
                "message": str(exc)
            },
            "meta": {
                "timestamp": datetime.now().isoformat(),
                "path": str(request.url)
            }
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("API_PORT", 8000)),
        reload=os.getenv("API_RELOAD", "true").lower() == "true"
    )
