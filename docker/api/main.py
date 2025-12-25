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
from routers import auth, data, metadata, tracker, system, chat, dictionary, meddra, golden_suite, docs, audit, users

# Import middleware
try:
    from middleware.audit import AuditMiddleware
    AUDIT_MIDDLEWARE_AVAILABLE = True
except ImportError:
    AUDIT_MIDDLEWARE_AVAILABLE = False

# Import audit service for lifecycle logging
try:
    from core.audit import get_audit_service
    AUDIT_SERVICE_AVAILABLE = True
except ImportError:
    AUDIT_SERVICE_AVAILABLE = False

# Import user migration
try:
    from core.users import migrate_from_env_user
    USERS_AVAILABLE = True
except ImportError:
    USERS_AVAILABLE = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    print("SAGE API starting up...")

    # Migrate admin user from environment if needed
    if USERS_AVAILABLE:
        try:
            if migrate_from_env_user():
                print("Admin user migrated from environment variables")
            else:
                print("Users database already initialized")
        except Exception as e:
            print(f"Warning: User migration failed: {e}")

    # Log startup to audit
    if AUDIT_SERVICE_AVAILABLE:
        try:
            audit_service = get_audit_service()
            audit_service.log_system_startup(version="1.0.0")
        except Exception as e:
            print(f"Warning: Could not log startup to audit: {e}")

    yield

    # Shutdown
    print("SAGE API shutting down...")
    if AUDIT_SERVICE_AVAILABLE:
        try:
            audit_service = get_audit_service()
            audit_service.log_system_shutdown(reason="normal")
        except Exception as e:
            print(f"Warning: Could not log shutdown to audit: {e}")


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
# Security: Default to localhost origins only; configure CORS_ORIGINS in .env for production
_cors_origins = os.getenv("CORS_ORIGINS", "http://localhost,http://localhost:8501,http://localhost:80").split(",")
_cors_origins = [origin.strip() for origin in _cors_origins if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Requested-With"],
)

# Add audit middleware (logs all API requests)
if AUDIT_MIDDLEWARE_AVAILABLE and os.getenv("AUDIT_LOG_API_REQUESTS", "true").lower() == "true":
    app.add_middleware(AuditMiddleware)


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

app.include_router(
    golden_suite.router,
    prefix="/api/v1/golden-suite",
    tags=["Golden Test Suite"]
)

app.include_router(
    docs.router,
    prefix="/api/v1/docs",
    tags=["Documentation"]
)

app.include_router(
    audit.router,
    prefix="/api/v1/audit",
    tags=["Audit Logs"]
)

app.include_router(
    users.router,
    prefix="/api/v1/users",
    tags=["User Management"]
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
            "audit": "/api/v1/audit",
            "chat": "/api/v1/chat",
            "data": "/api/v1/data",
            "metadata": "/api/v1/metadata",
            "dictionary": "/api/v1/dictionary",
            "meddra": "/api/v1/meddra",
            "tracker": "/api/v1/tracker",
            "system": "/api/v1/system",
            "golden_suite": "/api/v1/golden-suite",
            "docs": "/api/v1/docs",
            "users": "/api/v1/users"
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
