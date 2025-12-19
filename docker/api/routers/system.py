# SAGE API - System Router
# =========================
"""System endpoints for health, info, logs, and administration."""

import os
import sys
import platform
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Depends

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from .auth import get_current_user

router = APIRouter()

# Configuration
DATA_DIR = Path(os.getenv("DATA_DIR", project_root / "data"))
LOGS_DIR = Path(os.getenv("LOGS_DIR", project_root / "logs"))
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", project_root / "backups"))

# Track server start time
SERVER_START_TIME = datetime.now()


def get_uptime() -> str:
    """Calculate server uptime."""
    delta = datetime.now() - SERVER_START_TIME
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")

    return " ".join(parts)


# ============================================
# Health Endpoints
# ============================================

@router.get("/health")
async def health_check():
    """
    Basic health check.

    No authentication required.
    """
    return {
        "success": True,
        "data": {
            "status": "healthy",
            "timestamp": datetime.now().isoformat()
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/health/detailed")
async def detailed_health_check(current_user: dict = Depends(get_current_user)):
    """
    Detailed health check for all services.
    """
    services = {}

    # Check DuckDB
    try:
        import duckdb
        db_path = DATA_DIR / "database" / "clinical.duckdb"
        if db_path.exists():
            conn = duckdb.connect(str(db_path), read_only=True)
            conn.execute("SELECT 1").fetchone()
            conn.close()
            services["duckdb"] = "healthy"
        else:
            services["duckdb"] = "not_configured"
    except Exception as e:
        services["duckdb"] = f"unhealthy: {str(e)}"

    # Check Tracker DB
    try:
        tracker_path = project_root / "tracker" / "project_tracker.db"
        if tracker_path.exists():
            import sqlite3
            conn = sqlite3.connect(str(tracker_path))
            conn.execute("SELECT 1").fetchone()
            conn.close()
            services["tracker_db"] = "healthy"
        else:
            services["tracker_db"] = "not_configured"
    except Exception as e:
        services["tracker_db"] = f"unhealthy: {str(e)}"

    # Check Metadata Store
    try:
        metadata_path = project_root / "knowledge" / "golden_metadata.json"
        if metadata_path.exists():
            services["metadata_store"] = "healthy"
        else:
            services["metadata_store"] = "not_configured"
    except Exception as e:
        services["metadata_store"] = f"unhealthy: {str(e)}"

    # Check ChromaDB (if configured)
    try:
        chroma_path = project_root / "knowledge" / "chroma"
        if chroma_path.exists():
            services["chromadb"] = "configured"
        else:
            services["chromadb"] = "not_configured"
    except Exception:
        services["chromadb"] = "not_configured"

    # Overall status
    unhealthy = [k for k, v in services.items() if "unhealthy" in str(v)]
    overall = "unhealthy" if unhealthy else "healthy"

    return {
        "success": True,
        "data": {
            "status": overall,
            "services": services,
            "uptime": get_uptime()
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# System Info Endpoints
# ============================================

@router.get("/info")
async def get_system_info(current_user: dict = Depends(get_current_user)):
    """
    Get system information.
    """
    # Disk usage
    disk = shutil.disk_usage(project_root)
    disk_info = {
        "total_gb": round(disk.total / (1024**3), 2),
        "used_gb": round(disk.used / (1024**3), 2),
        "free_gb": round(disk.free / (1024**3), 2),
        "percent_used": round((disk.used / disk.total) * 100, 1)
    }

    # Memory (if psutil available)
    try:
        import psutil
        mem = psutil.virtual_memory()
        memory_info = {
            "total_gb": round(mem.total / (1024**3), 2),
            "available_gb": round(mem.available / (1024**3), 2),
            "percent_used": mem.percent
        }
    except ImportError:
        memory_info = {"status": "psutil not installed"}

    return {
        "success": True,
        "data": {
            "version": os.getenv("SAGE_VERSION", "1.0.0"),
            "uptime": get_uptime(),
            "platform": platform.system(),
            "platform_version": platform.version(),
            "python_version": platform.python_version(),
            "disk_usage": disk_info,
            "memory_usage": memory_info,
            "project_root": str(project_root)
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/config")
async def get_configuration(current_user: dict = Depends(get_current_user)):
    """
    Get current configuration.

    Requires admin role.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    # Return non-sensitive configuration
    config = {
        "data_dir": str(DATA_DIR),
        "logs_dir": str(LOGS_DIR),
        "backup_dir": str(BACKUP_DIR),
        "auth_provider": os.getenv("AUTH_PROVIDER", "local"),
        "access_token_expire_minutes": int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60")),
        "refresh_token_expire_days": int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7")),
        "llm_model": os.getenv("LLM_MODEL", "llama3.1:70b"),
        "embedding_model": os.getenv("EMBEDDING_MODEL", "nomic-embed-text"),
        "confidence_threshold": float(os.getenv("CONFIDENCE_THRESHOLD", "0.7"))
    }

    return {
        "success": True,
        "data": config,
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.put("/config")
async def update_configuration(
    key: str,
    value: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Update a configuration value.

    Requires admin role. Changes are runtime only.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    # Only allow certain keys to be updated
    allowed_keys = [
        "LLM_MODEL", "EMBEDDING_MODEL", "CONFIDENCE_THRESHOLD",
        "ACCESS_TOKEN_EXPIRE_MINUTES"
    ]

    if key.upper() not in allowed_keys:
        raise HTTPException(
            status_code=422,
            detail={"code": "VALIDATION_ERROR", "message": f"Cannot update key: {key}"}
        )

    # Update environment variable (runtime only)
    os.environ[key.upper()] = value

    return {
        "success": True,
        "data": {"message": f"Configuration updated: {key}={value} (runtime only)"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# Logs Endpoints
# ============================================

@router.get("/logs")
async def get_logs(
    level: Optional[str] = None,
    service: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=1000),
    current_user: dict = Depends(get_current_user)
):
    """
    Get recent log entries.
    """
    logs = []

    # Read from log files if they exist
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    for log_file in LOGS_DIR.glob("*.log"):
        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()[-limit:]
                for line in lines:
                    entry = {
                        "timestamp": datetime.now().isoformat(),
                        "level": "INFO",
                        "service": log_file.stem,
                        "message": line.strip()
                    }

                    # Apply filters
                    if level and entry["level"].upper() != level.upper():
                        continue
                    if service and entry["service"] != service:
                        continue

                    logs.append(entry)
        except Exception:
            continue

    # Sort by timestamp descending and limit
    logs = logs[:limit]

    return {
        "success": True,
        "data": logs,
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(logs)}
    }


@router.get("/logs/audit")
async def get_audit_logs(
    user: Optional[str] = None,
    action: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=1000),
    current_user: dict = Depends(get_current_user)
):
    """
    Get audit log entries.

    Requires admin role.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    audit_logs = []

    # Get from tracker activity log
    try:
        from core.admin.tracker_db import TrackerDB
        tracker_path = project_root / "tracker" / "project_tracker.db"
        if tracker_path.exists():
            db = TrackerDB(str(tracker_path))
            activities = db.get_activity_log(limit=limit)
            for activity in activities:
                if user and activity.get("user") != user:
                    continue
                if action and activity.get("action") != action:
                    continue
                audit_logs.append(activity)
    except Exception:
        pass

    return {
        "success": True,
        "data": audit_logs,
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(audit_logs)}
    }


# ============================================
# Services Endpoints
# ============================================

@router.get("/services")
async def list_services(current_user: dict = Depends(get_current_user)):
    """
    List all SAGE services and their status.
    """
    services = [
        {
            "name": "api",
            "status": "running",
            "uptime": get_uptime(),
            "port": int(os.getenv("API_PORT", "8000"))
        },
        {
            "name": "admin-ui",
            "status": "unknown",
            "port": int(os.getenv("ADMIN_PORT", "8501"))
        },
        {
            "name": "chat-ui",
            "status": "unknown",
            "port": int(os.getenv("CHAT_PORT", "8502"))
        },
        {
            "name": "chromadb",
            "status": "unknown",
            "port": int(os.getenv("CHROMA_PORT", "8000"))
        }
    ]

    # Check if services are responding
    import socket

    for service in services[1:]:  # Skip API (we know it's running)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('localhost', service["port"]))
            if result == 0:
                service["status"] = "running"
            else:
                service["status"] = "stopped"
            sock.close()
        except Exception:
            service["status"] = "unknown"

    return {
        "success": True,
        "data": services,
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/services/{name}/restart")
async def restart_service(
    name: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Restart a service.

    Requires admin role.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    valid_services = ["admin-ui", "chat-ui", "chromadb", "api"]
    if name not in valid_services:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Unknown service: {name}"}
        )

    # In Docker environment, this would use docker-compose restart
    # For now, return acknowledgment
    return {
        "success": True,
        "data": {
            "message": f"Restart requested for service: {name}",
            "note": "Service restart requires Docker environment"
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# Backup Endpoints
# ============================================

@router.get("/backups")
async def list_backups(current_user: dict = Depends(get_current_user)):
    """
    List available backups.
    """
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backups = []

    for backup_file in BACKUP_DIR.glob("*.zip"):
        stat = backup_file.stat()
        backups.append({
            "filename": backup_file.name,
            "size": stat.st_size,
            "created_at": datetime.fromtimestamp(stat.st_mtime).isoformat()
        })

    # Sort by date descending
    backups.sort(key=lambda x: x["created_at"], reverse=True)

    return {
        "success": True,
        "data": backups,
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(backups)}
    }


@router.post("/backups")
async def create_backup(current_user: dict = Depends(get_current_user)):
    """
    Create a new backup.

    Requires admin role.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    import zipfile

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"sage_backup_{timestamp}.zip"
    backup_path = BACKUP_DIR / backup_filename

    try:
        with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Backup tracker database
            tracker_db = project_root / "tracker" / "project_tracker.db"
            if tracker_db.exists():
                zipf.write(tracker_db, "tracker/project_tracker.db")

            # Backup metadata
            metadata_file = project_root / "knowledge" / "golden_metadata.json"
            if metadata_file.exists():
                zipf.write(metadata_file, "knowledge/golden_metadata.json")

            # Backup DuckDB
            duckdb_file = DATA_DIR / "database" / "clinical.duckdb"
            if duckdb_file.exists():
                zipf.write(duckdb_file, "data/database/clinical.duckdb")

        stat = backup_path.stat()

        return {
            "success": True,
            "data": {
                "filename": backup_filename,
                "size": stat.st_size,
                "created_at": datetime.now().isoformat()
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": f"Backup failed: {str(e)}"}
        )


@router.post("/backups/{filename}/restore")
async def restore_backup(
    filename: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Restore from a backup.

    Requires admin role.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    backup_path = BACKUP_DIR / filename
    if not backup_path.exists():
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Backup not found: {filename}"}
        )

    import zipfile

    try:
        with zipfile.ZipFile(backup_path, 'r') as zipf:
            # Extract to project root, preserving directory structure
            zipf.extractall(project_root)

        return {
            "success": True,
            "data": {"message": f"Backup restored: {filename}"},
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": f"Restore failed: {str(e)}"}
        )


@router.delete("/backups/{filename}")
async def delete_backup(
    filename: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Delete a backup file.

    Requires admin role.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    backup_path = BACKUP_DIR / filename
    if not backup_path.exists():
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Backup not found: {filename}"}
        )

    backup_path.unlink()

    return {
        "success": True,
        "data": {"message": f"Backup deleted: {filename}"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# Cache Management Endpoints
# ============================================

@router.get("/cache")
async def get_cache_stats(current_user: dict = Depends(get_current_user)):
    """
    Get query cache statistics.

    Returns cache size, hit rate, and data version information.
    """
    try:
        from core.engine.cache import get_query_cache

        # Get or create cache with DuckDB path
        db_path = str(DATA_DIR / "database" / "clinical.duckdb")
        cache = get_query_cache(db_path=db_path)
        stats = cache.get_stats()

        return {
            "success": True,
            "data": stats,
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        return {
            "success": True,
            "data": {
                "size": 0,
                "max_size": 1000,
                "hits": 0,
                "misses": 0,
                "error": str(e)
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }


@router.get("/cache/detailed")
async def get_cache_detailed_stats(current_user: dict = Depends(get_current_user)):
    """
    Get detailed query cache statistics including age distribution.

    Returns comprehensive stats with oldest/newest entry ages and TTL info.
    """
    try:
        from core.engine.cache import get_query_cache

        # Get or create cache with DuckDB path
        db_path = str(DATA_DIR / "database" / "clinical.duckdb")
        cache = get_query_cache(db_path=db_path)
        stats = cache.get_detailed_stats()

        return {
            "success": True,
            "data": stats,
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        return {
            "success": True,
            "data": {
                "size": 0,
                "max_size": 1000,
                "hits": 0,
                "misses": 0,
                "hit_rate": "0.0%",
                "error": str(e)
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }


@router.post("/cache/clear")
async def clear_cache(current_user: dict = Depends(get_current_user)):
    """
    Clear the query response cache.

    Use this after loading new data to ensure queries return fresh results.
    Requires admin role.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    try:
        from core.engine.cache import get_query_cache

        cache = get_query_cache()
        entries_before = len(cache)
        cache.clear()

        return {
            "success": True,
            "data": {
                "message": "Cache cleared successfully",
                "entries_cleared": entries_before
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": f"Failed to clear cache: {str(e)}"}
        )


@router.get("/cache/entries")
async def get_cache_entries(
    limit: int = Query(default=50, ge=1, le=200),
    current_user: dict = Depends(get_current_user)
):
    """
    Get information about cached entries.

    Requires admin role.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    try:
        from core.engine.cache import get_query_cache

        cache = get_query_cache()
        entries = cache.get_entries()[:limit]

        return {
            "success": True,
            "data": entries,
            "meta": {"timestamp": datetime.now().isoformat(), "count": len(entries)}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": f"Failed to get cache entries: {str(e)}"}
        )


# ============================================
# LLM Provider Endpoints
# ============================================

@router.get("/llm")
async def get_llm_config(current_user: dict = Depends(get_current_user)):
    """
    Get current LLM configuration.

    Returns provider info, model, and safety settings.
    """
    try:
        from core.engine.llm_providers import LLMConfig, get_available_providers

        config = LLMConfig.from_env()
        available = get_available_providers()

        return {
            "success": True,
            "data": {
                "provider": config.provider.value,
                "model": config.claude_model,
                "available_providers": available,
                "settings": {
                    "temperature": config.temperature,
                    "max_tokens": config.max_tokens,
                    "timeout": config.timeout,
                    "safety_audit_enabled": config.enable_safety_audit,
                    "block_pii": config.block_potential_pii
                },
                "claude": {
                    "model": config.claude_model,
                    "api_key_configured": bool(config.anthropic_api_key)
                }
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": f"Failed to get LLM config: {str(e)}"}
        )


@router.get("/llm/providers")
async def list_llm_providers(current_user: dict = Depends(get_current_user)):
    """
    List available LLM providers and their status.
    """
    try:
        from core.engine.llm_providers import get_available_providers, LLMConfig

        config = LLMConfig.from_env()
        available = get_available_providers()

        providers = [
            {
                "id": "claude",
                "name": "Claude (Anthropic API)",
                "description": "Anthropic's Claude API for SQL generation. Only schema metadata is sent.",
                "available": available.get("claude", False),
                "is_local": False,
                "requires_api_key": True,
                "api_key_configured": bool(config.anthropic_api_key),
                "models": ["claude-sonnet-4-20250514", "claude-3-5-sonnet-20241022", "claude-3-haiku-20240307"]
            },
            {
                "id": "mock",
                "name": "Mock (Testing)",
                "description": "Mock provider for testing. Returns pre-defined SQL patterns.",
                "available": True,
                "is_local": True,
                "requires_api_key": False,
                "models": ["mock-model"]
            }
        ]

        return {
            "success": True,
            "data": {
                "current_provider": config.provider.value,
                "providers": providers
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": f"Failed to list providers: {str(e)}"}
        )


@router.post("/llm/provider")
async def set_llm_provider(
    provider: str = Query(..., description="Provider to use: claude or mock"),
    model: Optional[str] = Query(None, description="Optional model override"),
    api_key: Optional[str] = Query(None, description="API key (for claude)"),
    current_user: dict = Depends(get_current_user)
):
    """
    Change the LLM provider.

    Requires admin role.

    Note: This changes the runtime configuration. For persistent changes,
    update environment variables.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    try:
        from core.engine.llm_providers import LLMProvider, set_provider, LLMConfig

        # Validate provider
        try:
            provider_enum = LLMProvider(provider.lower())
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail={"code": "INVALID_PROVIDER", "message": f"Invalid provider: {provider}. Use: claude or mock"}
            )

        # Build kwargs
        kwargs = {}
        if model:
            kwargs["claude_model"] = model

        if api_key and provider_enum == LLMProvider.CLAUDE:
            kwargs["anthropic_api_key"] = api_key

        # Set the provider
        new_provider = set_provider(provider_enum, **kwargs)

        # Verify it's available
        if not new_provider.is_available():
            return {
                "success": True,
                "data": {
                    "provider": provider_enum.value,
                    "model": new_provider.get_model_name(),
                    "available": False,
                    "warning": "Provider set but not currently available. Check configuration."
                },
                "meta": {"timestamp": datetime.now().isoformat()}
            }

        return {
            "success": True,
            "data": {
                "provider": provider_enum.value,
                "model": new_provider.get_model_name(),
                "available": True,
                "message": f"LLM provider changed to {provider_enum.value}"
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": f"Failed to set provider: {str(e)}"}
        )


@router.post("/llm/test")
async def test_llm_connection(current_user: dict = Depends(get_current_user)):
    """
    Test the current LLM provider connection.

    Sends a simple test prompt and returns the result.
    """
    try:
        from core.engine.llm_providers import get_current_provider, LLMRequest

        provider = get_current_provider()

        if not provider.is_available():
            return {
                "success": False,
                "data": {
                    "provider": provider.get_provider_name(),
                    "model": provider.get_model_name(),
                    "status": "unavailable",
                    "message": "Provider is not available. Check configuration."
                },
                "meta": {"timestamp": datetime.now().isoformat()}
            }

        # Send a simple test request
        import time
        start = time.time()

        request = LLMRequest(
            prompt="Generate a simple SQL query: SELECT 1 as test",
            system_prompt="You are a SQL assistant. Return only SQL.",
            max_tokens=100,
            temperature=0.1
        )

        response = provider.generate(request)
        elapsed_ms = (time.time() - start) * 1000

        return {
            "success": True,
            "data": {
                "provider": provider.get_provider_name(),
                "model": provider.get_model_name(),
                "status": "connected",
                "response_time_ms": round(elapsed_ms, 2),
                "response_preview": response.content[:200] if response.content else None,
                "message": "LLM connection successful"
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    except Exception as e:
        return {
            "success": False,
            "data": {
                "status": "error",
                "error": str(e),
                "message": f"LLM connection failed: {str(e)}"
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }


@router.get("/llm/audit")
async def get_llm_audit_log(
    limit: int = Query(default=100, ge=1, le=1000),
    date: Optional[str] = Query(None, description="Date in YYYYMMDD format"),
    current_user: dict = Depends(get_current_user)
):
    """
    Get LLM audit log entries.

    Requires admin role.

    Shows what data was sent to external LLM APIs for compliance auditing.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    try:
        import json
        from core.engine.llm_providers import LLMConfig

        config = LLMConfig.from_env()
        audit_dir = Path(config.audit_log_path) if config.audit_log_path else LOGS_DIR / "llm_audit"

        if not audit_dir.exists():
            return {
                "success": True,
                "data": {
                    "entries": [],
                    "message": "No audit logs found. Audit logging may not be configured."
                },
                "meta": {"timestamp": datetime.now().isoformat()}
            }

        # Find log files
        if date:
            log_files = list(audit_dir.glob(f"llm_audit_{date}.jsonl"))
        else:
            log_files = sorted(audit_dir.glob("llm_audit_*.jsonl"), reverse=True)

        entries = []
        for log_file in log_files[:5]:  # Check up to 5 most recent files
            try:
                with open(log_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            entries.append(json.loads(line))
                            if len(entries) >= limit:
                                break
            except Exception:
                continue

            if len(entries) >= limit:
                break

        # Sort by timestamp descending
        entries.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

        return {
            "success": True,
            "data": {
                "entries": entries[:limit],
                "total_found": len(entries),
                "audit_enabled": config.enable_safety_audit
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": f"Failed to read audit log: {str(e)}"}
        )
