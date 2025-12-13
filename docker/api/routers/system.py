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
            "name": "ollama",
            "status": "unknown",
            "port": int(os.getenv("OLLAMA_PORT", "11434"))
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

    valid_services = ["admin-ui", "chat-ui", "ollama", "api"]
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
