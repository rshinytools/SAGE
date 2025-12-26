# SAGE API - Dashboard Router
# ============================
"""Dashboard endpoints for aggregated statistics and analytics."""

import os
import sys
import json
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query, Depends

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from .auth import get_current_user
from .data import get_duckdb_connection

router = APIRouter()

# Configuration
DATA_DIR = Path(os.getenv("DATA_DIR", project_root / "data"))
KNOWLEDGE_DIR = Path(os.getenv("KNOWLEDGE_DIR", project_root / "knowledge"))


def _get_audit_stats() -> Dict[str, Any]:
    """Get query statistics from audit database."""
    audit_db_path = DATA_DIR / "audit.db"

    if not audit_db_path.exists():
        return {
            "today": 0,
            "total": 0,
            "avg_confidence": 0,
            "avg_execution_time_ms": 0,
            "confidence_distribution": {"high": 0, "medium": 0, "low": 0},
            "top_tables": []
        }

    try:
        conn = sqlite3.connect(str(audit_db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get today's date range
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        today_iso = today_start.isoformat()

        # Queries today (count chat/query actions - uppercase QUERY)
        cursor.execute("""
            SELECT COUNT(*) as count FROM audit_logs
            WHERE action IN ('QUERY', 'chat_query', 'query')
            AND timestamp >= ?
        """, (today_iso,))
        queries_today = cursor.fetchone()["count"]

        # Total queries
        cursor.execute("""
            SELECT COUNT(*) as count FROM audit_logs
            WHERE action IN ('QUERY', 'chat_query', 'query')
        """)
        queries_total = cursor.fetchone()["count"]

        # Average confidence score (from query_audit_details table - joined by audit_log_id)
        cursor.execute("""
            SELECT qad.confidence_score, qad.execution_time_ms
            FROM query_audit_details qad
            JOIN audit_logs al ON qad.audit_log_id = al.id
            WHERE al.action IN ('QUERY', 'chat_query', 'query')
            AND qad.confidence_score IS NOT NULL
            ORDER BY al.timestamp DESC
            LIMIT 100
        """)

        confidence_scores = []
        execution_times = []
        for row in cursor.fetchall():
            if row["confidence_score"] is not None:
                confidence_scores.append(float(row["confidence_score"]))
            if row["execution_time_ms"] is not None:
                execution_times.append(float(row["execution_time_ms"]))

        avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
        avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0

        # Confidence distribution
        high = sum(1 for c in confidence_scores if c >= 90)
        medium = sum(1 for c in confidence_scores if 70 <= c < 90)
        low = sum(1 for c in confidence_scores if c < 70)
        total_scores = len(confidence_scores) or 1

        # Top tables queried (from query_audit_details table)
        cursor.execute("""
            SELECT qad.tables_accessed
            FROM query_audit_details qad
            JOIN audit_logs al ON qad.audit_log_id = al.id
            WHERE al.action IN ('QUERY', 'chat_query', 'query')
            AND qad.tables_accessed IS NOT NULL
            ORDER BY al.timestamp DESC
            LIMIT 500
        """)

        table_counts = {}
        for row in cursor.fetchall():
            try:
                # tables_accessed is stored as JSON string like '["ADSL"]'
                tables_str = row["tables_accessed"]
                if tables_str:
                    tables = json.loads(tables_str) if tables_str.startswith('[') else [tables_str]
                    for table in tables:
                        if table:
                            table_counts[table] = table_counts.get(table, 0) + 1
            except (json.JSONDecodeError, ValueError):
                pass

        top_tables = [{"table": k, "count": v} for k, v in sorted(table_counts.items(), key=lambda x: x[1], reverse=True)[:5]]

        conn.close()

        return {
            "today": queries_today,
            "total": queries_total,
            "avg_confidence": round(avg_confidence, 1),
            "avg_execution_time_ms": round(avg_execution_time, 1),
            "confidence_distribution": {
                "high": high,
                "medium": medium,
                "low": low
            },
            "top_tables": top_tables
        }
    except Exception as e:
        return {
            "today": 0,
            "total": 0,
            "avg_confidence": 0,
            "avg_execution_time_ms": 0,
            "confidence_distribution": {"high": 0, "medium": 0, "low": 0},
            "top_tables": [],
            "error": str(e)
        }


def _relative_time(dt_str: str) -> str:
    """Convert datetime string to relative time."""
    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        now = datetime.now()
        diff = now - dt.replace(tzinfo=None)

        if diff.total_seconds() < 60:
            return "just now"
        elif diff.total_seconds() < 3600:
            mins = int(diff.total_seconds() / 60)
            return f"{mins} min ago"
        elif diff.total_seconds() < 86400:
            hours = int(diff.total_seconds() / 3600)
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        else:
            days = int(diff.total_seconds() / 86400)
            return f"{days} day{'s' if days > 1 else ''} ago"
    except Exception:
        return "unknown"


def _get_user_stats() -> Dict[str, Any]:
    """Get user statistics from users database."""
    users_db_path = DATA_DIR / "users.db"

    if not users_db_path.exists():
        return {
            "total": 0,
            "active_24h": 0,
            "by_access_level": {"admin": 0, "user_admin": 0, "chat_only": 0},
            "recent_logins": []
        }

    try:
        conn = sqlite3.connect(str(users_db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Total users
        cursor.execute("SELECT COUNT(*) as count FROM users WHERE is_active = 1")
        total_users = cursor.fetchone()["count"]

        # Active users in last 24h
        yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
        cursor.execute("""
            SELECT COUNT(*) as count FROM users
            WHERE last_login >= ? AND is_active = 1
        """, (yesterday,))
        active_24h = cursor.fetchone()["count"]

        # Access levels breakdown
        cursor.execute("SELECT permissions FROM users WHERE is_active = 1")
        full_admin = 0
        user_admin = 0
        chat_only = 0

        for row in cursor.fetchall():
            try:
                perms = json.loads(row["permissions"]) if row["permissions"] else []
                if "*" in perms:
                    full_admin += 1
                elif "user_admin" in perms:
                    user_admin += 1
                else:
                    chat_only += 1
            except (json.JSONDecodeError, TypeError):
                chat_only += 1

        # Recent logins
        cursor.execute("""
            SELECT username, last_login FROM users
            WHERE last_login IS NOT NULL AND is_active = 1
            ORDER BY last_login DESC
            LIMIT 5
        """)

        recent_logins = []
        for row in cursor.fetchall():
            recent_logins.append({
                "username": row["username"],
                "timestamp": row["last_login"],
                "relative": _relative_time(row["last_login"])
            })

        conn.close()

        return {
            "total": total_users,
            "active_24h": active_24h,
            "by_access_level": {
                "admin": full_admin,
                "user_admin": user_admin,
                "chat_only": chat_only
            },
            "recent_logins": recent_logins
        }
    except Exception as e:
        return {
            "total": 0,
            "active_24h": 0,
            "by_access_level": {"admin": 0, "user_admin": 0, "chat_only": 0},
            "recent_logins": [],
            "error": str(e)
        }


def _get_data_stats() -> Dict[str, Any]:
    """Get data statistics from DuckDB using shared connection."""
    try:
        conn = get_duckdb_connection()
        if conn is None:
            return {
                "total_tables": 0,
                "total_rows": 0,
                "total_columns": 0,
                "tables": [],
                "error": "DuckDB connection not available"
            }

        # Get table list
        tables_result = conn.execute("SHOW TABLES").fetchall()
        tables = []
        total_rows = 0
        total_columns = 0

        for (table_name,) in tables_result:
            if table_name.startswith("_"):
                continue
            try:
                count_result = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
                row_count = count_result[0] if count_result else 0

                cols_result = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
                col_count = len(cols_result)

                # Estimate table size (rough approximation)
                size_kb = int((row_count * col_count * 20) / 1024)  # ~20 bytes per cell average

                tables.append({
                    "name": table_name,
                    "rows": row_count,
                    "columns": col_count,
                    "size_kb": size_kb
                })
                total_rows += row_count
                total_columns += col_count
            except Exception:
                tables.append({
                    "name": table_name,
                    "rows": 0,
                    "columns": 0,
                    "size_kb": 0
                })

        # Don't close - using shared connection

        # Sort by row count descending
        tables.sort(key=lambda x: x["rows"], reverse=True)

        return {
            "total_tables": len(tables),
            "total_rows": total_rows,
            "total_columns": total_columns,
            "tables": tables[:10]  # Top 10 tables
        }
    except Exception as e:
        return {
            "total_tables": 0,
            "total_rows": 0,
            "total_columns": 0,
            "tables": [],
            "error": str(e)
        }


def _get_metadata_stats() -> Dict[str, Any]:
    """Get metadata statistics from golden_metadata.json."""
    metadata_path = KNOWLEDGE_DIR / "golden_metadata.json"

    if not metadata_path.exists():
        return {
            "total_variables": 0,
            "pending": 0,
            "approved": 0,
            "rejected": 0,
            "domains": []
        }

    try:
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)

        total_vars = 0
        pending = 0
        approved = 0
        rejected = 0
        domain_counts = {}

        # Handle different metadata structures
        if isinstance(metadata, dict):
            # Check if it's domain-based structure
            for domain_name, domain_data in metadata.items():
                if isinstance(domain_data, dict):
                    variables = domain_data.get("variables", {})
                    domain_approved = 0
                    if isinstance(variables, dict):
                        domain_counts[domain_name] = {"count": len(variables), "approved": 0}
                        for var_name, var_data in variables.items():
                            total_vars += 1
                            status = var_data.get("approval_status", var_data.get("status", "pending"))
                            if status == "approved":
                                approved += 1
                                domain_approved += 1
                            elif status == "rejected":
                                rejected += 1
                            else:
                                pending += 1
                        domain_counts[domain_name]["approved"] = domain_approved
                    elif isinstance(variables, list):
                        domain_counts[domain_name] = {"count": len(variables), "approved": 0}
                        total_vars += len(variables)
                        for var_data in variables:
                            status = var_data.get("approval_status", var_data.get("status", "pending"))
                            if status == "approved":
                                approved += 1
                                domain_approved += 1
                            elif status == "rejected":
                                rejected += 1
                            else:
                                pending += 1
                        domain_counts[domain_name]["approved"] = domain_approved

        # Sort domains by variable count
        domains = [{"name": k, "count": v["count"], "approved": v["approved"]} for k, v in domain_counts.items()]
        domains.sort(key=lambda x: x["count"], reverse=True)

        return {
            "total_variables": total_vars,
            "pending": pending,
            "approved": approved,
            "rejected": rejected,
            "domains": domains[:10]
        }
    except Exception as e:
        return {
            "total_variables": 0,
            "pending": 0,
            "approved": 0,
            "rejected": 0,
            "domains": [],
            "error": str(e)
        }


def _get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics."""
    try:
        from core.engine.cache import get_query_cache

        db_path = str(DATA_DIR / "database" / "clinical.duckdb")
        cache = get_query_cache(db_path=db_path)
        stats = cache.get_stats()

        return {
            "total_entries": stats.get("size", 0),
            "hit_rate": stats.get("hit_rate", 0),
            "size_mb": round(stats.get("size", 0) * 0.01, 2),  # Rough estimate
            "max_size_mb": round(stats.get("max_size", 1000) * 0.01, 2)
        }
    except Exception as e:
        return {
            "total_entries": 0,
            "hit_rate": 0,
            "size_mb": 0,
            "max_size_mb": 10,
            "error": str(e)
        }


def _get_llm_stats() -> Dict[str, Any]:
    """Get LLM provider statistics."""
    try:
        from core.engine.llm_providers import get_current_provider, LLMConfig

        config = LLMConfig.from_env()
        provider = get_current_provider()

        return {
            "provider": config.provider.value,
            "model": provider.get_model_name(),
            "status": "available" if provider.is_available() else "unavailable",
            "last_response_ms": None  # Could be tracked if needed
        }
    except Exception as e:
        return {
            "provider": "unknown",
            "model": "unknown",
            "status": "unavailable",
            "last_response_ms": None,
            "error": str(e)
        }


def _get_service_health() -> List[Dict[str, Any]]:
    """Get detailed service health - returns a list of services."""
    import time
    services = []

    # DuckDB - use shared connection
    try:
        start = time.time()
        conn = get_duckdb_connection()
        if conn is not None:
            conn.execute("SELECT 1").fetchone()
            latency = round((time.time() - start) * 1000, 1)
            services.append({"name": "DuckDB", "status": "healthy", "latency_ms": latency, "details": None})
        else:
            services.append({"name": "DuckDB", "status": "unknown", "latency_ms": None, "details": "Connection not initialized"})
    except Exception as e:
        services.append({"name": "DuckDB", "status": "unhealthy", "latency_ms": None, "details": str(e)})

    # Users DB
    users_db_path = DATA_DIR / "users.db"
    try:
        start = time.time()
        if users_db_path.exists():
            conn = sqlite3.connect(str(users_db_path))
            conn.execute("SELECT 1").fetchone()
            conn.close()
            latency = round((time.time() - start) * 1000, 1)
            services.append({"name": "Users DB", "status": "healthy", "latency_ms": latency, "details": None})
        else:
            services.append({"name": "Users DB", "status": "unknown", "latency_ms": None, "details": "Not configured"})
    except Exception as e:
        services.append({"name": "Users DB", "status": "unhealthy", "latency_ms": None, "details": str(e)})

    # Audit DB
    audit_db_path = DATA_DIR / "audit.db"
    try:
        start = time.time()
        if audit_db_path.exists():
            conn = sqlite3.connect(str(audit_db_path))
            conn.execute("SELECT 1").fetchone()
            conn.close()
            latency = round((time.time() - start) * 1000, 1)
            services.append({"name": "Audit DB", "status": "healthy", "latency_ms": latency, "details": None})
        else:
            services.append({"name": "Audit DB", "status": "unknown", "latency_ms": None, "details": "Not configured"})
    except Exception as e:
        services.append({"name": "Audit DB", "status": "unhealthy", "latency_ms": None, "details": str(e)})

    # Metadata Store
    metadata_path = KNOWLEDGE_DIR / "golden_metadata.json"
    services.append({
        "name": "Metadata Store",
        "status": "healthy" if metadata_path.exists() else "unknown",
        "latency_ms": None,
        "details": None if metadata_path.exists() else "Not configured"
    })

    return services


def _get_system_resources() -> Dict[str, Any]:
    """Get system resource usage."""
    # Disk usage
    disk = shutil.disk_usage(project_root)
    disk_total_gb = round(disk.total / (1024**3), 2)
    disk_used_gb = round(disk.used / (1024**3), 2)
    disk_percent = round((disk.used / disk.total) * 100, 1)

    # Memory and CPU (if psutil available)
    try:
        import psutil
        mem = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.1)
        memory_total_gb = round(mem.total / (1024**3), 2)
        memory_used_gb = round(mem.used / (1024**3), 2)
        memory_percent = mem.percent
        cpu_percent = cpu
    except ImportError:
        memory_total_gb = 0
        memory_used_gb = 0
        memory_percent = 0
        cpu_percent = 0

    return {
        "cpu_percent": cpu_percent,
        "memory_percent": memory_percent,
        "memory_used_gb": memory_used_gb,
        "memory_total_gb": memory_total_gb,
        "disk_percent": disk_percent,
        "disk_used_gb": disk_used_gb,
        "disk_total_gb": disk_total_gb
    }


# ============================================
# Dashboard Endpoints
# ============================================

@router.get("/stats")
async def get_dashboard_stats(current_user: dict = Depends(get_current_user)):
    """
    Get aggregated dashboard statistics.

    Returns comprehensive stats from all data sources.
    """
    return {
        "success": True,
        "data": {
            "queries": _get_audit_stats(),
            "users": _get_user_stats(),
            "data": _get_data_stats(),
            "metadata": _get_metadata_stats(),
            "cache": _get_cache_stats(),
            "llm": _get_llm_stats(),
            "services": _get_service_health(),
            "resources": _get_system_resources()
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/queries/recent")
async def get_recent_queries(
    limit: int = Query(default=10, ge=1, le=50),
    current_user: dict = Depends(get_current_user)
):
    """
    Get recent queries with details.
    """
    audit_db_path = DATA_DIR / "audit.db"

    if not audit_db_path.exists():
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    try:
        conn = sqlite3.connect(str(audit_db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                al.id,
                al.timestamp,
                al.user_id,
                al.details,
                al.status,
                qad.confidence_score,
                qad.execution_time_ms as exec_time,
                qad.original_question
            FROM audit_logs al
            LEFT JOIN query_audit_details qad ON qad.audit_log_id = al.id
            WHERE al.action IN ('QUERY', 'chat_query', 'query')
            ORDER BY al.timestamp DESC
            LIMIT ?
        """, (limit,))

        recent_queries = []
        for row in cursor.fetchall():
            try:
                details = json.loads(row["details"]) if row["details"] else {}
                # Prefer original_question from query_audit_details, fallback to details JSON
                question = row["original_question"] or details.get("question_preview", details.get("question", details.get("query", "")))
                recent_queries.append({
                    "id": row["id"],
                    "timestamp": row["timestamp"],
                    "relative": _relative_time(row["timestamp"]),
                    "username": row["user_id"] or "unknown",
                    "question": question[:150] if question else "",
                    "confidence": row["confidence_score"],  # From query_audit_details
                    "execution_time_ms": row["exec_time"],  # From query_audit_details
                    "status": row["status"] or "success"
                })
            except (json.JSONDecodeError, ValueError):
                pass

        conn.close()

        return {
            "success": True,
            "data": recent_queries,
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        return {
            "success": True,
            "data": [],
            "meta": {"timestamp": datetime.now().isoformat(), "error": str(e)}
        }


@router.get("/users/activity")
async def get_user_activity(current_user: dict = Depends(get_current_user)):
    """
    Get user activity summary.
    """
    return {
        "success": True,
        "data": _get_user_stats(),
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/services/health")
async def get_services_health(current_user: dict = Depends(get_current_user)):
    """
    Get detailed service health status.
    """
    return {
        "success": True,
        "data": {"services": _get_service_health()},
        "meta": {"timestamp": datetime.now().isoformat()}
    }
