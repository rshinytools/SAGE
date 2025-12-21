# SAGE API Test Suite
# ====================
"""Comprehensive tests for the SAGE FastAPI service."""

import pytest
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import json
import base64
import hmac
import hashlib

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fastapi.testclient import TestClient

# Import the FastAPI app
from docker.api.main import app

# Create test client
client = TestClient(app)


# ============================================
# Test Fixtures and Helpers
# ============================================

def get_test_token(username: str = "admin", roles: list = None) -> str:
    """Generate a test token for authentication."""
    if roles is None:
        roles = ["admin"]

    secret = os.getenv("JWT_SECRET", "sage-development-secret-key-change-in-production")

    payload = {
        "sub": username,
        "roles": roles,
        "type": "access",
        "exp": (datetime.utcnow() + timedelta(hours=1)).isoformat(),
        "iat": datetime.utcnow().isoformat()
    }

    payload_json = json.dumps(payload, sort_keys=True)
    payload_b64 = base64.urlsafe_b64encode(payload_json.encode()).decode()

    signature = hmac.new(
        secret.encode(),
        payload_b64.encode(),
        hashlib.sha256
    ).hexdigest()

    return f"{payload_b64}.{signature}"


def auth_headers(username: str = "admin", roles: list = None) -> dict:
    """Get authorization headers with valid token."""
    token = get_test_token(username, roles)
    return {"Authorization": f"Bearer {token}"}


# ============================================
# Root Endpoint Tests
# ============================================

class TestRootEndpoints:
    """Test root-level API endpoints."""

    def test_root_endpoint(self):
        """Test the root endpoint returns welcome message."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "SAGE API" in data.get("service", "") or "SAGE" in str(data)

    def test_api_version_endpoint(self):
        """Test API version endpoint."""
        response = client.get("/api/v1")
        assert response.status_code == 200
        data = response.json()
        # The API returns version directly, not wrapped
        assert "version" in data or "endpoints" in data


# ============================================
# Authentication Tests
# ============================================

class TestAuthentication:
    """Test authentication endpoints."""

    def test_login_missing_credentials(self):
        """Test login fails without credentials."""
        response = client.post("/api/v1/auth/login")
        assert response.status_code == 422  # Validation error

    def test_login_invalid_credentials(self):
        """Test login fails with invalid credentials."""
        response = client.post(
            "/api/v1/auth/login",
            params={"username": "invalid", "password": "invalid"}
        )
        assert response.status_code == 401
        data = response.json()
        assert data["detail"]["code"] == "AUTH_INVALID"

    def test_login_success(self):
        """Test successful login."""
        username = os.getenv("ADMIN_USERNAME", "admin")
        password = os.getenv("ADMIN_PASSWORD", "sage2024!")

        response = client.post(
            "/api/v1/auth/login",
            params={"username": username, "password": password}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "access_token" in data["data"]
        assert "refresh_token" in data["data"]
        assert data["data"]["token_type"] == "bearer"

    def test_me_without_auth(self):
        """Test /me endpoint requires authentication."""
        response = client.get("/api/v1/auth/me")
        assert response.status_code == 401

    def test_me_with_auth(self):
        """Test /me endpoint with valid token."""
        response = client.get("/api/v1/auth/me", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "username" in data["data"]

    def test_logout(self):
        """Test logout endpoint."""
        response = client.post("/api/v1/auth/logout", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    def test_refresh_token_invalid(self):
        """Test refresh with invalid token."""
        response = client.post(
            "/api/v1/auth/refresh",
            params={"refresh_token": "invalid-token"}
        )
        assert response.status_code == 401

    def test_password_change_wrong_current(self):
        """Test password change fails with wrong current password."""
        response = client.put(
            "/api/v1/auth/password",
            params={"old_password": "wrong", "new_password": "newpass123"},
            headers=auth_headers()
        )
        assert response.status_code == 401


# ============================================
# Data Factory Tests
# ============================================

class TestDataFactory:
    """Test Data Factory API endpoints."""

    def test_list_files_requires_auth(self):
        """Test file listing requires authentication."""
        response = client.get("/api/v1/data/files")
        assert response.status_code == 401

    def test_list_files_success(self):
        """Test listing files with authentication."""
        response = client.get("/api/v1/data/files", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "data" in data
        assert isinstance(data["data"], list)

    def test_list_tables_success(self):
        """Test listing database tables."""
        response = client.get("/api/v1/data/tables", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)

    def test_process_files_endpoint(self):
        """Test file processing endpoint."""
        response = client.post(
            "/api/v1/data/process",
            headers=auth_headers()
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "job_id" in data["data"]

    def test_query_only_select(self):
        """Test that only SELECT queries are allowed."""
        response = client.post(
            "/api/v1/data/query",
            params={"sql": "DROP TABLE users"},
            headers=auth_headers()
        )
        assert response.status_code == 422
        data = response.json()
        assert data["detail"]["code"] == "VALIDATION_ERROR"

    def test_query_blocks_dangerous_keywords(self):
        """Test that dangerous SQL keywords are blocked."""
        dangerous_queries = [
            "SELECT * FROM users; DELETE FROM users",
            "SELECT * FROM users WHERE 1=1; DROP TABLE users",
            "SELECT * FROM (INSERT INTO users VALUES (1))",
        ]

        for query in dangerous_queries:
            response = client.post(
                "/api/v1/data/query",
                params={"sql": query},
                headers=auth_headers()
            )
            assert response.status_code == 422

    def test_validation_report(self):
        """Test validation report endpoint."""
        response = client.get(
            "/api/v1/data/validation/test_table",
            headers=auth_headers()
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "quality_score" in data["data"]

    def test_run_validation(self):
        """Test running validation."""
        response = client.post(
            "/api/v1/data/validate",
            params={"tables": ["dm", "ae"]},
            headers=auth_headers()
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    def test_drop_table_requires_admin(self):
        """Test table drop requires admin role."""
        # Test with non-admin user
        response = client.delete(
            "/api/v1/data/tables/test_table",
            headers=auth_headers(roles=["user"])
        )
        assert response.status_code == 403

        # Test with admin user
        response = client.delete(
            "/api/v1/data/tables/test_table",
            headers=auth_headers(roles=["admin"])
        )
        assert response.status_code == 200


# ============================================
# Metadata Factory Tests
# ============================================

class TestMetadataFactory:
    """Test Metadata Factory API endpoints."""

    def test_list_domains(self):
        """Test listing domains."""
        response = client.get("/api/v1/metadata/domains", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)

    def test_get_domain_not_found(self):
        """Test getting non-existent domain."""
        response = client.get(
            "/api/v1/metadata/domains/NONEXISTENT",
            headers=auth_headers()
        )
        # Should either return 404 or empty data depending on implementation
        assert response.status_code in [200, 404]

    def test_metadata_stats(self):
        """Test metadata statistics endpoint."""
        response = client.get("/api/v1/metadata/stats", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "total_domains" in data["data"]
        assert "total_variables" in data["data"]

    def test_metadata_search(self):
        """Test metadata search endpoint."""
        response = client.get(
            "/api/v1/metadata/search",
            params={"q": "subject"},  # API uses 'q' parameter
            headers=auth_headers()
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)

    def test_list_codelists(self):
        """Test listing codelists."""
        response = client.get("/api/v1/metadata/codelists", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)

    def test_pending_items(self):
        """Test getting pending approval items."""
        response = client.get("/api/v1/metadata/pending", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    def test_version_history(self):
        """Test version history endpoint."""
        response = client.get("/api/v1/metadata/versions", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True


# ============================================
# Project Tracker Tests
# ============================================

class TestProjectTracker:
    """Test Project Tracker API endpoints."""

    def test_tracker_summary(self):
        """Test tracker summary endpoint."""
        response = client.get("/api/v1/tracker/summary", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        # Summary should have progress fields
        assert "data" in data

    def test_next_steps(self):
        """Test next steps endpoint."""
        response = client.get("/api/v1/tracker/next-steps", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)

    def test_list_phases(self):
        """Test listing phases."""
        response = client.get("/api/v1/tracker/phases", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)

    def test_list_tasks(self):
        """Test listing tasks."""
        response = client.get("/api/v1/tracker/tasks", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)

    def test_list_tasks_with_filters(self):
        """Test listing tasks with filters."""
        response = client.get(
            "/api/v1/tracker/tasks",
            params={"status": "pending", "priority": "high"},
            headers=auth_headers()
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    def test_activity_log(self):
        """Test activity log endpoint."""
        response = client.get("/api/v1/tracker/activity", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)

    def test_log_activity(self):
        """Test logging custom activity."""
        response = client.post(
            "/api/v1/tracker/activity",
            params={"action": "test_action", "details": "Test activity"},
            headers=auth_headers()
        )
        # Should succeed or fail gracefully if tracker DB doesn't exist
        assert response.status_code in [200, 404]


# ============================================
# System Tests
# ============================================

class TestSystem:
    """Test System API endpoints."""

    def test_health_no_auth_required(self):
        """Test health check doesn't require authentication."""
        response = client.get("/api/v1/system/health")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["status"] == "healthy"

    def test_detailed_health_requires_auth(self):
        """Test detailed health requires authentication."""
        response = client.get("/api/v1/system/health/detailed")
        assert response.status_code == 401

    def test_detailed_health_with_auth(self):
        """Test detailed health with authentication."""
        response = client.get(
            "/api/v1/system/health/detailed",
            headers=auth_headers()
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "services" in data["data"]
        assert "uptime" in data["data"]

    def test_system_info(self):
        """Test system info endpoint."""
        response = client.get("/api/v1/system/info", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "version" in data["data"]
        assert "uptime" in data["data"]
        assert "python_version" in data["data"]
        assert "disk_usage" in data["data"]

    def test_config_requires_admin(self):
        """Test config endpoint requires admin role."""
        response = client.get(
            "/api/v1/system/config",
            headers=auth_headers(roles=["user"])
        )
        assert response.status_code == 403

    def test_config_with_admin(self):
        """Test config endpoint with admin role."""
        response = client.get(
            "/api/v1/system/config",
            headers=auth_headers(roles=["admin"])
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "data_dir" in data["data"]

    def test_list_services(self):
        """Test listing services."""
        response = client.get("/api/v1/system/services", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)
        assert len(data["data"]) > 0
        # API service should always be running
        api_service = next((s for s in data["data"] if s["name"] == "api"), None)
        assert api_service is not None
        assert api_service["status"] == "running"

    def test_restart_service_requires_admin(self):
        """Test service restart requires admin role."""
        response = client.post(
            "/api/v1/system/services/admin-ui/restart",
            headers=auth_headers(roles=["user"])
        )
        assert response.status_code == 403

    def test_list_backups(self):
        """Test listing backups."""
        response = client.get("/api/v1/system/backups", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)

    def test_create_backup_requires_admin(self):
        """Test backup creation requires admin role."""
        response = client.post(
            "/api/v1/system/backups",
            headers=auth_headers(roles=["user"])
        )
        assert response.status_code == 403

    def test_get_logs(self):
        """Test getting logs."""
        response = client.get("/api/v1/system/logs", headers=auth_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert isinstance(data["data"], list)

    def test_audit_logs_requires_admin(self):
        """Test audit logs require admin role."""
        response = client.get(
            "/api/v1/system/logs/audit",
            headers=auth_headers(roles=["user"])
        )
        assert response.status_code == 403


# ============================================
# Response Format Tests
# ============================================

class TestResponseFormat:
    """Test that all responses follow the standard format."""

    def test_success_response_format(self):
        """Test successful responses have correct format."""
        response = client.get("/api/v1/system/health")
        data = response.json()

        assert "success" in data
        assert "data" in data
        assert "meta" in data
        assert "timestamp" in data["meta"]

    def test_error_response_format(self):
        """Test error responses have correct format."""
        response = client.get("/api/v1/auth/me")  # No auth
        data = response.json()

        assert "detail" in data
        assert "code" in data["detail"]
        assert "message" in data["detail"]


# ============================================
# Security Tests
# ============================================

class TestSecurity:
    """Test security features."""

    def test_expired_token_rejected(self):
        """Test that expired tokens are rejected."""
        secret = os.getenv("JWT_SECRET", "sage-development-secret-key-change-in-production")

        # Create an expired token
        payload = {
            "sub": "admin",
            "roles": ["admin"],
            "type": "access",
            "exp": (datetime.utcnow() - timedelta(hours=1)).isoformat(),  # Expired
            "iat": datetime.utcnow().isoformat()
        }

        payload_json = json.dumps(payload, sort_keys=True)
        payload_b64 = base64.urlsafe_b64encode(payload_json.encode()).decode()

        signature = hmac.new(
            secret.encode(),
            payload_b64.encode(),
            hashlib.sha256
        ).hexdigest()

        expired_token = f"{payload_b64}.{signature}"

        response = client.get(
            "/api/v1/auth/me",
            headers={"Authorization": f"Bearer {expired_token}"}
        )
        assert response.status_code == 401

    def test_invalid_signature_rejected(self):
        """Test that tokens with invalid signatures are rejected."""
        token = get_test_token()
        # Tamper with the signature
        parts = token.split(".")
        parts[1] = "tampered_signature"
        tampered_token = ".".join(parts)

        response = client.get(
            "/api/v1/auth/me",
            headers={"Authorization": f"Bearer {tampered_token}"}
        )
        assert response.status_code == 401

    def test_role_based_access_control(self):
        """Test role-based access control."""
        # Admin endpoint with user role should fail
        response = client.get(
            "/api/v1/system/config",
            headers=auth_headers(roles=["user"])
        )
        assert response.status_code == 403

        # Admin endpoint with admin role should succeed
        response = client.get(
            "/api/v1/system/config",
            headers=auth_headers(roles=["admin"])
        )
        assert response.status_code == 200


# ============================================
# Pagination Tests
# ============================================

class TestPagination:
    """Test pagination parameters."""

    def test_limit_parameter(self):
        """Test limit parameter is respected."""
        response = client.get(
            "/api/v1/tracker/activity",
            params={"limit": 5},
            headers=auth_headers()
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) <= 5

    def test_invalid_limit_rejected(self):
        """Test invalid limit values are rejected."""
        # Limit too high
        response = client.get(
            "/api/v1/tracker/activity",
            params={"limit": 10000},
            headers=auth_headers()
        )
        assert response.status_code == 422


# ============================================
# Run Tests
# ============================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
