"""
Audit Middleware
================

FastAPI middleware for logging all API requests to the audit trail.
"""

import os
import sys
import time
import json
import re
from pathlib import Path
from typing import Optional, Set
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from fastapi import HTTPException

# Add project root to path for imports
project_root = Path(os.environ.get('APP_ROOT', '/app'))
sys.path.insert(0, str(project_root))

try:
    from core.audit import get_audit_service, AuditAction, AuditStatus, AuditEvent
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False


class AuditMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log all API requests to the audit trail.

    Features:
    - Logs request method, path, status code, duration
    - Captures authenticated user from JWT token
    - Sanitizes sensitive fields from request body
    - Excludes configurable paths (health checks, docs)
    - Non-blocking async logging
    """

    # Paths to exclude from logging
    EXCLUDED_PATHS: Set[str] = {
        "/health",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/favicon.ico",
        "/metrics",
        "/api/v1/system/health",
        "/api/v1/auth/me",  # Session verification - login/logout already logged
    }

    # Path prefixes to exclude (avoids feedback loop and noise)
    EXCLUDED_PREFIXES: Set[str] = {
        "/docs",
        "/redoc",
        "/api/v1/audit",  # Don't log audit page requests (feedback loop)
    }

    # Sensitive fields to sanitize in request body
    SENSITIVE_FIELDS: Set[str] = {
        "password",
        "token",
        "secret",
        "api_key",
        "authorization",
        "access_token",
        "refresh_token",
    }

    # Maximum request body size to log (bytes)
    MAX_BODY_SIZE: int = int(os.getenv("AUDIT_MAX_REQUEST_BODY", "10000"))

    # Enable/disable API request logging
    ENABLED: bool = os.getenv("AUDIT_LOG_API_REQUESTS", "true").lower() == "true"

    def __init__(self, app, exclude_paths: Optional[Set[str]] = None):
        super().__init__(app)
        if exclude_paths:
            self.EXCLUDED_PATHS = self.EXCLUDED_PATHS.union(exclude_paths)

    async def dispatch(self, request: Request, call_next) -> Response:
        """Process the request and log to audit trail."""
        # Skip if audit not available or disabled
        if not AUDIT_AVAILABLE or not self.ENABLED:
            return await call_next(request)

        # Skip excluded paths
        path = request.url.path
        if self._should_skip(path):
            return await call_next(request)

        # Capture start time
        start_time = time.time()

        # Extract user info from request state or headers
        user_id, username = self._extract_user(request)

        # Get request body (for POST/PUT/PATCH)
        request_body = None
        if request.method in {"POST", "PUT", "PATCH"}:
            try:
                body = await request.body()
                if len(body) <= self.MAX_BODY_SIZE:
                    request_body = self._sanitize_body(body.decode('utf-8'))
            except Exception:
                pass

        # Process request
        error_message = None
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
        except HTTPException as e:
            status_code = e.status_code
            error_message = e.detail
            raise
        except Exception as e:
            error_message = str(e)
            raise
        finally:
            # Calculate duration
            duration_ms = int((time.time() - start_time) * 1000)

            # Log to audit trail asynchronously
            try:
                self._log_request(
                    user_id=user_id,
                    username=username,
                    method=request.method,
                    path=path,
                    status_code=status_code,
                    duration_ms=duration_ms,
                    ip_address=self._get_client_ip(request),
                    user_agent=request.headers.get("user-agent"),
                    request_body=request_body,
                    error_message=error_message,
                )
            except Exception:
                # Don't let audit logging failures break the request
                pass

        return response

    def _should_skip(self, path: str) -> bool:
        """Check if the path should be skipped."""
        if path in self.EXCLUDED_PATHS:
            return True

        for prefix in self.EXCLUDED_PREFIXES:
            if path.startswith(prefix):
                return True

        # Skip static files
        if path.endswith(('.js', '.css', '.png', '.jpg', '.ico', '.svg', '.woff', '.woff2')):
            return True

        return False

    def _extract_user(self, request: Request) -> tuple:
        """Extract user ID and username from request."""
        # Try to get from request state (set by auth middleware/dependency)
        if hasattr(request.state, 'user'):
            user = request.state.user
            return user.get('username', 'unknown'), user.get('username', 'Unknown')

        # Try to decode from Authorization header
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            try:
                token = auth_header[7:]
                # Handle custom JWT format (base64_payload.signature)
                parts = token.split(".")
                if len(parts) == 2:
                    import base64
                    payload_b64 = parts[0]
                    payload_json = base64.urlsafe_b64decode(payload_b64.encode()).decode()
                    payload = json.loads(payload_json)
                    username = payload.get("sub", "unknown")
                    return username, username
                # Handle standard JWT format (header.payload.signature)
                elif len(parts) == 3:
                    import base64
                    # Add padding if needed
                    payload_b64 = parts[1]
                    payload_b64 += "=" * (4 - len(payload_b64) % 4)
                    payload_json = base64.urlsafe_b64decode(payload_b64.encode()).decode()
                    payload = json.loads(payload_json)
                    username = payload.get("sub", "unknown")
                    return username, username
            except Exception:
                pass

        return "anonymous", "Anonymous"

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address."""
        # Check for forwarded headers (behind proxy)
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()

        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip

        # Fall back to client host
        if request.client:
            return request.client.host

        return "unknown"

    def _sanitize_body(self, body: str) -> str:
        """Sanitize sensitive fields from request body."""
        try:
            data = json.loads(body)
            data = self._sanitize_dict(data)
            return json.dumps(data)
        except json.JSONDecodeError:
            # Not JSON, return as-is but truncate
            return body[:self.MAX_BODY_SIZE]

    def _sanitize_dict(self, data: dict) -> dict:
        """Recursively sanitize dictionary."""
        if not isinstance(data, dict):
            return data

        sanitized = {}
        for key, value in data.items():
            if key.lower() in self.SENSITIVE_FIELDS:
                sanitized[key] = "[REDACTED]"
            elif isinstance(value, dict):
                sanitized[key] = self._sanitize_dict(value)
            elif isinstance(value, list):
                sanitized[key] = [
                    self._sanitize_dict(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                sanitized[key] = value

        return sanitized

    def _log_request(
        self,
        user_id: str,
        username: str,
        method: str,
        path: str,
        status_code: int,
        duration_ms: int,
        ip_address: str,
        user_agent: Optional[str],
        request_body: Optional[str],
        error_message: Optional[str],
    ) -> None:
        """Log the request to audit service."""
        audit_service = get_audit_service()
        audit_service.log_api_request(
            user_id=user_id,
            username=username,
            method=method,
            path=path,
            status_code=status_code,
            duration_ms=duration_ms,
            ip_address=ip_address,
            user_agent=user_agent,
            request_body=request_body,
            error_message=error_message,
        )
