# SAGE API - Authentication Router
# =================================
"""Authentication endpoints for JWT-based auth."""

import os
import sys
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, status, Form, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

# JWT handling - using simple approach for now
import json
import base64
import hmac

# Add project root to path for imports
project_root = Path(os.environ.get('APP_ROOT', '/app'))
sys.path.insert(0, str(project_root))

# Import audit service
try:
    from core.audit import get_audit_service
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False

# Import user service
try:
    from core.users import get_user_service
    USERS_AVAILABLE = True
except ImportError:
    USERS_AVAILABLE = False

router = APIRouter()
security = HTTPBearer(auto_error=False)


def _get_client_ip(request: Request) -> str:
    """Extract client IP from request."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip
    if request.client:
        return request.client.host
    return "unknown"


def _log_auth_event(
    username: str,
    success: bool,
    request: Request,
    failure_reason: str = None,
    action: str = "login"
):
    """Log authentication event to audit trail."""
    if not AUDIT_AVAILABLE:
        return
    try:
        audit_service = get_audit_service()
        ip_address = _get_client_ip(request)
        user_agent = request.headers.get("user-agent")

        if action == "login":
            audit_service.log_login(
                user_id=username,
                username=username,
                ip_address=ip_address,
                user_agent=user_agent,
                success=success,
                failure_reason=failure_reason,
            )
        elif action == "logout":
            audit_service.log_logout(
                user_id=username,
                username=username,
                ip_address=ip_address,
            )
        elif action == "token_refresh":
            audit_service.log_token_refresh(
                user_id=username,
                username=username,
                ip_address=ip_address,
            )
    except Exception as e:
        # Don't let audit failures break auth
        print(f"Warning: Failed to log auth event: {e}")

# Configuration
SECRET_KEY = os.getenv("JWT_SECRET", "sage-development-secret-key-change-in-production")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

# Fallback user store (used only if database is not available)
FALLBACK_USERS = {
    os.getenv("ADMIN_USERNAME", "admin"): {
        "password_hash": hashlib.sha256(
            os.getenv("ADMIN_PASSWORD", "sage2024").encode()
        ).hexdigest(),
        "roles": ["admin"]
    }
}

# Token blacklist (in-memory fallback, database used when available)
TOKEN_BLACKLIST = set()


def _get_user_from_db(username: str):
    """Get user from database, returns None if not found or DB unavailable."""
    if not USERS_AVAILABLE:
        return None
    try:
        user_service = get_user_service()
        return user_service.get_user_by_username(username)
    except Exception:
        return None


def _authenticate_user(username: str, password: str):
    """
    Authenticate user against database or fallback.
    Returns (user_dict, error_message) tuple.
    """
    if USERS_AVAILABLE:
        try:
            user_service = get_user_service()
            user, error = user_service.authenticate(username, password)
            if user:
                return {
                    "username": user.username,
                    "roles": [user.role] if user.role == "admin" else [user.role],
                    "permissions": user.permissions,
                    "must_change_password": user.must_change_password,
                    "id": user.id,
                }, None
            return None, error
        except Exception as e:
            # Fall back to hardcoded user on DB error
            print(f"Warning: User DB error, using fallback: {e}")

    # Fallback to hardcoded user
    user = FALLBACK_USERS.get(username)
    if not user:
        return None, "Invalid username or password"

    password_hash = hashlib.sha256(password.encode()).hexdigest()
    if not hmac.compare_digest(user["password_hash"], password_hash):
        return None, "Invalid username or password"

    return {
        "username": username,
        "roles": user["roles"],
        "permissions": ["*"],
        "must_change_password": False,
    }, None


def create_token(data: dict, expires_delta: timedelta) -> str:
    """Create a simple JWT-like token."""
    payload = {
        **data,
        "exp": (datetime.utcnow() + expires_delta).isoformat(),
        "iat": datetime.utcnow().isoformat()
    }
    payload_json = json.dumps(payload, sort_keys=True)
    payload_b64 = base64.urlsafe_b64encode(payload_json.encode()).decode()

    signature = hmac.new(
        SECRET_KEY.encode(),
        payload_b64.encode(),
        hashlib.sha256
    ).hexdigest()

    return f"{payload_b64}.{signature}"


def decode_token(token: str) -> Optional[dict]:
    """Decode and verify a token."""
    try:
        parts = token.split(".")
        if len(parts) != 2:
            return None

        payload_b64, signature = parts

        # Verify signature
        expected_sig = hmac.new(
            SECRET_KEY.encode(),
            payload_b64.encode(),
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(signature, expected_sig):
            return None

        # Decode payload
        payload_json = base64.urlsafe_b64decode(payload_b64.encode()).decode()
        payload = json.loads(payload_json)

        # Check expiration
        exp = datetime.fromisoformat(payload["exp"])
        if datetime.utcnow() > exp:
            return None

        # Check blacklist
        if token in TOKEN_BLACKLIST:
            return None

        return payload
    except Exception:
        return None


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> dict:
    """Dependency to get the current authenticated user."""
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "AUTH_REQUIRED", "message": "Authentication required"},
            headers={"WWW-Authenticate": "Bearer"}
        )

    payload = decode_token(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "AUTH_INVALID", "message": "Invalid or expired token"},
            headers={"WWW-Authenticate": "Bearer"}
        )

    return payload


async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> Optional[dict]:
    """Dependency to get user if authenticated, None otherwise."""
    if not credentials:
        return None

    return decode_token(credentials.credentials)


# ============================================
# Endpoints
# ============================================

@router.post("/token")
async def token(request: Request, username: str = Form(...), password: str = Form(...)):
    """
    OAuth2-compatible token endpoint (form-urlencoded).

    This is the primary endpoint for the React frontend.
    Accepts form data with username and password fields.
    """
    user_data, error = _authenticate_user(username, password)

    if not user_data:
        _log_auth_event(username, False, request, error)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "AUTH_INVALID", "message": error or "Invalid username or password"}
        )

    # Create tokens (include permissions for access control)
    access_token = create_token(
        {
            "sub": username,
            "roles": user_data["roles"],
            "permissions": user_data.get("permissions", []),
            "type": "access"
        },
        timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )

    refresh_token = create_token(
        {"sub": username, "type": "refresh"},
        timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    )

    # Create session in database if available
    if USERS_AVAILABLE and user_data.get("id"):
        try:
            user_service = get_user_service()
            user_service.create_session(
                user_id=user_data["id"],
                token=access_token,
                expires_in_minutes=ACCESS_TOKEN_EXPIRE_MINUTES,
                ip_address=_get_client_ip(request),
                user_agent=request.headers.get("user-agent"),
            )
        except Exception as e:
            print(f"Warning: Failed to create session: {e}")

    # Log successful login
    _log_auth_event(username, True, request)

    return {
        "success": True,
        "data": {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            "refresh_token": refresh_token,
            "must_change_password": user_data.get("must_change_password", False)
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/login")
async def login(request: Request, username: str, password: str):
    """
    Authenticate and get access token (query parameters).

    - **username**: User's username
    - **password**: User's password

    Returns access token and refresh token.

    Note: For form-urlencoded requests, use /token endpoint instead.
    """
    user_data, error = _authenticate_user(username, password)

    if not user_data:
        _log_auth_event(username, False, request, error)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "AUTH_INVALID", "message": error or "Invalid username or password"}
        )

    # Create tokens (include permissions for access control)
    access_token = create_token(
        {
            "sub": username,
            "roles": user_data["roles"],
            "permissions": user_data.get("permissions", []),
            "type": "access"
        },
        timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )

    refresh_token = create_token(
        {"sub": username, "type": "refresh"},
        timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    )

    # Create session in database if available
    if USERS_AVAILABLE and user_data.get("id"):
        try:
            user_service = get_user_service()
            user_service.create_session(
                user_id=user_data["id"],
                token=access_token,
                expires_in_minutes=ACCESS_TOKEN_EXPIRE_MINUTES,
                ip_address=_get_client_ip(request),
                user_agent=request.headers.get("user-agent"),
            )
        except Exception as e:
            print(f"Warning: Failed to create session: {e}")

    # Log successful login
    _log_auth_event(username, True, request)

    return {
        "success": True,
        "data": {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            "refresh_token": refresh_token,
            "must_change_password": user_data.get("must_change_password", False)
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/logout")
async def logout(request: Request, current_user: dict = Depends(get_current_user)):
    """
    Logout and invalidate current token.
    """
    username = current_user.get("sub", "unknown")

    # Log logout event
    _log_auth_event(username, True, request, action="logout")

    # In a real implementation, we'd add the token to a blacklist
    return {
        "success": True,
        "data": {"message": "Successfully logged out"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/me")
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """
    Get current user information.
    """
    username = current_user.get("sub")

    # Try to get from database first
    db_user = _get_user_from_db(username)
    if db_user:
        return {
            "success": True,
            "data": {
                "id": db_user.id,
                "username": db_user.username,
                "email": db_user.email,
                "roles": [db_user.role],
                "permissions": db_user.permissions,
                "last_login": db_user.last_login.isoformat() if db_user.last_login else None,
                "must_change_password": db_user.must_change_password,
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    # Fallback to token data
    return {
        "success": True,
        "data": {
            "username": username,
            "roles": current_user.get("roles", []),
            "permissions": ["*"],
            "last_login": datetime.now().isoformat()
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/refresh")
async def refresh_token_endpoint(request: Request, refresh_token: str):
    """
    Refresh access token using refresh token.
    """
    payload = decode_token(refresh_token)
    if not payload or payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "AUTH_EXPIRED", "message": "Invalid or expired refresh token"}
        )

    username = payload.get("sub")

    # Try to get user from database first
    db_user = _get_user_from_db(username)
    if db_user:
        roles = [db_user.role]
        permissions = db_user.permissions
        user_id = db_user.id
        # Check if user is still active
        if not db_user.is_active:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"code": "AUTH_INVALID", "message": "User account is inactive"}
            )
    else:
        # Fallback to hardcoded user
        user = FALLBACK_USERS.get(username)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"code": "AUTH_INVALID", "message": "User not found"}
            )
        roles = user["roles"]
        permissions = ["*"]
        user_id = None

    # Create new access token (include permissions for access control)
    access_token = create_token(
        {
            "sub": username,
            "roles": roles,
            "permissions": permissions,
            "type": "access"
        },
        timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )

    # Create session in database if available
    if USERS_AVAILABLE and user_id:
        try:
            user_service = get_user_service()
            user_service.create_session(
                user_id=user_id,
                token=access_token,
                expires_in_minutes=ACCESS_TOKEN_EXPIRE_MINUTES,
                ip_address=_get_client_ip(request),
                user_agent=request.headers.get("user-agent"),
            )
        except Exception as e:
            print(f"Warning: Failed to create session: {e}")

    # Log token refresh
    _log_auth_event(username, True, request, action="token_refresh")

    return {
        "success": True,
        "data": {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.put("/password")
async def change_password(
    old_password: str,
    new_password: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Change user password.
    """
    username = current_user.get("sub")

    # Try database first
    if USERS_AVAILABLE:
        try:
            user_service = get_user_service()
            db_user = user_service.get_user_by_username(username)
            if db_user:
                success, error = user_service.change_password(
                    db_user.id, old_password, new_password
                )
                if not success:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={"code": "PASSWORD_CHANGE_FAILED", "message": error}
                    )
                return {
                    "success": True,
                    "data": {"message": "Password changed successfully"},
                    "meta": {"timestamp": datetime.now().isoformat()}
                }
        except HTTPException:
            raise
        except Exception as e:
            print(f"Warning: User DB error in password change: {e}")

    # Fallback to hardcoded user
    user = FALLBACK_USERS.get(username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "NOT_FOUND", "message": "User not found"}
        )

    # Verify old password
    old_hash = hashlib.sha256(old_password.encode()).hexdigest()
    if not hmac.compare_digest(user["password_hash"], old_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "AUTH_INVALID", "message": "Current password is incorrect"}
        )

    # Update password (in memory only for fallback)
    user["password_hash"] = hashlib.sha256(new_password.encode()).hexdigest()

    return {
        "success": True,
        "data": {"message": "Password changed successfully"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }
