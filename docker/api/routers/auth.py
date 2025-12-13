# SAGE API - Authentication Router
# =================================
"""Authentication endpoints for JWT-based auth."""

import os
import hashlib
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, status, Form
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

# JWT handling - using simple approach for now
import json
import base64
import hmac

router = APIRouter()
security = HTTPBearer(auto_error=False)

# Configuration
SECRET_KEY = os.getenv("JWT_SECRET", "sage-development-secret-key-change-in-production")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

# Simple user store (replace with database in production)
USERS = {
    os.getenv("ADMIN_USERNAME", "admin"): {
        "password_hash": hashlib.sha256(
            os.getenv("ADMIN_PASSWORD", "sage2024").encode()
        ).hexdigest(),
        "roles": ["admin"]
    }
}

# Token blacklist (in-memory, use Redis in production)
TOKEN_BLACKLIST = set()


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
async def token(username: str = Form(...), password: str = Form(...)):
    """
    OAuth2-compatible token endpoint (form-urlencoded).

    This is the primary endpoint for the React frontend.
    Accepts form data with username and password fields.
    """
    user = USERS.get(username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "AUTH_INVALID", "message": "Invalid username or password"}
        )

    password_hash = hashlib.sha256(password.encode()).hexdigest()
    if not hmac.compare_digest(user["password_hash"], password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "AUTH_INVALID", "message": "Invalid username or password"}
        )

    # Create tokens
    access_token = create_token(
        {"sub": username, "roles": user["roles"], "type": "access"},
        timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )

    refresh_token = create_token(
        {"sub": username, "type": "refresh"},
        timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    )

    return {
        "success": True,
        "data": {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            "refresh_token": refresh_token
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/login")
async def login(username: str, password: str):
    """
    Authenticate and get access token (query parameters).

    - **username**: User's username
    - **password**: User's password

    Returns access token and refresh token.

    Note: For form-urlencoded requests, use /token endpoint instead.
    """
    user = USERS.get(username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "AUTH_INVALID", "message": "Invalid username or password"}
        )

    password_hash = hashlib.sha256(password.encode()).hexdigest()
    if not hmac.compare_digest(user["password_hash"], password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "AUTH_INVALID", "message": "Invalid username or password"}
        )

    # Create tokens
    access_token = create_token(
        {"sub": username, "roles": user["roles"], "type": "access"},
        timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )

    refresh_token = create_token(
        {"sub": username, "type": "refresh"},
        timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    )

    return {
        "success": True,
        "data": {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            "refresh_token": refresh_token
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/logout")
async def logout(current_user: dict = Depends(get_current_user)):
    """
    Logout and invalidate current token.
    """
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
    user = USERS.get(username, {})

    return {
        "success": True,
        "data": {
            "username": username,
            "roles": user.get("roles", []),
            "last_login": datetime.now().isoformat()
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/refresh")
async def refresh_token(refresh_token: str):
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
    user = USERS.get(username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "AUTH_INVALID", "message": "User not found"}
        )

    # Create new access token
    access_token = create_token(
        {"sub": username, "roles": user["roles"], "type": "access"},
        timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )

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
    user = USERS.get(username)

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

    # Update password
    user["password_hash"] = hashlib.sha256(new_password.encode()).hexdigest()

    return {
        "success": True,
        "data": {"message": "Password changed successfully"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }
