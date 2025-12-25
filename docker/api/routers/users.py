# SAGE API - User Management Router
# ==================================
"""User management endpoints for admin users."""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends, Query, status, Request
from pydantic import BaseModel, EmailStr

# Add project root to path for imports
project_root = Path(os.environ.get('APP_ROOT', '/app'))
sys.path.insert(0, str(project_root))

# Import user service
try:
    from core.users import (
        get_user_service,
        CreateUserRequest,
        UpdateUserRequest,
        UserResponse,
        PaginatedUsersResponse,
        StatusToggleRequest,
        AVAILABLE_PERMISSIONS,
    )
    USERS_AVAILABLE = True
except ImportError as e:
    print(f"Warning: User module not available: {e}")
    USERS_AVAILABLE = False

# Import auth dependencies
from .auth import get_current_user

# Import audit service
try:
    from core.audit import get_audit_service, AuditAction, AuditStatus
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False

router = APIRouter()


# ==================== DEPENDENCIES ====================

async def require_user_admin(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Dependency to require user_admin or full admin (*) permission.

    This allows access to user management features for:
    - Users with "*" permission (full admin)
    - Users with "user_admin" permission

    Raises:
        HTTPException: If user doesn't have required permission
    """
    permissions = current_user.get("permissions", [])

    # Check for full admin (*) or user_admin permission
    if "*" in permissions or "user_admin" in permissions:
        return current_user

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail={"code": "FORBIDDEN", "message": "User management access required"}
    )


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


def _log_user_event(
    action: str,
    username: str,
    target_user: str,
    request: Request,
    success: bool = True,
    error_message: Optional[str] = None,
):
    """Log user management event to audit trail."""
    if not AUDIT_AVAILABLE:
        return
    try:
        from core.audit import get_audit_service, AuditAction, AuditStatus, AuditEvent

        audit_service = get_audit_service()
        event = AuditEvent(
            user_id=username,
            username=username,
            action=action,
            resource_type="user",
            resource_id=target_user,
            status=AuditStatus.SUCCESS if success else AuditStatus.FAILURE,
            ip_address=_get_client_ip(request),
            user_agent=request.headers.get("user-agent"),
            error_message=error_message,
        )
        audit_service.log_event(event)
    except Exception as e:
        print(f"Warning: Failed to log user event: {e}")


# ==================== RESPONSE MODELS ====================

class UserAccountResponse(BaseModel):
    """User account response matching frontend expectations."""
    id: str
    username: str
    email: str
    role: str
    permissions: List[str]
    created_at: datetime
    last_login: Optional[datetime] = None
    is_active: bool


class PaginatedUsersApiResponse(BaseModel):
    """Paginated users response matching frontend expectations."""
    items: List[UserAccountResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class CreateUserApiRequest(BaseModel):
    """Create user request matching frontend."""
    username: str
    email: EmailStr
    password: str
    role: str = "user"
    permissions: List[str] = []


class UpdateUserApiRequest(BaseModel):
    """Update user request matching frontend."""
    email: Optional[EmailStr] = None
    role: Optional[str] = None
    permissions: Optional[List[str]] = None
    is_active: Optional[bool] = None


class ResetPasswordResponse(BaseModel):
    """Reset password response."""
    temporary_password: str
    message: str = "Password has been reset. User must change password on next login."


# ==================== ENDPOINTS ====================

@router.get("/permissions", response_model=List[str])
async def get_available_permissions(
    current_user: dict = Depends(require_user_admin)
):
    """
    Get list of available permissions.

    Returns list of permission strings that can be assigned to users.
    """
    if not USERS_AVAILABLE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "SERVICE_UNAVAILABLE", "message": "User service not available"}
        )

    user_service = get_user_service()
    return user_service.get_available_permissions()


@router.get("", response_model=PaginatedUsersApiResponse)
async def list_users(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    current_user: dict = Depends(require_user_admin)
):
    """
    List all users with pagination.

    - **page**: Page number (default: 1)
    - **page_size**: Items per page (default: 50, max: 100)

    Requires admin role.
    """
    if not USERS_AVAILABLE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "SERVICE_UNAVAILABLE", "message": "User service not available"}
        )

    user_service = get_user_service()
    result = user_service.list_users(page, page_size, include_inactive=True)

    # Convert to API response format
    items = [
        UserAccountResponse(
            id=u.id,
            username=u.username,
            email=u.email,
            role=u.role,
            permissions=u.permissions,
            created_at=u.created_at,
            last_login=u.last_login,
            is_active=u.is_active,
        )
        for u in result.users
    ]

    return PaginatedUsersApiResponse(
        items=items,
        total=result.total,
        page=result.page,
        page_size=result.page_size,
        total_pages=result.total_pages,
    )


@router.get("/{user_id}", response_model=UserAccountResponse)
async def get_user(
    user_id: str,
    current_user: dict = Depends(require_user_admin)
):
    """
    Get a user by ID.

    Requires admin role.
    """
    if not USERS_AVAILABLE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "SERVICE_UNAVAILABLE", "message": "User service not available"}
        )

    user_service = get_user_service()
    user = user_service.get_user(user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "NOT_FOUND", "message": "User not found"}
        )

    return UserAccountResponse(
        id=user.id,
        username=user.username,
        email=user.email,
        role=user.role,
        permissions=user.permissions,
        created_at=user.created_at,
        last_login=user.last_login,
        is_active=user.is_active,
    )


@router.post("", response_model=UserAccountResponse, status_code=status.HTTP_201_CREATED)
async def create_user(
    request_data: CreateUserApiRequest,
    request: Request,
    current_user: dict = Depends(require_user_admin)
):
    """
    Create a new user.

    - **username**: Unique username (3-50 characters)
    - **email**: Unique email address
    - **password**: Password (min 8 characters, must include upper, lower, digit, special)
    - **role**: User role (admin, user, viewer)
    - **permissions**: List of permission strings

    Requires admin role.
    """
    if not USERS_AVAILABLE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "SERVICE_UNAVAILABLE", "message": "User service not available"}
        )

    user_service = get_user_service()
    admin_username = current_user.get("sub", "unknown")

    try:
        # Convert to service request
        create_request = CreateUserRequest(
            username=request_data.username,
            email=request_data.email,
            password=request_data.password,
            role=request_data.role,
            permissions=request_data.permissions,
        )

        user = user_service.create_user(create_request)

        _log_user_event("USER_CREATE", admin_username, request_data.username, request)

        return UserAccountResponse(
            id=user.id,
            username=user.username,
            email=user.email,
            role=user.role,
            permissions=user.permissions,
            created_at=user.created_at,
            last_login=user.last_login,
            is_active=user.is_active,
        )

    except ValueError as e:
        _log_user_event(
            "USER_CREATE",
            admin_username,
            request_data.username,
            request,
            success=False,
            error_message=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "VALIDATION_ERROR", "message": str(e)}
        )


@router.put("/{user_id}", response_model=UserAccountResponse)
async def update_user(
    user_id: str,
    request_data: UpdateUserApiRequest,
    request: Request,
    current_user: dict = Depends(require_user_admin)
):
    """
    Update a user.

    - **email**: New email (optional)
    - **role**: New role (optional)
    - **permissions**: New permissions list (optional)
    - **is_active**: New active status (optional)

    Requires admin role.
    """
    if not USERS_AVAILABLE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "SERVICE_UNAVAILABLE", "message": "User service not available"}
        )

    user_service = get_user_service()
    admin_username = current_user.get("sub", "unknown")

    # Get current user info for logging
    existing_user = user_service.get_user(user_id)
    if not existing_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "NOT_FOUND", "message": "User not found"}
        )

    try:
        update_request = UpdateUserRequest(
            email=request_data.email,
            role=request_data.role,
            permissions=request_data.permissions,
            is_active=request_data.is_active,
        )

        user = user_service.update_user(user_id, update_request)

        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"code": "NOT_FOUND", "message": "User not found"}
            )

        _log_user_event("USER_UPDATE", admin_username, existing_user.username, request)

        return UserAccountResponse(
            id=user.id,
            username=user.username,
            email=user.email,
            role=user.role,
            permissions=user.permissions,
            created_at=user.created_at,
            last_login=user.last_login,
            is_active=user.is_active,
        )

    except ValueError as e:
        _log_user_event(
            "USER_UPDATE",
            admin_username,
            existing_user.username,
            request,
            success=False,
            error_message=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "VALIDATION_ERROR", "message": str(e)}
        )


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: str,
    request: Request,
    current_user: dict = Depends(require_user_admin)
):
    """
    Delete a user.

    Cannot delete your own account.

    Requires admin role.
    """
    if not USERS_AVAILABLE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "SERVICE_UNAVAILABLE", "message": "User service not available"}
        )

    user_service = get_user_service()
    admin_username = current_user.get("sub", "unknown")

    # Get user to check if it's self-deletion
    user = user_service.get_user(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "NOT_FOUND", "message": "User not found"}
        )

    # Prevent self-deletion
    if user.username == admin_username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "SELF_DELETE", "message": "Cannot delete your own account"}
        )

    success = user_service.delete_user(user_id)

    if success:
        _log_user_event("USER_DELETE", admin_username, user.username, request)
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "NOT_FOUND", "message": "User not found"}
        )


@router.post("/{user_id}/reset-password", response_model=ResetPasswordResponse)
async def reset_user_password(
    user_id: str,
    request: Request,
    current_user: dict = Depends(require_user_admin)
):
    """
    Reset a user's password.

    Generates a temporary password that the user must change on next login.

    Requires admin role.
    """
    if not USERS_AVAILABLE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "SERVICE_UNAVAILABLE", "message": "User service not available"}
        )

    user_service = get_user_service()
    admin_username = current_user.get("sub", "unknown")

    # Get user info for logging
    user = user_service.get_user(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "NOT_FOUND", "message": "User not found"}
        )

    temp_password, error = user_service.reset_password(user_id)

    if error:
        _log_user_event(
            "PASSWORD_RESET",
            admin_username,
            user.username,
            request,
            success=False,
            error_message=error
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "RESET_FAILED", "message": error}
        )

    _log_user_event("PASSWORD_RESET", admin_username, user.username, request)

    return ResetPasswordResponse(
        temporary_password=temp_password,
        message="Password has been reset. User must change password on next login."
    )


@router.patch("/{user_id}/status", response_model=UserAccountResponse)
async def toggle_user_status(
    user_id: str,
    request_data: StatusToggleRequest,
    request: Request,
    current_user: dict = Depends(require_user_admin)
):
    """
    Activate or deactivate a user.

    Cannot deactivate your own account.

    Requires admin role.
    """
    if not USERS_AVAILABLE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "SERVICE_UNAVAILABLE", "message": "User service not available"}
        )

    user_service = get_user_service()
    admin_username = current_user.get("sub", "unknown")

    # Get user to check if it's self-deactivation
    existing_user = user_service.get_user(user_id)
    if not existing_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "NOT_FOUND", "message": "User not found"}
        )

    # Prevent self-deactivation
    if existing_user.username == admin_username and not request_data.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "SELF_DEACTIVATE", "message": "Cannot deactivate your own account"}
        )

    user = user_service.toggle_status(user_id, request_data.is_active)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "NOT_FOUND", "message": "User not found"}
        )

    action = "USER_ACTIVATE" if request_data.is_active else "USER_DEACTIVATE"
    _log_user_event(action, admin_username, existing_user.username, request)

    return UserAccountResponse(
        id=user.id,
        username=user.username,
        email=user.email,
        role=user.role,
        permissions=user.permissions,
        created_at=user.created_at,
        last_login=user.last_login,
        is_active=user.is_active,
    )
