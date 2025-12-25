"""
SAGE User Management Module
============================

User management with role-based access control.

This module provides:
- SQLite-based user storage
- bcrypt password hashing
- Role and permission management
- Account lockout protection
- Session management with token revocation
"""

from .models import (
    UserRole,
    UserStatus,
    User,
    UserResponse,
    CreateUserRequest,
    UpdateUserRequest,
    ChangePasswordRequest,
    ResetPasswordResponse,
    StatusToggleRequest,
    PaginatedUsersResponse,
    Session,
    AVAILABLE_PERMISSIONS,
)
from .database import UserDB
from .service import UserService, get_user_service
from .security import PasswordManager, AccountLockout
from .migration import migrate_from_env_user, ensure_admin_exists

__all__ = [
    # Models
    "UserRole",
    "UserStatus",
    "User",
    "UserResponse",
    "CreateUserRequest",
    "UpdateUserRequest",
    "ChangePasswordRequest",
    "ResetPasswordResponse",
    "StatusToggleRequest",
    "PaginatedUsersResponse",
    "Session",
    "AVAILABLE_PERMISSIONS",
    # Database
    "UserDB",
    # Service
    "UserService",
    "get_user_service",
    # Security
    "PasswordManager",
    "AccountLockout",
    # Migration
    "migrate_from_env_user",
    "ensure_admin_exists",
]
