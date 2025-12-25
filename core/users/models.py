"""
User Models
===========

Pydantic models and dataclasses for user management.
"""

from datetime import datetime
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field, EmailStr


class UserRole(str, Enum):
    """User roles for access control."""
    ADMIN = "admin"
    USER = "user"
    VIEWER = "viewer"


class UserStatus(str, Enum):
    """User account status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    LOCKED = "locked"


# Available permissions in the system
# Simplified to 3 access levels:
# 1. Admin (*) - Full access to everything
# 2. User Admin (user_admin) - User Management + Audit + Chat
# 3. Chat Only (no permission needed) - Just Chat access
AVAILABLE_PERMISSIONS = [
    "*",                 # Full access (superuser/admin)
    "user_admin",        # User Management + Audit pages + Chat
]


class User(BaseModel):
    """User model for database records."""
    id: str
    username: str
    email: str
    password_hash: str
    role: str = UserRole.USER.value
    permissions: List[str] = Field(default_factory=list)
    is_active: bool = True
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_login: Optional[datetime] = None
    failed_login_attempts: int = 0
    locked_until: Optional[datetime] = None
    password_changed_at: Optional[datetime] = None
    must_change_password: bool = False

    class Config:
        from_attributes = True


class UserResponse(BaseModel):
    """User response model (without sensitive fields)."""
    id: str
    username: str
    email: str
    role: str
    permissions: List[str]
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_login: Optional[datetime] = None
    must_change_password: bool = False


class CreateUserRequest(BaseModel):
    """Request model for creating a new user."""
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=8)
    role: str = UserRole.USER.value
    permissions: List[str] = Field(default_factory=list)

    class Config:
        json_schema_extra = {
            "example": {
                "username": "john.doe",
                "email": "john.doe@example.com",
                "password": "SecurePass123",
                "role": "user",
                "permissions": ["data_query", "audit_view"]
            }
        }


class UpdateUserRequest(BaseModel):
    """Request model for updating a user."""
    email: Optional[EmailStr] = None
    role: Optional[str] = None
    permissions: Optional[List[str]] = None
    is_active: Optional[bool] = None


class ChangePasswordRequest(BaseModel):
    """Request model for changing password."""
    old_password: str
    new_password: str = Field(..., min_length=8)


class ResetPasswordResponse(BaseModel):
    """Response model for password reset."""
    temporary_password: str
    message: str = "Password has been reset. User must change password on next login."


class StatusToggleRequest(BaseModel):
    """Request model for toggling user status."""
    is_active: bool


class PaginatedUsersResponse(BaseModel):
    """Paginated response for user list."""
    users: List[UserResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class Session(BaseModel):
    """User session model."""
    id: str
    user_id: str
    token_hash: str
    created_at: datetime
    expires_at: datetime
    revoked: bool = False
    revoked_at: Optional[datetime] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
