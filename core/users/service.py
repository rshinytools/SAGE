"""
User Service Module
===================

High-level user management service with business logic.
"""

import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

from .database import UserDB
from .models import (
    User,
    UserResponse,
    CreateUserRequest,
    UpdateUserRequest,
    PaginatedUsersResponse,
    AVAILABLE_PERMISSIONS,
)
from .security import PasswordManager, AccountLockout

logger = logging.getLogger(__name__)


class UserService:
    """
    Service layer for user management.

    Handles business logic, password hashing, and coordinates
    between the database and API layers.
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the UserService.

        Args:
            db_path: Optional path to the users database
        """
        self._db = UserDB(db_path)
        self._password_manager = PasswordManager()

    # ==================== USER CRUD ====================

    def create_user(self, request: CreateUserRequest) -> UserResponse:
        """
        Create a new user.

        Args:
            request: User creation request

        Returns:
            Created user response

        Raises:
            ValueError: If validation fails or user exists
        """
        # Validate password strength
        is_valid, errors = self._password_manager.validate_password_strength(
            request.password
        )
        if not is_valid:
            raise ValueError(f"Password requirements not met: {'; '.join(errors)}")

        # Validate role
        valid_roles = ["admin", "user", "viewer"]
        if request.role not in valid_roles:
            raise ValueError(f"Invalid role. Must be one of: {valid_roles}")

        # Validate permissions
        invalid_perms = set(request.permissions) - set(AVAILABLE_PERMISSIONS)
        if invalid_perms:
            raise ValueError(f"Invalid permissions: {invalid_perms}")

        # Hash password
        password_hash = self._password_manager.hash_password(request.password)

        # Create user
        user = self._db.create_user(
            username=request.username,
            email=request.email,
            password_hash=password_hash,
            role=request.role,
            permissions=request.permissions,
        )

        logger.info(f"Created user: {request.username}")
        return self._to_response(user)

    def get_user(self, user_id: str) -> Optional[UserResponse]:
        """Get a user by ID."""
        user = self._db.get_user_by_id(user_id)
        return self._to_response(user) if user else None

    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get full user object by username (internal use)."""
        return self._db.get_user_by_username(username)

    def list_users(
        self,
        page: int = 1,
        page_size: int = 50,
        include_inactive: bool = True,
    ) -> PaginatedUsersResponse:
        """
        List users with pagination.

        Args:
            page: Page number (1-indexed)
            page_size: Number of users per page
            include_inactive: Whether to include inactive users

        Returns:
            Paginated user response
        """
        users, total = self._db.list_users(page, page_size, include_inactive)
        total_pages = (total + page_size - 1) // page_size

        return PaginatedUsersResponse(
            users=[self._to_response(u) for u in users],
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    def update_user(
        self,
        user_id: str,
        request: UpdateUserRequest,
    ) -> Optional[UserResponse]:
        """
        Update a user.

        Args:
            user_id: ID of user to update
            request: Update request

        Returns:
            Updated user or None if not found

        Raises:
            ValueError: If validation fails
        """
        # Validate role if provided
        if request.role is not None:
            valid_roles = ["admin", "user", "viewer"]
            if request.role not in valid_roles:
                raise ValueError(f"Invalid role. Must be one of: {valid_roles}")

        # Validate permissions if provided
        if request.permissions is not None:
            invalid_perms = set(request.permissions) - set(AVAILABLE_PERMISSIONS)
            if invalid_perms:
                raise ValueError(f"Invalid permissions: {invalid_perms}")

        user = self._db.update_user(
            user_id=user_id,
            email=request.email,
            role=request.role,
            permissions=request.permissions,
            is_active=request.is_active,
        )

        if user:
            logger.info(f"Updated user: {user.username}")
            return self._to_response(user)
        return None

    def delete_user(self, user_id: str) -> bool:
        """
        Delete a user.

        Args:
            user_id: ID of user to delete

        Returns:
            True if deleted, False if not found
        """
        user = self._db.get_user_by_id(user_id)
        if user:
            result = self._db.delete_user(user_id)
            if result:
                logger.info(f"Deleted user: {user.username}")
            return result
        return False

    # ==================== AUTHENTICATION ====================

    def authenticate(
        self,
        username: str,
        password: str,
    ) -> Tuple[Optional[User], Optional[str]]:
        """
        Authenticate a user.

        Args:
            username: Username to authenticate
            password: Password to verify

        Returns:
            Tuple of (User if authenticated, error message if failed)
        """
        user = self._db.get_user_by_username(username)

        if not user:
            return None, "Invalid username or password"

        # Check if account is locked
        if self._db.is_account_locked(user.id):
            return None, "Account is locked. Please try again later."

        # Check if account is active
        if not user.is_active:
            return None, "Account is inactive. Please contact administrator."

        # Verify password
        if not self._password_manager.verify_password(password, user.password_hash):
            # Record failed attempt
            failed_count = self._db.record_failed_login(user.id)
            if AccountLockout.should_lock(failed_count):
                self._db.lock_account(
                    user.id,
                    AccountLockout.LOCKOUT_DURATION_MINUTES
                )
                logger.warning(f"Account locked due to failed attempts: {username}")
                return None, "Account locked due to too many failed attempts"
            return None, "Invalid username or password"

        # Reset failed login counter
        self._db.reset_failed_logins(user.id)

        # Check if password needs rehash
        if self._password_manager.needs_rehash(user.password_hash):
            new_hash = self._password_manager.hash_password(password)
            self._db.update_password(user.id, new_hash)

        logger.info(f"User authenticated: {username}")
        return user, None

    def change_password(
        self,
        user_id: str,
        old_password: str,
        new_password: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        Change a user's password.

        Args:
            user_id: ID of user
            old_password: Current password
            new_password: New password

        Returns:
            Tuple of (success, error message if failed)
        """
        user = self._db.get_user_by_id(user_id)
        if not user:
            return False, "User not found"

        # Verify old password
        if not self._password_manager.verify_password(old_password, user.password_hash):
            return False, "Current password is incorrect"

        # Validate new password
        is_valid, errors = self._password_manager.validate_password_strength(
            new_password
        )
        if not is_valid:
            return False, f"Password requirements not met: {'; '.join(errors)}"

        # Update password
        new_hash = self._password_manager.hash_password(new_password)
        self._db.update_password(user_id, new_hash, must_change=False)

        # Revoke all existing sessions (security measure)
        self._db.revoke_all_sessions(user_id)

        logger.info(f"Password changed for user: {user.username}")
        return True, None

    def reset_password(self, user_id: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Reset a user's password (admin action).

        Args:
            user_id: ID of user

        Returns:
            Tuple of (temporary password, error message if failed)
        """
        user = self._db.get_user_by_id(user_id)
        if not user:
            return None, "User not found"

        # Generate temporary password
        temp_password = self._password_manager.generate_temp_password()
        new_hash = self._password_manager.hash_password(temp_password)

        # Update password with must_change flag
        self._db.update_password(user_id, new_hash, must_change=True)

        # Revoke all existing sessions
        self._db.revoke_all_sessions(user_id)

        # Unlock account if locked
        self._db.unlock_account(user_id)

        logger.info(f"Password reset for user: {user.username}")
        return temp_password, None

    def toggle_status(
        self,
        user_id: str,
        is_active: bool,
    ) -> Optional[UserResponse]:
        """
        Activate or deactivate a user.

        Args:
            user_id: ID of user
            is_active: New status

        Returns:
            Updated user or None if not found
        """
        user = self._db.update_user(user_id, is_active=is_active)
        if user:
            action = "activated" if is_active else "deactivated"
            logger.info(f"User {action}: {user.username}")

            # If deactivating, revoke all sessions
            if not is_active:
                self._db.revoke_all_sessions(user_id)

            return self._to_response(user)
        return None

    # ==================== SESSION MANAGEMENT ====================

    def create_session(
        self,
        user_id: str,
        token: str,
        expires_in_minutes: int = 60,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> str:
        """
        Create a session for a user.

        Args:
            user_id: ID of the user
            token: JWT token
            expires_in_minutes: Token expiration time
            ip_address: Client IP
            user_agent: Client user agent

        Returns:
            Session ID
        """
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        expires_at = datetime.utcnow() + timedelta(minutes=expires_in_minutes)

        return self._db.create_session(
            user_id=user_id,
            token_hash=token_hash,
            expires_at=expires_at,
            ip_address=ip_address,
            user_agent=user_agent,
        )

    def revoke_session(self, token: str) -> bool:
        """Revoke a session by token."""
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        return self._db.revoke_session_by_token(token_hash)

    def is_token_valid(self, token: str) -> bool:
        """Check if a token is valid (not revoked and not expired)."""
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        return not self._db.is_token_revoked(token_hash)

    def cleanup_sessions(self) -> int:
        """Remove expired sessions."""
        return self._db.cleanup_expired_sessions()

    # ==================== UTILITY ====================

    def get_available_permissions(self) -> List[str]:
        """Get list of available permissions."""
        return AVAILABLE_PERMISSIONS.copy()

    def user_exists(self) -> bool:
        """Check if any users exist in the database."""
        return self._db.user_count() > 0

    def _to_response(self, user: User) -> UserResponse:
        """Convert User to UserResponse (without sensitive fields)."""
        return UserResponse(
            id=user.id,
            username=user.username,
            email=user.email,
            role=user.role,
            permissions=user.permissions,
            is_active=user.is_active,
            created_at=user.created_at,
            updated_at=user.updated_at,
            last_login=user.last_login,
            must_change_password=user.must_change_password,
        )


# Singleton instance
_user_service: Optional[UserService] = None


def get_user_service() -> UserService:
    """
    Get the global UserService instance.

    Returns:
        UserService singleton
    """
    global _user_service
    if _user_service is None:
        _user_service = UserService()
    return _user_service
