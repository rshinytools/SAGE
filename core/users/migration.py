"""
User Migration Module
=====================

Migrates the hardcoded admin user from environment variables
to the database on first run.
"""

import os
import logging
from typing import Optional

from .service import get_user_service
from .models import CreateUserRequest

logger = logging.getLogger(__name__)


def migrate_from_env_user() -> bool:
    """
    Migrate the admin user from environment variables to the database.

    This function should be called on application startup. It checks if
    any users exist in the database, and if not, creates an admin user
    from environment variables.

    Environment variables used:
    - ADMIN_USERNAME: Username for admin (default: "admin")
    - ADMIN_PASSWORD: Password for admin (default: "sage2024")
    - ADMIN_EMAIL: Email for admin (default: "admin@example.com")

    Returns:
        True if migration was performed, False if skipped (users already exist)
    """
    from .database import UserDB
    from .security import PasswordManager

    try:
        db = UserDB()

        # Check if any users already exist
        if db.user_count() > 0:
            logger.debug("Users already exist in database, skipping migration")
            return False

        # Get admin credentials from environment
        admin_username = os.getenv("ADMIN_USERNAME", "admin")
        admin_password = os.getenv("ADMIN_PASSWORD", "sage2024")
        admin_email = os.getenv("ADMIN_EMAIL", "admin@example.com")

        # Hash password (bypass validation for migration - user can change later)
        password_hash = PasswordManager.hash_password(admin_password)

        # Create the admin user directly in DB (bypasses validation)
        user = db.create_user(
            username=admin_username,
            email=admin_email,
            password_hash=password_hash,
            role="admin",
            permissions=["*"],  # Full access
        )

        logger.info(f"Migrated admin user from environment: {admin_username}")
        return True

    except Exception as e:
        logger.error(f"Failed to migrate admin user: {e}")
        raise


def ensure_admin_exists() -> bool:
    """
    Ensure at least one admin user exists.

    If no admin users exist, creates one from environment variables.
    This is a safety function to prevent lockout.

    Returns:
        True if an admin exists (created or already present)
    """
    try:
        user_service = get_user_service()

        # Try to find any admin user
        result = user_service.list_users(page=1, page_size=100, include_inactive=True)
        admins = [u for u in result.users if u.role == "admin"]

        if admins:
            logger.debug(f"Found {len(admins)} admin user(s)")
            return True

        # No admin found, create one
        logger.warning("No admin users found, creating from environment variables")
        return migrate_from_env_user()

    except Exception as e:
        logger.error(f"Failed to ensure admin exists: {e}")
        return False


def create_initial_users(users: list) -> int:
    """
    Create multiple initial users.

    Args:
        users: List of dicts with keys: username, email, password, role, permissions

    Returns:
        Number of users created
    """
    user_service = get_user_service()
    created = 0

    for user_data in users:
        try:
            # Check if user already exists
            existing = user_service.get_user_by_username(user_data.get("username"))
            if existing:
                logger.debug(f"User already exists: {user_data.get('username')}")
                continue

            create_request = CreateUserRequest(
                username=user_data.get("username"),
                email=user_data.get("email"),
                password=user_data.get("password"),
                role=user_data.get("role", "user"),
                permissions=user_data.get("permissions", []),
            )

            user_service.create_user(create_request)
            created += 1
            logger.info(f"Created user: {user_data.get('username')}")

        except Exception as e:
            logger.error(f"Failed to create user {user_data.get('username')}: {e}")

    return created
