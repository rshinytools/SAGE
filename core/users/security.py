"""
User Security Module
====================

Password hashing and validation using bcrypt.
"""

import re
import secrets
import string
from typing import Tuple, List

import bcrypt


class PasswordManager:
    """Handles password hashing, verification, and validation."""

    # bcrypt work factor (2^12 iterations)
    BCRYPT_ROUNDS = 12

    # Password requirements
    MIN_PASSWORD_LENGTH = 8
    MAX_PASSWORD_LENGTH = 128
    TEMP_PASSWORD_LENGTH = 16

    @staticmethod
    def hash_password(password: str) -> str:
        """
        Hash a password using bcrypt.

        Args:
            password: Plain text password

        Returns:
            bcrypt hash string
        """
        password_bytes = password.encode('utf-8')
        salt = bcrypt.gensalt(rounds=PasswordManager.BCRYPT_ROUNDS)
        hashed = bcrypt.hashpw(password_bytes, salt)
        return hashed.decode('utf-8')

    @staticmethod
    def verify_password(password: str, password_hash: str) -> bool:
        """
        Verify a password against a bcrypt hash.

        Args:
            password: Plain text password to verify
            password_hash: bcrypt hash to verify against

        Returns:
            True if password matches, False otherwise
        """
        try:
            password_bytes = password.encode('utf-8')
            hash_bytes = password_hash.encode('utf-8')
            return bcrypt.checkpw(password_bytes, hash_bytes)
        except Exception:
            return False

    @staticmethod
    def generate_temp_password() -> str:
        """
        Generate a secure temporary password.

        Returns:
            Random password string
        """
        # Use letters, digits, and some safe special characters
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        # Ensure at least one of each required character type
        password = [
            secrets.choice(string.ascii_uppercase),
            secrets.choice(string.ascii_lowercase),
            secrets.choice(string.digits),
            secrets.choice("!@#$%^&*"),
        ]
        # Fill the rest
        remaining_length = PasswordManager.TEMP_PASSWORD_LENGTH - len(password)
        password.extend(secrets.choice(alphabet) for _ in range(remaining_length))
        # Shuffle to avoid predictable positions
        password_list = list(password)
        secrets.SystemRandom().shuffle(password_list)
        return ''.join(password_list)

    @staticmethod
    def validate_password_strength(password: str) -> Tuple[bool, List[str]]:
        """
        Validate password meets complexity requirements.

        Args:
            password: Password to validate

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        if len(password) < PasswordManager.MIN_PASSWORD_LENGTH:
            errors.append(
                f"Password must be at least {PasswordManager.MIN_PASSWORD_LENGTH} characters"
            )

        if len(password) > PasswordManager.MAX_PASSWORD_LENGTH:
            errors.append(
                f"Password must be at most {PasswordManager.MAX_PASSWORD_LENGTH} characters"
            )

        if not any(c.isupper() for c in password):
            errors.append("Password must contain at least one uppercase letter")

        if not any(c.islower() for c in password):
            errors.append("Password must contain at least one lowercase letter")

        if not any(c.isdigit() for c in password):
            errors.append("Password must contain at least one number")

        # Check for special characters
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            errors.append("Password must contain at least one special character")

        return len(errors) == 0, errors

    @staticmethod
    def needs_rehash(password_hash: str) -> bool:
        """
        Check if a password hash needs to be upgraded.

        Args:
            password_hash: Existing bcrypt hash

        Returns:
            True if hash should be regenerated with new parameters
        """
        try:
            # Extract the work factor from the hash
            # bcrypt hash format: $2b$12$...
            parts = password_hash.split('$')
            if len(parts) >= 3:
                current_rounds = int(parts[2])
                return current_rounds < PasswordManager.BCRYPT_ROUNDS
            return True
        except Exception:
            return True


class AccountLockout:
    """Handles account lockout logic."""

    # Lockout configuration
    MAX_FAILED_ATTEMPTS = 5
    LOCKOUT_DURATION_MINUTES = 15

    @staticmethod
    def should_lock(failed_attempts: int) -> bool:
        """Check if account should be locked based on failed attempts."""
        return failed_attempts >= AccountLockout.MAX_FAILED_ATTEMPTS
