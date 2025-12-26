# SAGE Admin Package
"""
Admin functionality including authentication and user management.
"""

from .auth import get_auth_provider, check_authentication

__all__ = ["get_auth_provider", "check_authentication"]
