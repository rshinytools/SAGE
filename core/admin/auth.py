"""
Authentication Module for SAGE Admin UI

Provides authentication providers with support for:
- Local authentication (username/password from .env)
- LDAP/Active Directory (scaffold for future implementation)

The architecture uses a provider pattern to easily switch between
authentication methods based on configuration.
"""

import os
import hashlib
import secrets
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from pathlib import Path

# Try to load python-dotenv if available
try:
    from dotenv import load_dotenv
    # Load .env from project root
    project_root = Path(__file__).parent.parent.parent
    load_dotenv(project_root / ".env")
except ImportError:
    pass


class AuthProvider(ABC):
    """
    Abstract base class for authentication providers.

    All authentication providers must implement these methods
    to ensure consistent authentication behavior across the system.
    """

    @abstractmethod
    def authenticate(self, username: str, password: str) -> bool:
        """
        Authenticate a user with username and password.

        Args:
            username: The username to authenticate
            password: The password to verify

        Returns:
            True if authentication successful, False otherwise
        """
        pass

    @abstractmethod
    def get_user_info(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get user information for an authenticated user.

        Args:
            username: The username to look up

        Returns:
            Dictionary with user info or None if not found
        """
        pass

    @abstractmethod
    def get_user_roles(self, username: str) -> List[str]:
        """
        Get the roles assigned to a user.

        Args:
            username: The username to look up

        Returns:
            List of role names
        """
        pass


class LocalAuthProvider(AuthProvider):
    """
    Simple local authentication provider.

    Reads credentials from environment variables:
    - ADMIN_USERNAME: Admin username (default: admin)
    - ADMIN_PASSWORD: Admin password (default: sage2024)

    For development and simple deployments.
    """

    def __init__(self):
        """Initialize with credentials from environment."""
        self.admin_username = os.getenv("ADMIN_USERNAME", "admin")
        self.admin_password = os.getenv("ADMIN_PASSWORD", "sage2024")

        # Additional users can be defined as JSON in env
        # Format: {"user1": {"password": "pass1", "roles": ["analyst"]}}
        self.additional_users = self._load_additional_users()

    def _load_additional_users(self) -> Dict[str, Dict[str, Any]]:
        """Load additional users from environment variable."""
        import json
        users_json = os.getenv("SAGE_USERS", "{}")
        try:
            return json.loads(users_json)
        except json.JSONDecodeError:
            return {}

    def authenticate(self, username: str, password: str) -> bool:
        """Authenticate against local credentials."""
        # Check admin credentials
        if username == self.admin_username and password == self.admin_password:
            return True

        # Check additional users
        if username in self.additional_users:
            stored_password = self.additional_users[username].get("password", "")
            return password == stored_password

        return False

    def get_user_info(self, username: str) -> Optional[Dict[str, Any]]:
        """Get user information."""
        if username == self.admin_username:
            return {
                "username": username,
                "display_name": "Administrator",
                "email": os.getenv("ADMIN_EMAIL", "admin@localhost"),
                "roles": ["admin"]
            }

        if username in self.additional_users:
            user_data = self.additional_users[username]
            return {
                "username": username,
                "display_name": user_data.get("display_name", username),
                "email": user_data.get("email", ""),
                "roles": user_data.get("roles", ["viewer"])
            }

        return None

    def get_user_roles(self, username: str) -> List[str]:
        """Get user roles."""
        if username == self.admin_username:
            return ["admin"]

        if username in self.additional_users:
            return self.additional_users[username].get("roles", ["viewer"])

        return []


class LDAPAuthProvider(AuthProvider):
    """
    LDAP/Active Directory authentication provider.

    Configuration via environment variables:
    - LDAP_HOST: LDAP server hostname
    - LDAP_PORT: LDAP server port (default: 389, or 636 for SSL)
    - LDAP_USE_SSL: Use SSL connection (default: false)
    - LDAP_BASE_DN: Base DN for user search
    - LDAP_USER_DN_TEMPLATE: Template for user DN (e.g., "cn={},ou=users,dc=example,dc=com")
    - LDAP_SEARCH_FILTER: Filter template for user search
    - LDAP_ADMIN_GROUP: Group name for admin role

    This is a scaffold implementation. Full LDAP integration requires
    the python-ldap or ldap3 library.
    """

    def __init__(self):
        """Initialize LDAP configuration from environment."""
        self.host = os.getenv("LDAP_HOST", "localhost")
        self.port = int(os.getenv("LDAP_PORT", "389"))
        self.use_ssl = os.getenv("LDAP_USE_SSL", "false").lower() == "true"
        self.base_dn = os.getenv("LDAP_BASE_DN", "dc=example,dc=com")
        self.user_dn_template = os.getenv("LDAP_USER_DN_TEMPLATE", "cn={},ou=users,dc=example,dc=com")
        self.search_filter = os.getenv("LDAP_SEARCH_FILTER", "(sAMAccountName={})")
        self.admin_group = os.getenv("LDAP_ADMIN_GROUP", "SAGE_Admins")
        self.analyst_group = os.getenv("LDAP_ANALYST_GROUP", "SAGE_Analysts")

        # Check if ldap3 is available
        self._ldap_available = False
        try:
            import ldap3
            self._ldap_available = True
        except ImportError:
            pass

    def authenticate(self, username: str, password: str) -> bool:
        """
        Authenticate against LDAP server.

        Note: This is a scaffold. Full implementation requires ldap3 library.
        """
        if not self._ldap_available:
            print("WARNING: LDAP authentication requested but ldap3 not installed")
            print("Install with: pip install ldap3")
            return False

        try:
            import ldap3
            from ldap3 import Server, Connection, ALL

            # Create server connection
            server = Server(
                self.host,
                port=self.port,
                use_ssl=self.use_ssl,
                get_info=ALL
            )

            # Format user DN
            user_dn = self.user_dn_template.format(username)

            # Attempt bind
            conn = Connection(server, user=user_dn, password=password)
            if conn.bind():
                conn.unbind()
                return True
            return False

        except Exception as e:
            print(f"LDAP authentication error: {e}")
            return False

    def get_user_info(self, username: str) -> Optional[Dict[str, Any]]:
        """Get user information from LDAP."""
        if not self._ldap_available:
            return None

        try:
            import ldap3
            from ldap3 import Server, Connection, ALL, SUBTREE

            server = Server(self.host, port=self.port, use_ssl=self.use_ssl, get_info=ALL)

            # Use service account for search (would need LDAP_BIND_DN and LDAP_BIND_PASSWORD)
            bind_dn = os.getenv("LDAP_BIND_DN", "")
            bind_password = os.getenv("LDAP_BIND_PASSWORD", "")

            conn = Connection(server, user=bind_dn, password=bind_password)
            if not conn.bind():
                return None

            # Search for user
            search_filter = self.search_filter.format(username)
            conn.search(
                self.base_dn,
                search_filter,
                search_scope=SUBTREE,
                attributes=['cn', 'mail', 'memberOf', 'displayName']
            )

            if conn.entries:
                entry = conn.entries[0]
                return {
                    "username": username,
                    "display_name": str(entry.displayName) if hasattr(entry, 'displayName') else username,
                    "email": str(entry.mail) if hasattr(entry, 'mail') else "",
                    "roles": self._get_roles_from_groups(entry.memberOf if hasattr(entry, 'memberOf') else [])
                }

            conn.unbind()
            return None

        except Exception as e:
            print(f"LDAP user info error: {e}")
            return None

    def get_user_roles(self, username: str) -> List[str]:
        """Get user roles from LDAP groups."""
        user_info = self.get_user_info(username)
        if user_info:
            return user_info.get("roles", ["viewer"])
        return ["viewer"]

    def _get_roles_from_groups(self, groups: List[str]) -> List[str]:
        """Map LDAP groups to application roles."""
        roles = []
        for group in groups:
            group_str = str(group).lower()
            if self.admin_group.lower() in group_str:
                roles.append("admin")
            elif self.analyst_group.lower() in group_str:
                roles.append("analyst")

        if not roles:
            roles.append("viewer")

        return roles


# Session management for Streamlit
class SessionManager:
    """
    Manages user sessions for the Streamlit application.

    Provides session token generation, validation, and user state management.
    Sessions are stored in Streamlit's session_state.
    """

    SESSION_DURATION_HOURS = 8

    @staticmethod
    def create_session(username: str, user_info: Dict[str, Any]) -> str:
        """Create a new session and return session token."""
        import streamlit as st

        token = secrets.token_urlsafe(32)
        expiry = datetime.now() + timedelta(hours=SessionManager.SESSION_DURATION_HOURS)

        st.session_state["authenticated"] = True
        st.session_state["username"] = username
        st.session_state["user_info"] = user_info
        st.session_state["session_token"] = token
        st.session_state["session_expiry"] = expiry
        st.session_state["login_time"] = datetime.now()

        return token

    @staticmethod
    def is_authenticated() -> bool:
        """Check if current session is authenticated and not expired."""
        import streamlit as st

        if not st.session_state.get("authenticated", False):
            return False

        expiry = st.session_state.get("session_expiry")
        if expiry and datetime.now() > expiry:
            SessionManager.logout()
            return False

        return True

    @staticmethod
    def get_current_user() -> Optional[Dict[str, Any]]:
        """Get current user information."""
        import streamlit as st

        if SessionManager.is_authenticated():
            return st.session_state.get("user_info")
        return None

    @staticmethod
    def get_username() -> Optional[str]:
        """Get current username."""
        import streamlit as st

        if SessionManager.is_authenticated():
            return st.session_state.get("username")
        return None

    @staticmethod
    def has_role(role: str) -> bool:
        """Check if current user has a specific role."""
        user_info = SessionManager.get_current_user()
        if user_info:
            return role in user_info.get("roles", [])
        return False

    @staticmethod
    def is_admin() -> bool:
        """Check if current user is an admin."""
        return SessionManager.has_role("admin")

    @staticmethod
    def logout():
        """Clear session state."""
        import streamlit as st

        keys_to_clear = [
            "authenticated", "username", "user_info",
            "session_token", "session_expiry", "login_time"
        ]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]


def get_auth_provider() -> AuthProvider:
    """
    Factory function to get the configured authentication provider.

    Reads AUTH_PROVIDER environment variable:
    - "local" (default): Use LocalAuthProvider
    - "ldap": Use LDAPAuthProvider

    Returns:
        Configured AuthProvider instance
    """
    provider_type = os.getenv("AUTH_PROVIDER", "local").lower()

    if provider_type == "ldap":
        return LDAPAuthProvider()

    return LocalAuthProvider()


def check_authentication() -> bool:
    """
    Check if the current user is authenticated.

    Convenience function for use in Streamlit pages.

    Returns:
        True if authenticated, False otherwise
    """
    return SessionManager.is_authenticated()


def require_auth(func):
    """
    Decorator to require authentication for a Streamlit page.

    Usage:
        @require_auth
        def main():
            st.write("Protected content")
    """
    import functools
    import streamlit as st

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not check_authentication():
            st.warning("Please log in to access this page.")
            st.stop()
        return func(*args, **kwargs)

    return wrapper


def require_role(role: str):
    """
    Decorator factory to require a specific role for a Streamlit page.

    Usage:
        @require_role("admin")
        def admin_page():
            st.write("Admin only content")
    """
    import functools
    import streamlit as st

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not check_authentication():
                st.warning("Please log in to access this page.")
                st.stop()
            if not SessionManager.has_role(role):
                st.error(f"Access denied. Required role: {role}")
                st.stop()
            return func(*args, **kwargs)
        return wrapper
    return decorator
