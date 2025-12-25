"""
SAGE API Middleware Package
===========================

FastAPI middleware for cross-cutting concerns.
"""

from .audit import AuditMiddleware

__all__ = ["AuditMiddleware"]
