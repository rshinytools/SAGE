# SAGE API Routers
# =================
"""API route handlers for SAGE platform."""

from . import auth
from . import data
from . import metadata
from . import system
from . import chat
from . import dictionary
from . import meddra
from . import golden_suite
from . import docs
from . import audit
from . import dashboard

__all__ = [
    'auth',
    'audit',
    'chat',
    'dashboard',
    'data',
    'metadata',
    'system',
    'dictionary',
    'meddra',
    'golden_suite',
    'docs',
]
