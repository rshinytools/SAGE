"""
SAGE Audit Module
==================

Comprehensive audit logging for 21 CFR Part 11 compliance.

This module provides:
- Persistent SQLite-based audit storage
- Authentication event logging (login, logout, failed attempts)
- Query/LLM interaction logging with full details
- Data upload tracking
- API request logging
- Integrity checksums for tamper detection
- Electronic signature support
- Excel/PDF export capabilities
"""

from .models import (
    AuditAction,
    AuditStatus,
    AuditEvent,
    QueryAuditDetails,
    AuditLog,
    AuditFilters,
    AuditStatistics,
    ElectronicSignature,
    IntegrityCheckResult,
)
from .database import AuditDB
from .service import AuditService, get_audit_service

__all__ = [
    # Models
    "AuditAction",
    "AuditStatus",
    "AuditEvent",
    "QueryAuditDetails",
    "AuditLog",
    "AuditFilters",
    "AuditStatistics",
    "ElectronicSignature",
    "IntegrityCheckResult",
    # Database
    "AuditDB",
    # Service
    "AuditService",
    "get_audit_service",
]
