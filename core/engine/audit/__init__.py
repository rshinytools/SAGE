"""
Audit Module for SAGE AI Chat System.

This module provides:
- Structured Audit Trace: 21 CFR Part 11 compliant logging
- Query Audit: Full traceability of query processing
"""

from .trace_logger import (
    AuditTraceLogger,
    AuditEntry,
    AuditLevel,
    AuditEvent
)

__all__ = [
    'AuditTraceLogger',
    'AuditEntry',
    'AuditLevel',
    'AuditEvent'
]
