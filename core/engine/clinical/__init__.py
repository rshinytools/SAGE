"""
Clinical Enterprise Components for SAGE.

This module provides enterprise-grade clinical components for:
- Protocol Guard: Resolve ambiguous clinical terms
- Certified Answers: Deterministic execution for verified queries
- Schema Validation: Prevent stale SQL execution
- View Routing: Route complex queries to pre-joined views
- Helpful Refusals: Actionable guidance on low confidence
"""

from .protocol_guard import (
    ProtocolGuard,
    ProtocolGuardResult,
    AmbiguousTerm,
    TermResolution,
    ResolutionType,
    AMBIGUOUS_TERMS
)

from .certified_answer import (
    CertifiedAnswerSystem,
    CertificationResult,
    CertifiedMatch,
    CertificationLevel
)

from .schema_validator import (
    SchemaValidator,
    SchemaValidationResult,
    SchemaIssue,
    SchemaValidationStatus
)

from .view_router import (
    GoldenViewRouter,
    ViewMapping,
    ViewRoutingResult,
    GOLDEN_VIEWS
)

from .helpful_refusal import (
    HelpfulRefusalSystem,
    HelpfulRefusal,
    RefusalReason
)

__all__ = [
    # Protocol Guard
    'ProtocolGuard',
    'ProtocolGuardResult',
    'AmbiguousTerm',
    'TermResolution',
    'ResolutionType',
    'AMBIGUOUS_TERMS',

    # Certified Answers
    'CertifiedAnswerSystem',
    'CertificationResult',
    'CertifiedMatch',
    'CertificationLevel',

    # Schema Validation
    'SchemaValidator',
    'SchemaValidationResult',
    'SchemaIssue',
    'SchemaValidationStatus',

    # View Routing
    'GoldenViewRouter',
    'ViewMapping',
    'ViewRoutingResult',
    'GOLDEN_VIEWS',

    # Helpful Refusals
    'HelpfulRefusalSystem',
    'HelpfulRefusal',
    'RefusalReason'
]
