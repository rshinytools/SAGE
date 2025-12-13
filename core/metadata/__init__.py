# SAGE Metadata Factory Module
# =============================
# Factory 2: Metadata Refinery - Excel Specs to Golden Metadata
"""
Metadata processing and Golden Metadata management.

This module provides:
- ExcelParser: Parse SDTM/ADaM specification Excel files
- CodelistMerger: Merge codelists with variable definitions
- LLMDrafter: Generate plain-English descriptions using LLM
- VersionControl: Track metadata changes and maintain history
- MetadataStore: Store and manage golden metadata
"""

from .excel_parser import (
    ExcelParser,
    VariableSpec,
    DomainSpec,
    CodelistSpec,
    ParseResult
)
from .codelist_merger import (
    CodelistMerger,
    EnrichedVariable,
    EnrichedDomain,
    MergeResult
)
from .version_control import (
    VersionControl,
    MetadataVersion,
    MetadataChange,
    ChangeType,
    DiffResult
)
from .metadata_store import (
    MetadataStore,
    GoldenDomain,
    GoldenVariable,
    GoldenCodelist,
    ApprovalStatus
)
from .llm_drafter import (
    LLMDrafter,
    TemplateDrafter,
    DraftRequest,
    DraftResult
)
from .cdisc_library import (
    CDISCLibrary,
    CDISCDomain,
    CDISCVariable,
    MatchResult,
    initialize_cdisc_library
)
from .auto_approval import (
    AutoApprovalEngine,
    ApprovalDecision,
    AuditProgress,
    AuditResult,
    run_audit
)

__all__ = [
    # Excel Parser
    'ExcelParser',
    'VariableSpec',
    'DomainSpec',
    'CodelistSpec',
    'ParseResult',
    # Codelist Merger
    'CodelistMerger',
    'EnrichedVariable',
    'EnrichedDomain',
    'MergeResult',
    # Version Control
    'VersionControl',
    'MetadataVersion',
    'MetadataChange',
    'ChangeType',
    'DiffResult',
    # Metadata Store
    'MetadataStore',
    'GoldenDomain',
    'GoldenVariable',
    'GoldenCodelist',
    'ApprovalStatus',
    # LLM Drafter
    'LLMDrafter',
    'TemplateDrafter',
    'DraftRequest',
    'DraftResult',
    # CDISC Library
    'CDISCLibrary',
    'CDISCDomain',
    'CDISCVariable',
    'MatchResult',
    'initialize_cdisc_library',
    # Auto-Approval / Audit
    'AutoApprovalEngine',
    'ApprovalDecision',
    'AuditProgress',
    'AuditResult',
    'run_audit',
]
