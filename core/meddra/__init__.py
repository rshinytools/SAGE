# SAGE MedDRA Module
# ===================
"""
MedDRA (Medical Dictionary for Regulatory Activities) library management.

Provides:
- Loading MedDRA from SAS7BDAT files
- Term lookup with hierarchy
- Search functionality
- Abbreviation resolution

MedDRA Hierarchy:
- SOC (System Organ Class) - highest level, 27 classes
- HLGT (High Level Group Term) - groups HLTs
- HLT (High Level Term) - groups PTs
- PT (Preferred Term) - primary reporting level (~25,000 terms)
- LLT (Lowest Level Term) - most specific, includes synonyms
"""

from .loader import (
    MedDRALoader,
    MedDRAVersion,
    MedDRATerm,
    MedDRAHierarchy,
)

from .lookup import (
    MedDRALookup,
    SearchResult,
    LookupResult,
    ABBREVIATIONS,
)

__all__ = [
    # Loader
    "MedDRALoader",
    "MedDRAVersion",
    "MedDRATerm",
    "MedDRAHierarchy",

    # Lookup
    "MedDRALookup",
    "SearchResult",
    "LookupResult",
    "ABBREVIATIONS",
]
