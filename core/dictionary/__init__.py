# SAGE Dictionary Module
# ======================
"""
Factory 3: Dictionary Plant

Provides fuzzy matching for clinical data values.
Enables query understanding by matching user input to actual database values.

Components:
- ValueScanner: Extracts unique values from DuckDB tables
- FuzzyMatcher: RapidFuzz-based string matching for typo correction
- SchemaMapper: Generates schema_map.json for column lookups
- TermResolver: Unified resolution combining fuzzy matching + MedDRA lookup

Note: Semantic/embedding search has been intentionally excluded.
Clinical data requires exact terminology matching (MedDRA controlled vocabulary).
Synonym matching is handled via MedDRA hierarchy, not AI embeddings.
"""

from .value_scanner import (
    ValueScanner,
    ScanResult,
    ScanStatistics,
    scan_database,
    SCANNABLE_COLUMNS,
)

from .fuzzy_matcher import (
    FuzzyMatcher,
    FuzzyMatch,
    IndexEntry,
)

from .schema_mapper import (
    SchemaMapper,
    SchemaMap,
    ColumnInfo,
    TableInfo,
    build_schema_map,
)

from .term_resolver import (
    TermResolver,
    MatchSource,
    ResolvedTerm,
    TermSuggestion,
    ResolutionResult,
    create_term_resolver,
)


__all__ = [
    # Value Scanner
    "ValueScanner",
    "ScanResult",
    "ScanStatistics",
    "scan_database",
    "SCANNABLE_COLUMNS",

    # Fuzzy Matcher
    "FuzzyMatcher",
    "FuzzyMatch",
    "IndexEntry",

    # Schema Mapper
    "SchemaMapper",
    "SchemaMap",
    "ColumnInfo",
    "TableInfo",
    "build_schema_map",

    # Term Resolver
    "TermResolver",
    "MatchSource",
    "ResolvedTerm",
    "TermSuggestion",
    "ResolutionResult",
    "create_term_resolver",
]
