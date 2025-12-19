# SAGE - Table Resolver
# ======================
"""
Table Resolver
==============
Deterministically selects which table and columns to use based on:
1. What tables are available in the database
2. Clinical query configuration rules
3. Query context

This module ensures:
- ADaM tables are used over SDTM when available
- Correct population filters are applied
- Analysis columns are preferred over raw columns
- Full transparency in table/column selection
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Any

from .clinical_config import (
    ClinicalQueryConfig,
    QueryDomain,
    PopulationType,
    DEFAULT_CLINICAL_CONFIG
)

logger = logging.getLogger(__name__)


# No hard-coded patterns - LLM handles query understanding naturally
# The LLM understands clinical terminology and can determine:
# - "How many in safety population?" -> ADSL
# - "How many had nausea?" -> ADAE
# - "Patients over 65" -> ADSL


def is_population_count_query(query: str) -> bool:
    """
    This function is deprecated - LLM handles query routing naturally.

    The LLM understands clinical context and will select the appropriate
    table based on the query content. No pattern matching needed.

    Returns False to allow LLM to make table selection decisions.
    """
    # LLM handles all query understanding - no pattern matching
    return False


@dataclass
class ColumnResolution:
    """Resolution of a single column concept."""
    concept: str
    column_name: str
    reason: str
    is_fallback: bool = False


@dataclass
class TableResolution:
    """Result of table resolution - full transparency."""
    # Selected table
    selected_table: str
    table_type: str  # "ADaM" or "SDTM"
    domain: QueryDomain
    selection_reason: str

    # Population
    population: PopulationType
    population_filter: Optional[str]  # e.g., "SAFFL = 'Y'"
    population_name: str              # e.g., "Safety Population"

    # Columns
    columns_resolved: Dict[str, ColumnResolution]  # concept -> resolution

    # Metadata
    fallback_used: bool
    available_tables: List[str]
    table_columns: List[str]

    # Warnings/assumptions
    assumptions: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def get_grade_column(self) -> Optional[str]:
        """Get the resolved toxicity grade column."""
        if "toxicity_grade" in self.columns_resolved:
            return self.columns_resolved["toxicity_grade"].column_name
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'selected_table': self.selected_table,
            'table_type': self.table_type,
            'domain': self.domain.value,
            'selection_reason': self.selection_reason,
            'population': self.population.value,
            'population_filter': self.population_filter,
            'population_name': self.population_name,
            'columns_resolved': {
                k: {
                    'concept': v.concept,
                    'column_name': v.column_name,
                    'reason': v.reason,
                    'is_fallback': v.is_fallback
                }
                for k, v in self.columns_resolved.items()
            },
            'fallback_used': self.fallback_used,
            'assumptions': self.assumptions,
            'warnings': self.warnings
        }


class TableResolver:
    """
    Resolves which table and columns to use for clinical queries.

    This class implements the clinical rules engine's table selection logic:
    1. Detect query domain (AE, demographics, labs, etc.)
    2. Select table based on priority (ADaM > SDTM)
    3. Detect and apply population filter
    4. Resolve column concepts to actual columns

    Example:
        resolver = TableResolver(available_tables={
            'ADAE': ['USUBJID', 'AEDECOD', 'ATOXGR', 'SAFFL'],
            'AE': ['USUBJID', 'AEDECOD', 'AETOXGR'],
            'ADSL': ['USUBJID', 'SAFFL', 'TRT01A']
        })

        resolution = resolver.resolve("How many patients had Grade 4 hypertension?")
        # Returns: TableResolution with ADAE, SAFFL='Y', ATOXGR
    """

    def __init__(self,
                 available_tables: Dict[str, List[str]],
                 config: Optional[ClinicalQueryConfig] = None):
        """
        Initialize table resolver.

        Args:
            available_tables: Dict mapping table names to their columns
            config: Clinical query configuration (uses default if None)
        """
        self.config = config or DEFAULT_CLINICAL_CONFIG

        # Normalize table names to uppercase
        self.available_tables = {
            t.upper(): [c.upper() for c in cols]
            for t, cols in available_tables.items()
        }
        self.available_table_names = set(self.available_tables.keys())

    def resolve(self,
                query: str,
                explicit_population: Optional[PopulationType] = None,
                explicit_domain: Optional[QueryDomain] = None
               ) -> TableResolution:
        """
        Resolve which table to use for a query.

        Args:
            query: User's natural language query
            explicit_population: Override population detection
            explicit_domain: Override domain detection

        Returns:
            TableResolution with full transparency
        """
        assumptions = []
        warnings = []

        # Step 1: Detect domain
        # FIRST check if this is a population count query (should use ADSL)
        # This MUST come before is_safety_query() because "safety population"
        # contains "safety" which would trigger adverse events domain incorrectly
        if not explicit_domain and is_population_count_query(query):
            domain = QueryDomain.DEMOGRAPHICS
            assumptions.append("Detected as population count query, using Demographics (ADSL)")
            logger.info(f"Population count query detected: routing to DEMOGRAPHICS")
        else:
            domain = explicit_domain or self.config.detect_domain(query)

        if domain == QueryDomain.UNKNOWN:
            # Default to adverse events for safety queries
            if self.config.is_safety_query(query):
                domain = QueryDomain.ADVERSE_EVENTS
                assumptions.append("Detected as safety-related query, assuming Adverse Events domain")
            else:
                domain = QueryDomain.DEMOGRAPHICS
                assumptions.append("Could not detect domain, defaulting to Demographics")

        # Step 2: Get table priority for domain
        if domain not in self.config.table_priorities:
            # Fallback to demographics
            domain = QueryDomain.DEMOGRAPHICS
            warnings.append(f"No table priority defined for domain, using Demographics")

        table_priority = self.config.table_priorities[domain]

        # Step 3: Select table based on availability
        try:
            selected_table, is_fallback = table_priority.get_preferred_table(
                self.available_table_names
            )
        except ValueError as e:
            # No suitable table found
            raise ValueError(f"Cannot resolve table for query: {e}")

        # Determine table type
        if is_fallback:
            table_type = "SDTM"
            selection_reason = (f"Using {selected_table} (SDTM) - "
                              f"ADaM table {table_priority.adam_table} not available")
        else:
            table_type = "ADaM"
            selection_reason = (f"Using {selected_table} (ADaM) - "
                              f"preferred for analysis queries")

        # Step 4: Detect population
        population = explicit_population or self.config.detect_population(query)

        # Get population filter
        population_filter, population_name = self._get_population_filter(
            population, selected_table, table_type
        )

        if population != PopulationType.ALL_ENROLLED:
            if table_type == "SDTM" and population_filter is None:
                warnings.append(
                    f"Population filter ({population.value}) requires ADaM table. "
                    f"Cannot filter SDTM table {selected_table}."
                )
                assumptions.append(f"Using all subjects (no population filter in SDTM)")

        # Step 5: Resolve column priorities
        table_columns = self.available_tables.get(selected_table, [])
        columns_resolved = self._resolve_columns(set(table_columns))

        # Check if population flag exists in table
        pop_flag = population.get_flag_column()
        if pop_flag and pop_flag not in table_columns:
            warnings.append(f"Population flag {pop_flag} not found in {selected_table}")
            population_filter = None
            population_name = f"All Subjects ({pop_flag} not available)"

        return TableResolution(
            selected_table=selected_table,
            table_type=table_type,
            domain=domain,
            selection_reason=selection_reason,
            population=population,
            population_filter=population_filter,
            population_name=population_name,
            columns_resolved=columns_resolved,
            fallback_used=is_fallback,
            available_tables=list(self.available_table_names),
            table_columns=table_columns,
            assumptions=assumptions,
            warnings=warnings
        )

    def _get_population_filter(self,
                               population: PopulationType,
                               table: str,
                               table_type: str
                              ) -> Tuple[Optional[str], str]:
        """Get SQL filter and description for population."""
        flag_column = population.get_flag_column()
        population_name = population.get_display_name()

        if flag_column is None:
            return (None, population_name)

        # Check if flag exists in table
        table_columns = self.available_tables.get(table, [])
        if flag_column in table_columns:
            return (f"{flag_column} = 'Y'", population_name)

        # Flag not in this table - might need join with ADSL
        if 'ADSL' in self.available_tables:
            adsl_columns = self.available_tables['ADSL']
            if flag_column in adsl_columns:
                return (f"{flag_column} = 'Y'", f"{population_name} (via ADSL join)")

        return (None, f"{population_name} (flag not available)")

    def _resolve_columns(self, table_columns: Set[str]) -> Dict[str, ColumnResolution]:
        """Resolve column priorities based on availability."""
        resolved = {}

        for concept, priority in self.config.column_priorities.items():
            preferred = priority.preferred_column.upper()
            fallback = priority.fallback_column.upper()

            if preferred in table_columns:
                resolved[concept] = ColumnResolution(
                    concept=concept,
                    column_name=preferred,
                    reason=f"{preferred} (preferred): {priority.description}",
                    is_fallback=False
                )
            elif fallback in table_columns:
                resolved[concept] = ColumnResolution(
                    concept=concept,
                    column_name=fallback,
                    reason=f"{fallback} (fallback): {priority.description}",
                    is_fallback=True
                )

        return resolved

    def get_join_tables(self,
                        primary_table: str,
                        needed_columns: List[str]
                       ) -> List[Tuple[str, str, str]]:
        """
        Determine if joins are needed to get required columns.

        Args:
            primary_table: Main table being queried
            needed_columns: Columns that need to be in result

        Returns:
            List of (table, join_column, join_type) tuples
        """
        joins = []
        primary_columns = set(self.available_tables.get(primary_table, []))
        missing = set(c.upper() for c in needed_columns) - primary_columns

        if not missing:
            return []

        # Check ADSL for missing columns (most common join)
        if 'ADSL' in self.available_tables:
            adsl_columns = set(self.available_tables['ADSL'])
            found_in_adsl = missing & adsl_columns
            if found_in_adsl:
                joins.append(('ADSL', 'USUBJID', 'LEFT'))
                missing -= found_in_adsl

        return joins

    def validate_columns_exist(self,
                               table: str,
                               columns: List[str]
                              ) -> Tuple[bool, List[str]]:
        """
        Validate that columns exist in table.

        Returns:
            Tuple of (all_exist, missing_columns)
        """
        table_columns = set(self.available_tables.get(table.upper(), []))
        columns_upper = [c.upper() for c in columns]
        missing = [c for c in columns_upper if c not in table_columns]
        return (len(missing) == 0, missing)


def get_table_resolver_from_duckdb(db_path: str,
                                   config: Optional[ClinicalQueryConfig] = None
                                  ) -> TableResolver:
    """
    Create TableResolver by introspecting DuckDB database.

    Args:
        db_path: Path to DuckDB database
        config: Clinical configuration to use

    Returns:
        TableResolver initialized with available tables
    """
    import duckdb
    from .sql_security import validate_table_name, add_table_to_whitelist

    conn = duckdb.connect(db_path, read_only=True)
    try:
        # Get all tables
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()

        available_tables = {}
        for (table_name,) in tables:
            # Validate table name before use (and add to whitelist if valid pattern)
            # Table names from information_schema are trusted but we validate anyway
            add_table_to_whitelist(table_name)  # Add discovered tables to whitelist

            if validate_table_name(table_name):
                # Get columns for each table (table_name validated)
                columns = conn.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
                ).fetchall()
                available_tables[table_name] = [col[0] for col, in columns]
            else:
                logger.warning(f"Skipping table with invalid name pattern: {table_name}")

        return TableResolver(available_tables, config)
    finally:
        conn.close()
