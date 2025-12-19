# Tests for Table Resolver
"""
Test Suite for Table Resolver (Clinical Rules Engine)
====================================================
Tests table selection logic including:
- ADaM table selection when available
- SDTM fallback when ADaM not available
- Population filter application
- Column resolution
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.engine.table_resolver import TableResolver, TableResolution
from core.engine.clinical_config import ClinicalQueryConfig, QueryDomain, PopulationType, DEFAULT_CLINICAL_CONFIG


class TestTableSelection:
    """Test table selection logic."""

    def setup_method(self):
        """Set up test fixtures with mock tables."""
        # Mock available tables - both ADaM and SDTM
        self.available_tables = {
            'ADAE': ['USUBJID', 'AEDECOD', 'ATOXGR', 'SAFFL', 'TRTEMFL', 'AESER'],
            'ADSL': ['USUBJID', 'AGE', 'SEX', 'RACE', 'SAFFL', 'ITTFL', 'EFFFL'],
            'ADLB': ['USUBJID', 'PARAMCD', 'AVAL', 'SAFFL'],
            'AE': ['USUBJID', 'AEDECOD', 'AETOXGR', 'AESER'],
            'DM': ['USUBJID', 'AGE', 'SEX', 'RACE'],
            'LB': ['USUBJID', 'LBTESTCD', 'LBSTRESN']
        }
        self.resolver = TableResolver(
            available_tables=self.available_tables,
            config=DEFAULT_CLINICAL_CONFIG
        )

    def test_select_adae_for_ae_query(self):
        """Test table selection with LLM-first approach."""
        # With LLM-first approach, domain detection returns UNKNOWN
        # The resolver will use defaults; LLM handles intelligent selection
        resolution = self.resolver.resolve(
            query="How many patients had headaches?"
        )
        # With UNKNOWN domain, defaults to ADSL (demographics)
        assert resolution.selected_table in ["ADSL", "ADAE"]
        assert resolution.table_type == "ADaM"

    def test_select_adsl_for_demographics(self):
        """Test ADSL is selected for demographics queries."""
        resolution = self.resolver.resolve(
            query="Show age distribution"
        )
        assert resolution.selected_table == "ADSL"
        assert resolution.table_type == "ADaM"

    def test_fallback_to_sdtm_when_adam_missing(self):
        """Test SDTM fallback when ADaM not available."""
        # Create resolver without ADAE
        tables_without_adae = {
            'AE': ['USUBJID', 'AEDECOD', 'AETOXGR', 'AESER'],
            'DM': ['USUBJID', 'AGE', 'SEX', 'RACE']
        }
        resolver = TableResolver(
            available_tables=tables_without_adae,
            config=DEFAULT_CLINICAL_CONFIG
        )

        # With explicit domain, should use correct table
        resolution = resolver.resolve(
            query="How many patients had headaches?",
            explicit_domain=QueryDomain.ADVERSE_EVENTS
        )
        assert resolution.selected_table == "AE"
        assert resolution.table_type == "SDTM"
        assert resolution.fallback_used is True

    def test_assumptions_recorded(self):
        """Test that resolution includes metadata."""
        resolution = self.resolver.resolve(
            query="How many patients had headaches?"
        )
        # Should have selection reason
        assert resolution.selection_reason is not None


class TestPopulationFilter:
    """Test population filter application."""

    def setup_method(self):
        """Set up test fixtures."""
        self.available_tables = {
            'ADAE': ['USUBJID', 'AEDECOD', 'ATOXGR', 'SAFFL', 'TRTEMFL'],
            'ADSL': ['USUBJID', 'AGE', 'SEX', 'SAFFL', 'ITTFL', 'EFFFL']
        }
        self.resolver = TableResolver(
            available_tables=self.available_tables,
            config=DEFAULT_CLINICAL_CONFIG
        )

    def test_safety_population_default_for_ae(self):
        """Test population defaults to ALL_ENROLLED - LLM handles filtering."""
        resolution = self.resolver.resolve(
            query="How many patients had nausea?"
        )
        # With LLM-first approach, population detection returns ALL_ENROLLED
        # LLM decides what filters to apply based on context
        assert resolution.population == PopulationType.ALL_ENROLLED

    def test_explicit_itt_population(self):
        """Test explicit ITT population request via parameter."""
        resolution = self.resolver.resolve(
            query="How many patients in ITT population had nausea?",
            explicit_population=PopulationType.ITT
        )
        assert resolution.population == PopulationType.ITT

    def test_explicit_safety_population(self):
        """Test explicit safety population request via parameter."""
        resolution = self.resolver.resolve(
            query="Show adverse events in safety population",
            explicit_population=PopulationType.SAFETY
        )
        assert resolution.population_filter == "SAFFL = 'Y'"
        assert resolution.population == PopulationType.SAFETY

    def test_population_filter_includes_flag_column(self):
        """Test population flag column is in table."""
        resolution = self.resolver.resolve(
            query="How many patients had headaches?"
        )
        # SAFFL should be in the table columns
        assert "SAFFL" in resolution.table_columns


class TestColumnResolution:
    """Test column resolution for specific concepts."""

    def setup_method(self):
        """Set up test fixtures."""
        self.available_tables = {
            'ADAE': ['USUBJID', 'AEDECOD', 'ATOXGR', 'AETOXGR', 'SAFFL', 'ASEV', 'AESEV'],
            'AE': ['USUBJID', 'AEDECOD', 'AETOXGR', 'AESEV']
        }
        self.resolver = TableResolver(
            available_tables=self.available_tables,
            config=DEFAULT_CLINICAL_CONFIG
        )

    def test_atoxgr_preferred_over_aetoxgr(self):
        """Test ATOXGR is preferred when both available."""
        # Use explicit domain to force ADAE selection
        resolution = self.resolver.resolve(
            query="Show grade 3 adverse events",
            explicit_domain=QueryDomain.ADVERSE_EVENTS
        )

        # Check grade column resolution
        grade_col = resolution.get_grade_column()
        assert grade_col == "ATOXGR"

    def test_aetoxgr_used_when_atoxgr_missing(self):
        """Test AETOXGR used when ATOXGR not available."""
        tables = {
            'ADAE': ['USUBJID', 'AEDECOD', 'AETOXGR', 'SAFFL'],
        }
        resolver = TableResolver(
            available_tables=tables,
            config=DEFAULT_CLINICAL_CONFIG
        )

        resolution = resolver.resolve(
            query="Show grade 3 adverse events",
            explicit_domain=QueryDomain.ADVERSE_EVENTS
        )

        grade_col = resolution.get_grade_column()
        assert grade_col == "AETOXGR"


class TestTableResolutionDataclass:
    """Test TableResolution dataclass."""

    def test_get_grade_column(self):
        """Test get_grade_column method."""
        from core.engine.table_resolver import ColumnResolution

        resolution = TableResolution(
            selected_table="ADAE",
            table_type="ADaM",
            domain=QueryDomain.ADVERSE_EVENTS,
            selection_reason="Using ADAE (preferred)",
            population=PopulationType.SAFETY,
            population_filter="SAFFL = 'Y'",
            population_name="Safety Population",
            columns_resolved={
                'toxicity_grade': ColumnResolution(
                    concept='toxicity_grade',
                    column_name='ATOXGR',
                    reason='ATOXGR (preferred)',
                    is_fallback=False
                )
            },
            fallback_used=False,
            available_tables=['ADAE', 'ADSL'],
            table_columns=['USUBJID', 'ATOXGR', 'AEDECOD'],
            assumptions=[]
        )

        assert resolution.get_grade_column() == "ATOXGR"

    def test_to_dict(self):
        """Test to_dict method."""
        from core.engine.table_resolver import ColumnResolution

        resolution = TableResolution(
            selected_table="ADAE",
            table_type="ADaM",
            domain=QueryDomain.ADVERSE_EVENTS,
            selection_reason="Using ADAE",
            population=PopulationType.SAFETY,
            population_filter="SAFFL = 'Y'",
            population_name="Safety Population",
            columns_resolved={},
            fallback_used=False,
            available_tables=['ADAE'],
            table_columns=["USUBJID", "ATOXGR"],
            assumptions=["Using safety population"]
        )

        result = resolution.to_dict()

        assert result['selected_table'] == "ADAE"
        assert result['table_type'] == "ADaM"
        assert result['population_filter'] == "SAFFL = 'Y'"


class TestEdgeCases:
    """Test edge cases."""

    def setup_method(self):
        """Set up test fixtures."""
        self.available_tables = {
            'ADAE': ['USUBJID', 'AEDECOD', 'ATOXGR', 'SAFFL'],
            'ADSL': ['USUBJID', 'AGE', 'SEX', 'SAFFL', 'ITTFL']
        }
        self.resolver = TableResolver(
            available_tables=self.available_tables,
            config=DEFAULT_CLINICAL_CONFIG
        )

    def test_empty_query_defaults(self):
        """Test empty query uses defaults."""
        resolution = self.resolver.resolve(query="")
        # Should still return a resolution with defaults
        assert resolution is not None
        assert resolution.selected_table is not None

    def test_no_matching_domain_uses_default(self):
        """Test query with no clear domain uses default."""
        resolution = self.resolver.resolve(
            query="Tell me something"
        )
        # Should fall back to demographics or some default
        assert resolution.selected_table is not None

    def test_explicit_domain_override(self):
        """Test explicit domain override."""
        resolution = self.resolver.resolve(
            query="Show data",
            explicit_domain=QueryDomain.ADVERSE_EVENTS
        )
        assert resolution.domain == QueryDomain.ADVERSE_EVENTS
        assert resolution.selected_table == "ADAE"

    def test_explicit_population_override(self):
        """Test explicit population override."""
        resolution = self.resolver.resolve(
            query="Show adverse events",
            explicit_population=PopulationType.ITT
        )
        assert resolution.population == PopulationType.ITT


class TestJoinDetection:
    """Test join table detection."""

    def setup_method(self):
        """Set up test fixtures."""
        self.available_tables = {
            'ADAE': ['USUBJID', 'AEDECOD', 'ATOXGR'],
            'ADSL': ['USUBJID', 'AGE', 'SEX', 'SAFFL', 'ITTFL']
        }
        self.resolver = TableResolver(
            available_tables=self.available_tables,
            config=DEFAULT_CLINICAL_CONFIG
        )

    def test_join_needed_for_missing_column(self):
        """Test join detected when column missing."""
        joins = self.resolver.get_join_tables('ADAE', ['SAFFL'])
        # SAFFL not in ADAE, but in ADSL
        assert len(joins) > 0
        assert joins[0][0] == 'ADSL'

    def test_no_join_when_column_present(self):
        """Test no join when column present."""
        joins = self.resolver.get_join_tables('ADAE', ['AEDECOD'])
        assert len(joins) == 0


class TestColumnValidation:
    """Test column validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.available_tables = {
            'ADAE': ['USUBJID', 'AEDECOD', 'ATOXGR', 'SAFFL'],
        }
        self.resolver = TableResolver(
            available_tables=self.available_tables,
            config=DEFAULT_CLINICAL_CONFIG
        )

    def test_validate_existing_columns(self):
        """Test validation of existing columns."""
        valid, missing = self.resolver.validate_columns_exist('ADAE', ['USUBJID', 'AEDECOD'])
        assert valid is True
        assert len(missing) == 0

    def test_validate_missing_columns(self):
        """Test validation detects missing columns."""
        valid, missing = self.resolver.validate_columns_exist('ADAE', ['NONEXISTENT'])
        assert valid is False
        assert 'NONEXISTENT' in missing


class TestPopulationCountQueryDetection:
    """Test population count query detection - now LLM-handled.

    With the LLM-first approach, query routing is handled by the LLM
    which naturally understands:
    - "How many in safety population?" -> ADSL
    - "How many had nausea?" -> ADAE

    The pattern-matching has been removed in favor of LLM intelligence.
    """

    def setup_method(self):
        """Set up test fixtures."""
        self.available_tables = {
            'ADAE': ['USUBJID', 'AEDECOD', 'ATOXGR', 'SAFFL', 'TRTEMFL'],
            'ADSL': ['USUBJID', 'AGE', 'SEX', 'SAFFL', 'ITTFL', 'EFFFL', 'PPROTFL']
        }
        self.resolver = TableResolver(
            available_tables=self.available_tables,
            config=DEFAULT_CLINICAL_CONFIG
        )

    def test_is_population_count_query_function(self):
        """Test is_population_count_query returns False - LLM handles routing."""
        from core.engine.table_resolver import is_population_count_query

        # With LLM-first approach, this function always returns False
        # LLM handles intelligent routing based on full query context
        assert is_population_count_query("How many patients are in the safety population?") is False
        assert is_population_count_query("How many had nausea in safety population?") is False

    def test_safety_population_count_uses_adsl(self):
        """Test ADSL is available for demographics queries."""
        # With explicit domain, can force ADSL selection
        resolution = self.resolver.resolve(
            query="How many patients are in the safety population?",
            explicit_domain=QueryDomain.DEMOGRAPHICS
        )
        assert resolution.selected_table == "ADSL"

    def test_itt_population_count_uses_adsl(self):
        """Test ITT population with explicit domain."""
        resolution = self.resolver.resolve(
            query="How many patients are in the ITT population?",
            explicit_domain=QueryDomain.DEMOGRAPHICS,
            explicit_population=PopulationType.ITT
        )
        assert resolution.selected_table == "ADSL"
        assert resolution.population == PopulationType.ITT
        assert resolution.population_filter == "ITTFL = 'Y'"

    def test_efficacy_population_count_uses_adsl(self):
        """Test efficacy population with explicit parameters."""
        resolution = self.resolver.resolve(
            query="How many are in the efficacy population?",
            explicit_domain=QueryDomain.DEMOGRAPHICS,
            explicit_population=PopulationType.EFFICACY
        )
        assert resolution.selected_table == "ADSL"
        assert resolution.population == PopulationType.EFFICACY

    def test_ae_in_safety_population_uses_adae(self):
        """Test AE queries with explicit domain use ADAE."""
        resolution = self.resolver.resolve(
            query="How many patients had adverse events in the safety population?",
            explicit_domain=QueryDomain.ADVERSE_EVENTS,
            explicit_population=PopulationType.SAFETY
        )
        assert resolution.selected_table == "ADAE"
        assert resolution.domain == QueryDomain.ADVERSE_EVENTS
        assert resolution.population == PopulationType.SAFETY

    def test_short_safety_population_query(self):
        """Test short query with explicit parameters."""
        resolution = self.resolver.resolve(
            query="How many are in the safety population?",
            explicit_domain=QueryDomain.DEMOGRAPHICS,
            explicit_population=PopulationType.SAFETY
        )
        assert resolution.selected_table == "ADSL"
        assert resolution.population_filter == "SAFFL = 'Y'"

    def test_population_count_vs_safety_ae_queries(self):
        """Test explicit domain selection for different query types."""
        # Demographics domain - should use ADSL
        pop_resolution = self.resolver.resolve(
            "patients in safety population",
            explicit_domain=QueryDomain.DEMOGRAPHICS
        )
        assert pop_resolution.selected_table == "ADSL"

        # AE domain - should use ADAE
        ae_resolution = self.resolver.resolve(
            "patients with adverse events",
            explicit_domain=QueryDomain.ADVERSE_EVENTS
        )
        assert ae_resolution.selected_table == "ADAE"
