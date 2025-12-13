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
        """Test ADAE is selected for adverse event queries."""
        resolution = self.resolver.resolve(
            query="How many patients had headaches?"
        )
        assert resolution.selected_table == "ADAE"
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

        resolution = resolver.resolve(
            query="How many patients had headaches?"
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
        """Test safety population is default for AE queries."""
        resolution = self.resolver.resolve(
            query="How many patients had nausea?"
        )
        assert resolution.population_filter == "SAFFL = 'Y'"
        assert resolution.population == PopulationType.SAFETY

    def test_explicit_itt_population(self):
        """Test explicit ITT population request."""
        resolution = self.resolver.resolve(
            query="How many patients in ITT population had nausea?"
        )
        # Should detect ITT from query
        assert resolution.population == PopulationType.ITT

    def test_explicit_safety_population(self):
        """Test explicit safety population request."""
        resolution = self.resolver.resolve(
            query="Show adverse events in safety population"
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
        resolution = self.resolver.resolve(
            query="Show grade 3 adverse events"
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
            query="Show grade 3 adverse events"
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
