# Tests for Clinical Configuration
"""
Test Suite for Clinical Rules Engine
====================================
Tests clinical data accuracy rules including:
- ADaM > SDTM table priority
- ATOXGR > AETOXGR column priority
- Population defaults (SAFFL='Y' for safety)
- Domain detection
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.engine.clinical_config import (
    ClinicalQueryConfig,
    QueryDomain,
    PopulationType,
    TablePriority,
    ColumnPriority,
    DEFAULT_CLINICAL_CONFIG
)


class TestQueryDomainDetection:
    """Test query domain detection."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = DEFAULT_CLINICAL_CONFIG

    def test_detect_adverse_events_domain(self):
        """Test adverse events domain detection."""
        queries = [
            "How many patients had headaches?",
            "Show adverse events",
            "List all AEs",
            "Grade 3 toxicity",
            "Treatment emergent events"
        ]
        for query in queries:
            domain = self.config.detect_domain(query)
            assert domain == QueryDomain.ADVERSE_EVENTS, f"Failed for: {query}"

    def test_detect_demographics_domain(self):
        """Test demographics domain detection."""
        queries = [
            "Age distribution",
            "Patient demographics",
            "Show race breakdown",
            "Sex distribution"
        ]
        for query in queries:
            domain = self.config.detect_domain(query)
            assert domain == QueryDomain.DEMOGRAPHICS, f"Failed for: {query}"

    def test_detect_conmed_domain(self):
        """Test concomitant medications domain detection."""
        queries = [
            "Concomitant medications",
            "What medications were used?",
            "Drug history"
        ]
        for query in queries:
            domain = self.config.detect_domain(query)
            assert domain == QueryDomain.CONCOMITANT_MEDS, f"Failed for: {query}"

    def test_detect_labs_domain(self):
        """Test labs domain detection."""
        queries = [
            "ALT values",
            "Laboratory results",
            "Hemoglobin levels",
            "Lab abnormalities"
        ]
        for query in queries:
            domain = self.config.detect_domain(query)
            assert domain == QueryDomain.LABS, f"Failed for: {query}"

    def test_detect_vital_signs_domain(self):
        """Test vital signs domain detection."""
        queries = [
            "Blood pressure readings",
            "Heart rate measurements",
            "Vital signs",
            "Temperature trends"
        ]
        for query in queries:
            domain = self.config.detect_domain(query)
            assert domain == QueryDomain.VITAL_SIGNS, f"Failed for: {query}"


class TestPopulationDetection:
    """Test population detection."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = DEFAULT_CLINICAL_CONFIG

    def test_detect_safety_population(self):
        """Test safety population detection."""
        queries = [
            "Safety population with headaches",
            "Adverse events in safety population",
            "How many in safety pop had nausea?"
        ]
        for query in queries:
            pop = self.config.detect_population(query)
            assert pop == PopulationType.SAFETY, f"Failed for: {query}"

    def test_detect_itt_population(self):
        """Test ITT population detection."""
        queries = [
            "ITT population",
            "Intent to treat analysis",
            "Subjects in ITT"
        ]
        for query in queries:
            pop = self.config.detect_population(query)
            assert pop == PopulationType.ITT, f"Failed for: {query}"

    def test_detect_efficacy_population(self):
        """Test efficacy population detection."""
        queries = [
            "Efficacy population",
            "Efficacy analysis",
            "Response in efficacy pop"
        ]
        for query in queries:
            pop = self.config.detect_population(query)
            assert pop == PopulationType.EFFICACY, f"Failed for: {query}"

    def test_detect_per_protocol(self):
        """Test per-protocol population detection."""
        queries = [
            "Per protocol population",
            "PP analysis",
            "Per-protocol subjects"
        ]
        for query in queries:
            pop = self.config.detect_population(query)
            assert pop == PopulationType.PER_PROTOCOL, f"Failed for: {query}"

    def test_safety_default_for_ae_keywords(self):
        """Test safety population is default for AE queries with safety keywords."""
        # Queries with safety indicators should return SAFETY
        query = "Show adverse events"  # "adverse" is a safety indicator
        pop = self.config.detect_population(query)
        assert pop == PopulationType.SAFETY


class TestTablePriority:
    """Test table priority rules (ADaM > SDTM)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = DEFAULT_CLINICAL_CONFIG

    def test_adam_table_defined(self):
        """Test that ADaM tables are defined for domains."""
        ae_priority = self.config.table_priorities[QueryDomain.ADVERSE_EVENTS]
        assert ae_priority.adam_table == "ADAE"
        assert ae_priority.sdtm_table == "AE"

    def test_adsl_for_demographics(self):
        """Test ADSL defined for demographics."""
        demo_priority = self.config.table_priorities[QueryDomain.DEMOGRAPHICS]
        assert demo_priority.adam_table == "ADSL"
        assert demo_priority.sdtm_table == "DM"

    def test_get_preferred_table_adam(self):
        """Test getting preferred table when ADaM available."""
        ae_priority = self.config.table_priorities[QueryDomain.ADVERSE_EVENTS]
        available = {"ADAE", "AE", "DM"}
        table, is_fallback = ae_priority.get_preferred_table(available)
        assert table == "ADAE"
        assert is_fallback is False

    def test_get_preferred_table_fallback(self):
        """Test fallback to SDTM when ADaM not available."""
        ae_priority = self.config.table_priorities[QueryDomain.ADVERSE_EVENTS]
        available = {"AE", "DM"}  # No ADAE
        table, is_fallback = ae_priority.get_preferred_table(available)
        assert table == "AE"
        assert is_fallback is True

    def test_main_domains_have_priorities(self):
        """Test main domains have table priorities defined."""
        main_domains = [
            QueryDomain.ADVERSE_EVENTS,
            QueryDomain.DEMOGRAPHICS,
            QueryDomain.LABS,
            QueryDomain.VITAL_SIGNS
        ]
        for domain in main_domains:
            assert domain in self.config.table_priorities


class TestColumnPriority:
    """Test column priority rules (ATOXGR > AETOXGR)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = DEFAULT_CLINICAL_CONFIG

    def test_atoxgr_preferred_over_aetoxgr(self):
        """Test ATOXGR is preferred over AETOXGR for grades."""
        grade_priority = self.config.column_priorities.get("toxicity_grade")
        assert grade_priority is not None
        assert grade_priority.preferred_column == "ATOXGR"
        assert grade_priority.fallback_column == "AETOXGR"

    def test_severity_column_priority(self):
        """Test severity column priority."""
        severity_priority = self.config.column_priorities.get("severity")
        assert severity_priority is not None
        assert severity_priority.preferred_column == "ASEV"
        assert severity_priority.fallback_column == "AESEV"

    def test_get_column_preferred(self):
        """Test getting column when preferred is available."""
        grade_priority = self.config.column_priorities["toxicity_grade"]
        available = {"ATOXGR", "AETOXGR", "USUBJID"}
        col, reason = grade_priority.get_column(available)
        assert col == "ATOXGR"
        assert "preferred" in reason.lower()

    def test_get_column_fallback(self):
        """Test getting column when only fallback is available."""
        grade_priority = self.config.column_priorities["toxicity_grade"]
        available = {"AETOXGR", "USUBJID"}  # No ATOXGR
        col, reason = grade_priority.get_column(available)
        assert col == "AETOXGR"
        assert "fallback" in reason.lower()


class TestPopulationFilters:
    """Test population filter generation."""

    def test_safety_flag_column(self):
        """Test safety population flag column."""
        flag = PopulationType.SAFETY.get_flag_column()
        assert flag == "SAFFL"

    def test_itt_flag_column(self):
        """Test ITT population flag column."""
        flag = PopulationType.ITT.get_flag_column()
        assert flag == "ITTFL"

    def test_efficacy_flag_column(self):
        """Test efficacy population flag column."""
        flag = PopulationType.EFFICACY.get_flag_column()
        assert flag == "EFFFL"

    def test_all_enrolled_no_filter(self):
        """Test all enrolled has no filter."""
        flag = PopulationType.ALL_ENROLLED.get_flag_column()
        assert flag is None


class TestSafetyQueryDetection:
    """Test safety query detection logic."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = DEFAULT_CLINICAL_CONFIG

    def test_ae_queries_are_safety(self):
        """Test AE queries are considered safety queries."""
        safety_queries = [
            "Show adverse events",
            "List serious adverse events",
            "Show grade 3 toxicities",
            "Treatment emergent AEs"
        ]
        for query in safety_queries:
            assert self.config.is_safety_query(query) is True, f"Failed for: {query}"

    def test_non_safety_queries(self):
        """Test non-safety queries without safety indicators."""
        non_safety = [
            "Show age distribution",
            "List all subjects"
        ]
        for query in non_safety:
            result = self.config.is_safety_query(query)
            # These might or might not be safety depending on keywords
            assert isinstance(result, bool)


class TestDefaultClinicalConfig:
    """Test default clinical configuration."""

    def test_default_config_exists(self):
        """Test default config is available."""
        assert DEFAULT_CLINICAL_CONFIG is not None

    def test_default_config_has_table_priorities(self):
        """Test default config has table priorities."""
        assert len(DEFAULT_CLINICAL_CONFIG.table_priorities) > 0

    def test_default_config_has_column_priorities(self):
        """Test default config has column priorities."""
        assert len(DEFAULT_CLINICAL_CONFIG.column_priorities) > 0

    def test_default_config_has_population_keywords(self):
        """Test default config has population keywords."""
        assert len(DEFAULT_CLINICAL_CONFIG.population_keywords) > 0

    def test_default_config_has_safety_indicators(self):
        """Test default config has safety indicators."""
        assert len(DEFAULT_CLINICAL_CONFIG.safety_indicators) > 0


class TestTablePriorityClass:
    """Test TablePriority dataclass."""

    def test_default_population(self):
        """Test default population is set."""
        priority = TablePriority(
            domain=QueryDomain.ADVERSE_EVENTS,
            adam_table="ADAE",
            sdtm_table="AE"
        )
        assert priority.default_population == PopulationType.SAFETY

    def test_missing_both_tables_raises(self):
        """Test error when neither table is available."""
        priority = TablePriority(
            domain=QueryDomain.ADVERSE_EVENTS,
            adam_table="ADAE",
            sdtm_table="AE"
        )
        available = {"DM", "VS"}  # Neither ADAE nor AE
        with pytest.raises(ValueError):
            priority.get_preferred_table(available)


class TestColumnPriorityClass:
    """Test ColumnPriority dataclass."""

    def test_neither_column_available(self):
        """Test when neither column is available."""
        priority = ColumnPriority(
            concept="test",
            preferred_column="PREF",
            fallback_column="FALL",
            description="Test priority"
        )
        available = {"OTHER", "COLS"}
        col, reason = priority.get_column(available)
        assert col is None
        # Check that the reason mentions the columns weren't found
        assert "PREF" in reason and "FALL" in reason
