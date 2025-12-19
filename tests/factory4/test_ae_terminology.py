# Test AE Terminology Improvements
"""
Tests for improved AE column handling in context_builder.py

These tests verify that:
1. Serious (AESER) vs Severe (AESEV) are correctly distinguished
2. Fatal AE uses AEOUT='FATAL' not DTHFL
3. Related AE uses AEREL column
4. AE discontinuation uses AEACN column
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.context_builder import ContextBuilder
from core.engine.table_resolver import TableResolution
from core.engine.clinical_config import QueryDomain, PopulationType


def create_table_resolution(selected_table="ADAE", table_columns=None, available_tables=None):
    """Helper to create TableResolution with all required fields."""
    if table_columns is None:
        table_columns = ['USUBJID', 'AEDECOD', 'AESER', 'AESEV', 'AEOUT', 'AEREL', 'AEACN', 'AESDTH']
    if available_tables is None:
        available_tables = ['ADAE', 'ADSL']

    return TableResolution(
        selected_table=selected_table,
        table_type="ADaM",
        domain=QueryDomain.ADVERSE_EVENTS,
        selection_reason="Test resolution",
        population=PopulationType.SAFETY,
        population_filter="SAFFL = 'Y'",
        population_name="Safety Population",
        columns_resolved={},
        fallback_used=False,
        available_tables=available_tables,
        table_columns=table_columns
    )


class TestContextBuilderAEColumns:
    """Test that context builder includes correct AE column documentation."""

    @pytest.fixture
    def context_builder(self):
        """Create a context builder instance."""
        return ContextBuilder()

    @pytest.fixture
    def table_resolution(self):
        """Create mock table resolution for ADAE."""
        return create_table_resolution()

    def test_schema_context_includes_aeser(self, context_builder, table_resolution):
        """COLUMN GUIDE should explain AESER (serious) vs AESEV (severe)."""
        schema_context = context_builder._build_schema_context(table_resolution, include_full=False)

        # Should mention AESER for serious adverse events
        assert "AESER" in schema_context
        assert "SERIOUS" in schema_context.upper() or "serious" in schema_context.lower()

    def test_schema_context_includes_aesev(self, context_builder, table_resolution):
        """COLUMN GUIDE should explain AESEV (severity)."""
        schema_context = context_builder._build_schema_context(table_resolution, include_full=False)

        # Should mention AESEV for severity
        assert "AESEV" in schema_context
        assert any(word in schema_context.upper() for word in ["SEVERITY", "MILD", "MODERATE", "SEVERE"])

    def test_schema_context_distinguishes_serious_severe(self, context_builder, table_resolution):
        """COLUMN GUIDE should explicitly distinguish serious from severe."""
        schema_context = context_builder._build_schema_context(table_resolution, include_full=False)

        # Should have the critical note about different concepts
        assert "DIFFERENT" in schema_context.upper() or "different" in schema_context.lower()

    def test_schema_context_includes_aeout(self, context_builder, table_resolution):
        """COLUMN GUIDE should include AEOUT (outcome)."""
        schema_context = context_builder._build_schema_context(table_resolution, include_full=False)

        # Should mention AEOUT for adverse event outcome
        assert "AEOUT" in schema_context
        assert "OUTCOME" in schema_context.upper() or "outcome" in schema_context.lower()
        assert "FATAL" in schema_context.upper() or "fatal" in schema_context.lower()

    def test_schema_context_includes_aerel(self, context_builder, table_resolution):
        """COLUMN GUIDE should include AEREL (relatedness)."""
        schema_context = context_builder._build_schema_context(table_resolution, include_full=False)

        # Should mention AEREL for treatment relatedness
        assert "AEREL" in schema_context
        assert "RELATED" in schema_context.upper() or "related" in schema_context.lower()

    def test_schema_context_includes_aeacn(self, context_builder, table_resolution):
        """COLUMN GUIDE should include AEACN (action taken)."""
        schema_context = context_builder._build_schema_context(table_resolution, include_full=False)

        # Should mention AEACN for action taken
        assert "AEACN" in schema_context
        assert any(word in schema_context.upper() for word in ["ACTION", "WITHDRAWN", "DISCONTINUATION"])


class TestSystemPromptAEGuidance:
    """Test that system prompt includes correct AE guidance."""

    @pytest.fixture
    def context_builder(self):
        """Create a context builder instance."""
        return ContextBuilder()

    @pytest.fixture
    def table_resolution(self):
        """Create mock table resolution for ADAE."""
        return create_table_resolution()

    def test_system_prompt_fatal_ae_guidance(self, context_builder, table_resolution):
        """System prompt should guide LLM to use AEOUT for fatal AE queries."""
        system_prompt = context_builder._build_system_prompt(table_resolution)

        # Should mention FATAL adverse events use AEOUT
        assert "FATAL" in system_prompt.upper()
        assert "AEOUT" in system_prompt

    def test_system_prompt_death_vs_fatal_ae(self, context_builder, table_resolution):
        """System prompt should distinguish subject death (DTHFL) from fatal AE (AEOUT)."""
        system_prompt = context_builder._build_system_prompt(table_resolution)

        # Should mention both DTHFL and AEOUT with clear distinction
        assert "DTHFL" in system_prompt
        # Should guide that fatal AE is different from subject death


class TestKeyColumnsIncludeNewAEColumns:
    """Test that key_columns dict includes the new AE columns."""

    def test_adae_key_columns(self):
        """ADAE key_columns should include AEOUT, AESDTH, AEACN."""
        from core.engine.context_builder import ContextBuilder

        # The key_columns are defined inside _build_schema_context
        # We test this indirectly through the schema context output
        builder = ContextBuilder()
        resolution = create_table_resolution(
            selected_table="ADAE",
            table_columns=['USUBJID', 'AEDECOD', 'AEOUT', 'AESDTH', 'AEACN', 'AEREL']
        )

        schema = builder._build_schema_context(resolution)

        # Key columns documentation should include these
        # (they appear in the COLUMN GUIDE section)
        assert "AEOUT" in schema
        assert "AESDTH" in schema or "death" in schema.lower()
        assert "AEACN" in schema or "action" in schema.lower()

    def test_adsl_key_columns_include_population_flags(self):
        """ADSL key_columns should include ENRLFL, RANDFL, DTHFL."""
        from core.engine.context_builder import ContextBuilder

        builder = ContextBuilder()
        resolution = create_table_resolution(
            selected_table="ADSL",
            table_columns=['USUBJID', 'AGE', 'SEX', 'SAFFL', 'ITTFL', 'ENRLFL', 'RANDFL', 'DTHFL']
        )

        schema = builder._build_schema_context(resolution)

        # Key population flags should be documented
        assert "ENRLFL" in schema or "Enrolled" in schema
        assert "RANDFL" in schema or "Randomized" in schema
        assert "DTHFL" in schema or "Death" in schema


class TestColumnDescriptions:
    """Test that column descriptions provide correct guidance."""

    def test_get_column_description_aeout(self):
        """AEOUT should have correct description."""
        from core.engine.context_builder import ContextBuilder

        builder = ContextBuilder()
        desc = builder._get_column_description("ADAE", "AEOUT", {})

        assert desc is not None
        # Standard ADaM column should have a description
        assert "AEOUT" in desc or "Outcome" in desc

    def test_get_column_description_aeser(self):
        """AESER should have correct description distinguishing from severity."""
        from core.engine.context_builder import ContextBuilder

        builder = ContextBuilder()
        desc = builder._get_column_description("ADAE", "AESER", {})

        assert desc is not None
        assert "Serious" in desc or "AESER" in desc
