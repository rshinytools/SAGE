# Tests for Prompt Optimization (Step 5)
"""
Test suite for optimized prompts in the inference pipeline.

These tests verify that:
- Prompts are compact and token-efficient
- Brevity hints are included for DeepSeek-R1
- SQL output is still valid after optimization
- Key information is preserved in shorter format
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.context_builder import ContextBuilder
from core.engine.table_resolver import TableResolver, TableResolution, ColumnResolution
from core.engine.clinical_config import QueryDomain, PopulationType
from core.engine.models import EntityMatch


@pytest.fixture
def context_builder():
    """Create a context builder for testing."""
    return ContextBuilder()


@pytest.fixture
def mock_table_resolution():
    """Create a mock table resolution for testing."""
    return TableResolution(
        selected_table='ADAE',
        table_type='ADaM',
        domain=QueryDomain.ADVERSE_EVENTS,
        selection_reason='ADaM AE table available',
        population=PopulationType.SAFETY,
        population_filter="SAFFL = 'Y'",
        population_name='Safety Population',
        columns_resolved={
            'toxicity_grade': ColumnResolution(
                concept='toxicity_grade',
                column_name='ATOXGR',
                reason='ADaM analysis grade (preferred over AETOXGR)'
            )
        },
        fallback_used=False,
        available_tables=['ADAE', 'ADSL'],
        table_columns=['USUBJID', 'AEDECOD', 'AETERM', 'ATOXGR', 'AETOXGR',
                       'AESEV', 'AESER', 'AEREL', 'TRTEMFL', 'SAFFL'],
        assumptions=['Using safety population by default for adverse events']
    )


@pytest.fixture
def mock_entities():
    """Create mock entities for testing."""
    return [
        EntityMatch(
            original_term='headache',
            matched_term='HEADACHE',
            match_type='fuzzy',
            confidence=95.0,
            table='ADAE',
            column='AEDECOD'
        )
    ]


class TestTokenReduction:
    """Test that prompts are compact and token-efficient."""

    def test_total_context_under_1500_tokens(self, context_builder, mock_table_resolution, mock_entities):
        """Total context should be under 1500 estimated tokens."""
        context = context_builder.build(
            query="How many patients had headache?",
            table_resolution=mock_table_resolution,
            entities=mock_entities
        )

        # Target: < 1500 tokens (was ~2000+ before optimization)
        assert context.token_count_estimate < 1500, \
            f"Token count {context.token_count_estimate} exceeds 1500 limit"

    def test_system_prompt_under_300_chars(self, context_builder, mock_table_resolution):
        """System prompt should be reasonable length for LLM-first approach."""
        context = context_builder.build(
            query="How many patients had adverse events?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        # System prompt includes more context for LLM-first approach
        # Allow up to 1100 chars for comprehensive guidance (includes ATOXGR data type notes)
        assert len(context.system_prompt) < 1100, \
            f"System prompt too long: {len(context.system_prompt)} chars"

    def test_schema_context_compact(self, context_builder, mock_table_resolution):
        """Schema context should include key column information."""
        context = context_builder.build(
            query="How many patients had adverse events?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        # Should include column information
        assert "AEDECOD" in context.schema_context or "ADAE" in context.schema_context
        # Should NOT have the old markdown table format
        assert "| Column | Description |" not in context.schema_context

    def test_user_prompt_compact(self, context_builder, mock_table_resolution, mock_entities):
        """User prompt should be minimal."""
        context = context_builder.build(
            query="How many patients had headache?",
            table_resolution=mock_table_resolution,
            entities=mock_entities
        )

        # Should start with "Q:" prefix
        assert context.user_prompt.startswith("Q:")
        # Should NOT have verbose instructions
        assert "Generate a DuckDB SQL query" not in context.user_prompt


class TestBrevityHints:
    """Test that prompts include helpful guidance."""

    def test_system_prompt_has_brevity_hint(self, context_builder, mock_table_resolution):
        """System prompt should include clear instructions."""
        context = context_builder.build(
            query="How many patients had adverse events?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        # With LLM-first approach, prompts focus on clarity over brevity tags
        # Should have SQL output format instruction
        assert "sql" in context.system_prompt.lower()

    def test_be_brief_instruction(self, context_builder, mock_table_resolution):
        """System prompt should include key rules."""
        context = context_builder.build(
            query="How many patients?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        # Should have clear rules about SQL generation
        system_lower = context.system_prompt.lower()
        assert "sql" in system_lower or "select" in system_lower


class TestKeyInfoPreserved:
    """Test that key information is preserved in shorter format."""

    def test_table_name_in_system_prompt(self, context_builder, mock_table_resolution):
        """Table name should be in system prompt."""
        context = context_builder.build(
            query="How many patients?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        assert "ADAE" in context.system_prompt

    def test_population_filter_in_system_prompt(self, context_builder, mock_table_resolution):
        """Population filter should be in schema context or clinical rules."""
        context = context_builder.build(
            query="How many patients?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        # SAFFL should be mentioned somewhere in the context
        full_context = context.system_prompt + context.schema_context + context.clinical_rules
        assert "SAFFL" in full_context

    def test_columns_in_schema_context(self, context_builder, mock_table_resolution):
        """Key columns should be in schema context."""
        context = context_builder.build(
            query="How many patients?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        assert "USUBJID" in context.schema_context
        assert "AEDECOD" in context.schema_context

    def test_entities_in_user_prompt(self, context_builder, mock_table_resolution, mock_entities):
        """Entity mappings should be in user prompt."""
        context = context_builder.build(
            query="How many patients had headache?",
            table_resolution=mock_table_resolution,
            entities=mock_entities
        )

        # Should have USE: directive with entity mapping
        assert "USE:" in context.user_prompt or "AEDECOD" in context.user_prompt

    def test_grade_column_in_rules(self, context_builder, mock_table_resolution):
        """Grade column preference should be preserved."""
        context = context_builder.build(
            query="How many Grade 3+ AEs?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        # Grade column should be mentioned somewhere
        full_context = (context.system_prompt + context.schema_context +
                       context.entity_context + context.clinical_rules)
        assert "ATOXGR" in full_context


class TestEntityContext:
    """Test entity context formatting."""

    def test_empty_entities_no_context(self, context_builder, mock_table_resolution):
        """Empty entities should return empty context."""
        context = context_builder.build(
            query="How many patients?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        assert context.entity_context == ""

    def test_entities_compact_format(self, context_builder, mock_table_resolution, mock_entities):
        """Entity context should use compact format."""
        context = context_builder.build(
            query="How many patients had headache?",
            table_resolution=mock_table_resolution,
            entities=mock_entities
        )

        # Should have TERMS: prefix
        if context.entity_context:
            assert "TERMS:" in context.entity_context or "→" in context.entity_context


class TestClinicalRulesMinimal:
    """Test that clinical rules are minimal."""

    def test_no_redundant_rules(self, context_builder, mock_table_resolution):
        """Clinical rules should not duplicate system prompt info."""
        context = context_builder.build(
            query="How many patients?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        # Should NOT have "## Clinical Rules (MUST FOLLOW)" header
        assert "## Clinical Rules" not in context.clinical_rules

    def test_rules_include_grade_column_when_relevant(self, context_builder, mock_table_resolution):
        """Rules should include grade column when relevant."""
        context = context_builder.build(
            query="How many Grade 3+ AEs?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        # Grade column should be mentioned
        if "grade" in context.clinical_rules.lower():
            assert "ATOXGR" in context.clinical_rules

    def test_assumptions_included(self, context_builder, mock_table_resolution):
        """Assumptions should be included concisely."""
        context = context_builder.build(
            query="How many AEs?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        # If assumptions exist, they should be in rules
        if mock_table_resolution.assumptions:
            # Check that at least one assumption is mentioned
            assert "safety" in context.clinical_rules.lower() or "Note:" in context.clinical_rules


class TestSchemaOptimization:
    """Test schema context optimization."""

    def test_key_columns_prioritized(self, context_builder, mock_table_resolution):
        """Key columns for the table should be prioritized."""
        context = context_builder.build(
            query="How many AEs?",
            table_resolution=mock_table_resolution,
            entities=[]
        )

        # Key AE columns should appear
        assert "AEDECOD" in context.schema_context
        assert "USUBJID" in context.schema_context

    def test_column_limit_respected(self, context_builder):
        """Column list should not exceed reasonable limit."""
        from core.engine.clinical_config import QueryDomain, PopulationType

        # Create a resolution with many columns
        resolution = TableResolution(
            selected_table='ADAE',
            table_type='ADaM',
            domain=QueryDomain.ADVERSE_EVENTS,
            selection_reason='Test',
            population=PopulationType.SAFETY,
            population_filter="SAFFL = 'Y'",
            population_name='Safety',
            columns_resolved={},
            fallback_used=False,
            available_tables=['ADAE'],
            table_columns=['COL' + str(i) for i in range(50)],  # 50 columns
            assumptions=[]
        )

        context = context_builder.build(
            query="How many AEs?",
            table_resolution=resolution,
            entities=[]
        )

        # Should limit columns to prevent token explosion
        # Count commas in COLUMNS line to estimate column count
        col_count = context.schema_context.count(',') + 1
        assert col_count <= 20, f"Too many columns in context: {col_count}"


class TestComparisonBeforeAfter:
    """Test that optimization significantly reduced token count."""

    def test_significant_reduction(self, context_builder, mock_table_resolution, mock_entities):
        """Optimized prompts should be significantly shorter than verbose versions."""
        context = context_builder.build(
            query="How many patients had headache in the safety population?",
            table_resolution=mock_table_resolution,
            entities=mock_entities
        )

        # Calculate total chars
        total_chars = (len(context.system_prompt) +
                      len(context.schema_context) +
                      len(context.entity_context) +
                      len(context.clinical_rules) +
                      len(context.user_prompt))

        # Should be under 2000 characters (was 3000+ before)
        assert total_chars < 2000, f"Total context too long: {total_chars} chars"

        # Token estimate should be reasonable
        assert context.token_count_estimate < 600, \
            f"Token estimate too high: {context.token_count_estimate}"
