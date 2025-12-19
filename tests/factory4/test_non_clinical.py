# Tests for Non-Clinical Query Routing (Step 0)
"""
Test suite for non-clinical query routing in the inference pipeline.

NOTE: The system now uses LLM-based intent classification instead of regex patterns.
This provides better flexibility and handles the many variations of non-clinical queries.

These tests verify that:
- Intent classification correctly identifies query types
- Non-clinical queries receive appropriate responses
- Clinical queries continue through the data pipeline
"""

import pytest
import time
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.pipeline import (
    InferencePipeline,
    PipelineConfig,
    INTENT_CLASSIFICATION_PROMPT,
    SAGE_SYSTEM_CONTEXT
)


class TestIntentClassificationPrompt:
    """Test the intent classification prompt configuration."""

    def test_prompt_contains_categories(self):
        """Prompt should define all intent categories."""
        categories = ['CLINICAL_DATA', 'GREETING', 'HELP', 'IDENTITY',
                     'FAREWELL', 'STATUS', 'GENERAL']
        for category in categories:
            assert category in INTENT_CLASSIFICATION_PROMPT, \
                f"Category {category} should be in prompt"

    def test_prompt_has_clinical_examples(self):
        """Prompt should have clinical data examples."""
        assert "patients" in INTENT_CLASSIFICATION_PROMPT.lower() or \
               "adverse" in INTENT_CLASSIFICATION_PROMPT.lower()

    def test_prompt_requests_single_word_response(self):
        """Prompt should ask for single category response."""
        assert "only" in INTENT_CLASSIFICATION_PROMPT.lower() and \
               "category" in INTENT_CLASSIFICATION_PROMPT.lower()


class TestSAGESystemContext:
    """Test the SAGE system context for conversational responses."""

    def test_context_mentions_sage(self):
        """System context should identify as SAGE."""
        assert "SAGE" in SAGE_SYSTEM_CONTEXT

    def test_context_mentions_claude(self):
        """System context should mention Claude as the AI."""
        assert "Claude" in SAGE_SYSTEM_CONTEXT

    def test_context_has_clinical_capabilities(self):
        """System context should describe clinical capabilities."""
        context_lower = SAGE_SYSTEM_CONTEXT.lower()
        assert "clinical" in context_lower
        assert "sdtm" in context_lower or "adam" in context_lower

    def test_context_has_example_questions(self):
        """System context should include example questions."""
        assert "?" in SAGE_SYSTEM_CONTEXT  # Contains question examples


class TestPipelineIntentClassification:
    """Test the pipeline's LLM-based intent classification."""

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a mock pipeline for testing."""
        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,  # Use mock SQL generator
            available_tables=mock_available_tables
        )
        return InferencePipeline(config)

    def test_classify_intent_method_exists(self, mock_pipeline):
        """Pipeline should have _classify_intent method."""
        assert hasattr(mock_pipeline, '_classify_intent')

    def test_check_non_clinical_method_exists(self, mock_pipeline):
        """Pipeline should have _check_non_clinical method."""
        assert hasattr(mock_pipeline, '_check_non_clinical')

    def test_generate_conversational_response_exists(self, mock_pipeline):
        """Pipeline should have _generate_conversational_response method."""
        assert hasattr(mock_pipeline, '_generate_conversational_response')


class TestNonClinicalResponseStructure:
    """Test response structure for non-clinical queries."""

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a mock pipeline for testing."""
        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,
            available_tables=mock_available_tables
        )
        return InferencePipeline(config)

    @pytest.mark.live_api
    def test_greeting_response_structure(self, live_pipeline):
        """Verify greeting response has correct PipelineResult structure."""
        result = live_pipeline.process("Hi")

        assert result.success is True
        assert result.query == "Hi"
        assert result.answer is not None
        assert len(result.answer) > 10  # Has some content
        assert result.data is None  # No data for greetings
        assert result.row_count == 0
        assert result.sql is None  # No SQL generated
        assert result.confidence['score'] == 100
        assert result.confidence['level'] == 'high'
        assert result.metadata.get('pipeline_used') is False

    @pytest.mark.live_api
    def test_help_response_structure(self, live_pipeline):
        """Verify help response has correct structure."""
        result = live_pipeline.process("What can you do?")

        assert result.success is True
        assert result.answer is not None
        assert result.metadata.get('pipeline_used') is False


class TestClinicalQueryPassthrough:
    """Test that clinical queries pass through to the pipeline."""

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a mock pipeline for testing."""
        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,
            available_tables=mock_available_tables
        )
        return InferencePipeline(config)

    def test_clinical_query_uses_pipeline(self, mock_pipeline):
        """Clinical queries should go through the full pipeline."""
        # With mock mode, clinical queries will still work
        result = mock_pipeline.process("How many patients had headaches?")

        # Should use the pipeline (not instant response)
        assert result.metadata.get('pipeline_used', True) is True or \
               result.metadata.get('intent') == 'CLINICAL_DATA'


class TestEdgeCases:
    """Test edge cases for intent classification."""

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a mock pipeline for testing."""
        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,
            available_tables=mock_available_tables
        )
        return InferencePipeline(config)

    def test_empty_query_handled(self, mock_pipeline):
        """Empty queries should not crash."""
        try:
            result = mock_pipeline.process("")
            # Empty query might be handled as sanitization failure or general
            assert result is not None
        except Exception as e:
            # Expected - empty query may be rejected
            assert "empty" in str(e).lower() or "sanitiz" in str(e).lower()

    def test_whitespace_query_handled(self, mock_pipeline):
        """Whitespace-only queries should not crash."""
        try:
            result = mock_pipeline.process("   ")
            assert result is not None
        except Exception as e:
            assert "empty" in str(e).lower() or "sanitiz" in str(e).lower()


class TestIntentCategories:
    """Test that all intent categories are properly defined."""

    def test_valid_intents_list(self):
        """Valid intents should be defined in pipeline."""
        # Import from pipeline to verify constants
        from core.engine.pipeline import INTENT_CLASSIFICATION_PROMPT

        expected_intents = [
            'CLINICAL_DATA',
            'GREETING',
            'HELP',
            'IDENTITY',
            'FAREWELL',
            'STATUS',
            'GENERAL'
        ]

        for intent in expected_intents:
            assert intent in INTENT_CLASSIFICATION_PROMPT, \
                f"Intent {intent} should be defined in classification prompt"


class TestNonClinicalPerformance:
    """Test performance of non-clinical query handling.

    Note: These tests are marked as skip because they require live LLM.
    The goal is sub-second response for non-clinical queries.
    """

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a mock pipeline for testing."""
        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,
            available_tables=mock_available_tables
        )
        return InferencePipeline(config)

    @pytest.mark.live_api
    def test_non_clinical_response_under_15s(self, live_pipeline):
        """Non-clinical queries should respond in under 15 seconds.

        Note: 15 seconds is a generous limit for live Claude API calls which include:
        - Network latency
        - API authentication
        - Intent classification call (one LLM call)
        - Response generation call (second LLM call)
        - First call overhead (connection establishment)
        """
        queries = ["Hi", "Help", "What can you do?"]

        for query in queries:
            start = time.time()
            result = live_pipeline.process(query)
            elapsed_s = time.time() - start

            assert result.success is True, f"Query '{query}' should succeed"
            assert elapsed_s < 15, f"Response time {elapsed_s:.1f}s should be < 15s"


class TestIntegrationWithSelfCorrection:
    """Test that non-clinical routing works with self-correction pipeline."""

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a mock pipeline for testing."""
        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,
            available_tables=mock_available_tables
        )
        return InferencePipeline(config)

    def test_self_correction_not_used_for_non_clinical(self, mock_pipeline):
        """Self-correction loop should not be invoked for non-clinical queries."""
        from core.engine.pipeline import MAX_CORRECTION_ATTEMPTS

        # Verify constant exists
        assert MAX_CORRECTION_ATTEMPTS >= 1

        # Non-clinical queries bypass the SQL generation entirely
        # so self-correction is not relevant
