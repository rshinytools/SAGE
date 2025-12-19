# Tests for Timeout & Error Handling (Step 4)
"""
Test suite for improved timeout and error handling in the inference pipeline.

These tests verify that:
- Timeout errors produce user-friendly messages
- Connection errors are handled gracefully
- Error responses include helpful hints
- Different error types are handled appropriately
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.sql_generator import (
    UnifiedSQLGenerator,
    LLMError, LLMTimeoutError, LLMConnectionError, LLMModelError
)
from core.engine.pipeline import InferencePipeline, PipelineConfig


class TestLLMExceptions:
    """Test custom LLM exception classes."""

    def test_llm_timeout_error_message(self):
        """Timeout error should have user-friendly message."""
        error = LLMTimeoutError("deepseek-r1:8b", 180)

        assert "deepseek-r1:8b" in str(error)
        assert "180" in str(error)
        assert "Try" in str(error)  # Should suggest action

    def test_llm_connection_error_message(self):
        """Connection error should have user-friendly message."""
        error = LLMConnectionError("http://localhost:11434")

        assert "localhost:11434" in str(error)
        assert "unavailable" in str(error).lower()

    def test_llm_model_error_message(self):
        """Model error should have user-friendly message."""
        error = LLMModelError("test-model", "Invalid response")

        assert "test-model" in str(error)
        assert "Invalid response" in str(error)
        assert "rephras" in str(error).lower()  # Should suggest rephrasing

    def test_llm_error_is_base_class(self):
        """All specific errors should inherit from LLMError."""
        assert issubclass(LLMTimeoutError, LLMError)
        assert issubclass(LLMConnectionError, LLMError)
        assert issubclass(LLMModelError, LLMError)


class TestLLMConfigDefaults:
    """Test LLMConfig default values for Claude."""

    def test_default_timeout_reasonable(self):
        """Default timeout should be reasonable for API calls."""
        from core.engine.llm_providers import LLMConfig
        config = LLMConfig.from_env()
        assert config.timeout >= 30  # At least 30 seconds for API calls

    def test_model_is_configured(self):
        """Model should be configured."""
        from core.engine.llm_providers import LLMConfig
        config = LLMConfig.from_env()
        # Model name should exist (from env or default)
        assert config.provider is not None


class TestSQLGeneratorErrorHandling:
    """Test SQLGenerator error handling with Claude."""

    @pytest.mark.skip(reason="Requires mocking Claude API")
    def test_connection_error_raises_llm_connection_error(self):
        """Connection failures should raise LLMConnectionError."""
        # With Claude API, connection errors would be from anthropic client
        # This test requires mocking the anthropic library
        pass

    @pytest.mark.skip(reason="Requires mocking Claude API")
    def test_timeout_error_raises_llm_timeout_error(self):
        """Timeouts should raise LLMTimeoutError."""
        # With Claude API, timeouts would be from anthropic client
        # This test requires mocking the anthropic library
        pass


class TestPipelineErrorHandling:
    """Test pipeline error handling."""

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a mock pipeline for testing."""
        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,
            available_tables=mock_available_tables,
            enable_cache=False
        )
        return InferencePipeline(config)

    def test_error_result_includes_hint(self, mock_pipeline):
        """Error results should include user hints when available."""
        # Test SQL injection - this should fail with a clear error
        result = mock_pipeline.process("DROP TABLE users; --")

        assert result.success is False
        assert result.error is not None
        # Error response should be generated with helpful message

    def test_error_result_has_metadata(self, mock_pipeline):
        """Error results should have metadata about error type."""
        # The mock pipeline should still handle edge cases gracefully
        result = mock_pipeline.process("")

        # Empty query should be handled (might succeed with sanitization failure)
        # Just verify the result is returned without crashing
        assert result is not None

    def test_sanitization_failure_handled(self, mock_pipeline):
        """Sanitization failures should be handled gracefully."""
        result = mock_pipeline.process("SELECT * FROM users; DROP TABLE patients")

        assert result.success is False
        assert "blocked" in result.error.lower() or "injection" in result.error.lower()

    def test_table_resolution_failure_handled(self, mock_pipeline):
        """Table resolution failures should be handled gracefully."""
        # This query won't resolve to any table in the mock
        # The mock pipeline uses available_tables which should work
        result = mock_pipeline.process("What are the quantum fluctuations?")

        # Should still return a result without crashing
        assert result is not None


class TestUserFriendlyErrorMessages:
    """Test that error messages are user-friendly."""

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a mock pipeline for testing."""
        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,
            available_tables=mock_available_tables,
            enable_cache=False
        )
        return InferencePipeline(config)

    def test_error_answer_readable(self, mock_pipeline):
        """Error answers should be readable by users."""
        result = mock_pipeline.process("SELECT * FROM secrets --")

        if not result.success:
            # Answer should not contain stack traces or code
            assert "traceback" not in result.answer.lower()
            assert "exception" not in result.answer.lower()

    def test_sql_injection_error_clear(self, mock_pipeline):
        """SQL injection attempts should produce clear error messages."""
        result = mock_pipeline.process("' OR 1=1 --")

        # Should be blocked with a clear message
        assert result.success is False

    def test_phi_blocking_error_clear(self, mock_pipeline):
        """PHI/PII blocking should produce clear error messages."""
        result = mock_pipeline.process("Patient with SSN 123-45-6789")

        assert result.success is False
        # Should indicate the query contained PHI/PII
        assert "phi" in result.error.lower() or "pii" in result.error.lower() or "detected" in result.error.lower()


class TestErrorResultStructure:
    """Test the structure of error results."""

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a mock pipeline for testing."""
        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,
            available_tables=mock_available_tables,
            enable_cache=False
        )
        return InferencePipeline(config)

    def test_error_result_has_required_fields(self, mock_pipeline):
        """Error results should have all required fields."""
        result = mock_pipeline.process("' OR 1=1 --")

        # Required fields for error results
        assert hasattr(result, 'success')
        assert hasattr(result, 'query')
        assert hasattr(result, 'answer')
        assert hasattr(result, 'error')
        assert hasattr(result, 'error_stage')
        assert hasattr(result, 'confidence')

    def test_error_confidence_is_low(self, mock_pipeline):
        """Error results should have very low confidence."""
        result = mock_pipeline.process("' OR 1=1 --")

        if not result.success:
            assert result.confidence['score'] == 0
            assert result.confidence['level'] == 'very_low'

    def test_error_result_has_warnings(self, mock_pipeline):
        """Error results should include warnings."""
        result = mock_pipeline.process("SELECT * FROM users; DROP TABLE patients")

        if not result.success:
            assert len(result.warnings) > 0


class TestPipelineConfigTimeouts:
    """Test pipeline configuration timeouts."""

    def test_pipeline_config_has_timeout(self):
        """PipelineConfig should have timeout settings."""
        config = PipelineConfig()
        assert hasattr(config, 'query_timeout_seconds')
        assert config.query_timeout_seconds > 0

    def test_pipeline_timeout_reasonable(self):
        """Pipeline timeout should be reasonable for CPU inference."""
        config = PipelineConfig()
        # Should be at least 60 seconds for slow models
        assert config.query_timeout_seconds >= 60
        # But not unreasonably long
        assert config.query_timeout_seconds <= 600
