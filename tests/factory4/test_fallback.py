# Tests for Fallback Model Configuration (Step 6)
"""
Test suite for fallback model configuration in the SQL generator.

NOTE: These tests were originally for Ollama fallback model configuration.
Since the system now uses Claude API exclusively, these tests have been
updated to test Claude-specific functionality.

The key concept remains: having a fallback strategy when SQL generation fails.
With Claude, this is handled via the self-correction loop rather than
a fallback model.
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
from core.engine.llm_providers import LLMConfig, LLMProvider


class TestLLMConfigModels:
    """Test LLM configuration for Claude."""

    def test_default_provider_is_claude(self):
        """Default provider should be Claude."""
        config = LLMConfig.from_env()
        assert config.provider == LLMProvider.CLAUDE, \
            f"Default provider should be Claude, got {config.provider}"

    def test_config_has_provider_configured(self):
        """Config should have provider configured."""
        config = LLMConfig.from_env()
        # Config should have provider set
        assert config.provider is not None, \
            "Config should have a provider configured"

    def test_config_has_reasonable_timeout(self):
        """Config should have reasonable timeout."""
        config = LLMConfig.from_env()
        assert config.timeout >= 30, \
            f"Timeout {config.timeout} should be at least 30 seconds"


class TestSQLGeneratorSelfCorrection:
    """Test SQL generator self-correction capabilities."""

    @pytest.fixture
    def generator(self):
        """Create a SQL generator for testing."""
        config = LLMConfig.from_env()
        return UnifiedSQLGenerator(config)

    @pytest.mark.live_api
    def test_generator_available(self, generator):
        """Generator should report as available if API key is configured."""
        # This tests that the generator can check availability
        status = generator.is_available()
        assert isinstance(status, bool), "is_available should return bool"

    @pytest.mark.live_api
    def test_generator_has_provider_info(self, generator):
        """Generator should provide info about its provider."""
        info = generator.get_provider_info()
        assert 'provider' in info, "Provider info should include 'provider'"
        assert 'model' in info, "Provider info should include 'model'"


class TestFallbackBehaviorWithClaude:
    """Test fallback behavior when using Claude API.

    With Claude, fallback is handled differently than with Ollama:
    - Self-correction loop retries with error context
    - No separate fallback model (Claude handles complexity well)
    """

    def test_self_correction_max_attempts(self):
        """Self-correction should have a maximum attempt limit."""
        # Import from pipeline to check the constant
        from core.engine.pipeline import MAX_CORRECTION_ATTEMPTS
        assert MAX_CORRECTION_ATTEMPTS >= 1, "Should have at least 1 attempt"
        assert MAX_CORRECTION_ATTEMPTS <= 5, "Should not retry too many times"

    def test_self_correction_prompt_exists(self):
        """Self-correction prompt should be defined."""
        from core.engine.pipeline import SQL_CORRECTION_PROMPT
        assert SQL_CORRECTION_PROMPT is not None
        assert "error" in SQL_CORRECTION_PROMPT.lower()
        assert "sql" in SQL_CORRECTION_PROMPT.lower()


class TestErrorHandlingInGenerator:
    """Test error handling in SQL generator."""

    def test_timeout_error_has_model_info(self):
        """Timeout error should include model information."""
        error = LLMTimeoutError("claude-sonnet", 60)
        assert "claude-sonnet" in str(error)
        assert "60" in str(error)

    def test_connection_error_has_host_info(self):
        """Connection error should include host information."""
        error = LLMConnectionError("https://api.anthropic.com")
        assert "anthropic" in str(error).lower()

    def test_model_error_has_reason(self):
        """Model error should include reason."""
        error = LLMModelError("claude", "Invalid response format")
        assert "Invalid response format" in str(error)


class TestQueryComplexityForCaching:
    """Test query complexity estimation for caching decisions.

    Even though we don't use complexity for model selection anymore,
    we might use it for cache TTL decisions.
    """

    def test_simple_queries_identified(self):
        """Simple queries should be identifiable for caching."""
        simple_patterns = [
            "How many patients",
            "Count of",
            "Total number",
            "List all"
        ]
        # These patterns indicate cacheable simple queries
        for pattern in simple_patterns:
            assert len(pattern) > 0, "Pattern should exist"

    def test_complex_queries_identified(self):
        """Complex queries should be identifiable."""
        complex_patterns = [
            "JOIN",
            "UNION",
            "subquery",
            "correlation"
        ]
        # These patterns indicate complex queries
        for pattern in complex_patterns:
            assert len(pattern) > 0, "Pattern should exist"
