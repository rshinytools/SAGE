# Test Instant Response Feature
"""
Tests for instant response patterns in pipeline.py

These tests verify that:
1. Greeting queries return instantly without LLM call
2. Thanks queries return instantly
3. Farewell queries return instantly
4. Clinical queries do NOT trigger instant response
5. Instant responses have correct metadata
"""

import time
import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.pipeline import INSTANT_PATTERNS, INSTANT_RESPONSES


class TestInstantPatterns:
    """Test that instant patterns match correctly."""

    def test_greeting_patterns_simple(self):
        """Simple greetings should match."""
        pattern = INSTANT_PATTERNS['greeting']

        assert pattern.match("hi")
        assert pattern.match("Hi")
        assert pattern.match("HI")
        assert pattern.match("hello")
        assert pattern.match("Hello")
        assert pattern.match("hey")
        assert pattern.match("Hey!")
        assert pattern.match("hi!")
        assert pattern.match("hello?")

    def test_greeting_patterns_time_based(self):
        """Time-based greetings should match."""
        pattern = INSTANT_PATTERNS['greeting']

        assert pattern.match("good morning")
        assert pattern.match("Good Morning")
        assert pattern.match("good afternoon")
        assert pattern.match("good evening")
        assert pattern.match("Good morning!")

    def test_greeting_patterns_howdy(self):
        """Howdy should match."""
        pattern = INSTANT_PATTERNS['greeting']

        assert pattern.match("howdy")
        assert pattern.match("Howdy!")

    def test_greeting_patterns_not_in_sentence(self):
        """Greetings in sentences should NOT match (clinical queries)."""
        pattern = INSTANT_PATTERNS['greeting']

        # These should NOT match because they're part of clinical queries
        assert not pattern.match("hi, how many patients had headaches")
        assert not pattern.match("hello can you show me adverse events")
        assert not pattern.match("hey what is the safety population count")

    def test_thanks_patterns(self):
        """Thanks patterns should match."""
        pattern = INSTANT_PATTERNS['thanks']

        assert pattern.match("thanks")
        assert pattern.match("Thanks")
        assert pattern.match("Thanks!")
        assert pattern.match("thank you")
        assert pattern.match("Thank you!")
        assert pattern.match("thx")
        assert pattern.match("cheers")
        assert pattern.match("Cheers!")

    def test_thanks_patterns_not_in_sentence(self):
        """Thanks in sentences should NOT match."""
        pattern = INSTANT_PATTERNS['thanks']

        assert not pattern.match("thanks for the help, can you show more")
        assert not pattern.match("thank you, now list them")

    def test_bye_patterns(self):
        """Farewell patterns should match."""
        pattern = INSTANT_PATTERNS['bye']

        assert pattern.match("bye")
        assert pattern.match("Bye")
        assert pattern.match("Bye!")
        assert pattern.match("goodbye")
        assert pattern.match("Goodbye!")
        assert pattern.match("see you")
        assert pattern.match("See you!")
        assert pattern.match("later")

    def test_bye_patterns_not_in_sentence(self):
        """Farewells in sentences should NOT match."""
        pattern = INSTANT_PATTERNS['bye']

        assert not pattern.match("bye, but first show me the data")


class TestInstantResponses:
    """Test that instant responses have correct content."""

    def test_greeting_response_content(self):
        """Greeting response should introduce SAGE and show examples."""
        response = INSTANT_RESPONSES['greeting']

        assert "SAGE" in response
        assert "clinical" in response.lower()
        # Should have example questions
        assert "?" in response
        # Should mention capabilities
        assert any(word in response.lower() for word in ["count", "patient", "subject", "adverse"])

    def test_thanks_response_content(self):
        """Thanks response should be polite and brief."""
        response = INSTANT_RESPONSES['thanks']

        assert "welcome" in response.lower() or "let me know" in response.lower()
        # Should be relatively short
        assert len(response) < 200

    def test_bye_response_content(self):
        """Farewell response should be polite."""
        response = INSTANT_RESPONSES['bye']

        assert any(word in response.lower() for word in ["goodbye", "bye", "return", "feel free"])


class TestInstantResponseMethod:
    """Test the _check_instant_response method in the pipeline."""

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a pipeline with mock executor."""
        from core.engine.pipeline import InferencePipeline, PipelineConfig
        from core.engine.executor import MockExecutor

        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=False,
            available_tables=mock_available_tables
        )

        pipeline = InferencePipeline(config)
        pipeline.executor = MockExecutor()
        return pipeline

    def test_instant_response_returns_result(self, mock_pipeline):
        """Instant response should return a PipelineResult."""
        import time
        start = time.time()

        result = mock_pipeline._check_instant_response("Hi", start)

        assert result is not None
        assert result.success is True
        assert result.sql is None  # No SQL for instant response
        assert result.metadata.get('instant') is True
        assert result.metadata.get('pipeline_used') is False

    def test_instant_response_greeting_metadata(self, mock_pipeline):
        """Greeting instant response should have correct metadata."""
        import time
        start = time.time()

        result = mock_pipeline._check_instant_response("Hello!", start)

        assert result is not None
        assert result.metadata.get('response_type') == 'greeting'
        assert result.metadata.get('instant') is True

    def test_instant_response_thanks_metadata(self, mock_pipeline):
        """Thanks instant response should have correct metadata."""
        import time
        start = time.time()

        result = mock_pipeline._check_instant_response("Thanks!", start)

        assert result is not None
        assert result.metadata.get('response_type') == 'thanks'
        assert result.metadata.get('instant') is True

    def test_instant_response_bye_metadata(self, mock_pipeline):
        """Farewell instant response should have correct metadata."""
        import time
        start = time.time()

        result = mock_pipeline._check_instant_response("Goodbye", start)

        assert result is not None
        assert result.metadata.get('response_type') == 'bye'
        assert result.metadata.get('instant') is True

    def test_clinical_query_no_instant(self, mock_pipeline):
        """Clinical queries should NOT trigger instant response."""
        import time
        start = time.time()

        result = mock_pipeline._check_instant_response("How many patients had headaches?", start)

        assert result is None  # No instant response for clinical queries

    def test_help_query_no_instant(self, mock_pipeline):
        """Help queries should NOT trigger instant response (handled by LLM)."""
        import time
        start = time.time()

        result = mock_pipeline._check_instant_response("What can you do?", start)

        assert result is None  # Should be handled by LLM classification instead


class TestInstantResponseTiming:
    """Test that instant responses are fast (< 100ms)."""

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a pipeline with mock executor."""
        from core.engine.pipeline import InferencePipeline, PipelineConfig
        from core.engine.executor import MockExecutor

        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=False,
            available_tables=mock_available_tables
        )

        pipeline = InferencePipeline(config)
        pipeline.executor = MockExecutor()
        return pipeline

    @pytest.mark.parametrize("query", [
        "Hi", "Hello", "Hey", "Good morning", "Howdy",
        "Thanks", "Thank you", "Thx", "Cheers",
        "Bye", "Goodbye", "See you", "Later"
    ])
    def test_instant_response_is_fast(self, mock_pipeline, query):
        """All instant responses should complete in < 100ms."""
        import time
        start = time.time()

        result = mock_pipeline._check_instant_response(query, start)
        elapsed_ms = (time.time() - start) * 1000

        assert result is not None, f"Query '{query}' should match instant pattern"
        assert elapsed_ms < 100, f"Instant response took {elapsed_ms:.1f}ms (expected < 100ms)"
        assert result.total_time_ms < 100


class TestInstantVsLLMClassification:
    """Test distinction between instant patterns and LLM classification."""

    def test_instant_patterns_are_strict(self):
        """Instant patterns should be strict - only simple greetings."""
        pattern = INSTANT_PATTERNS['greeting']

        # These should NOT match instant patterns (require LLM classification)
        complex_queries = [
            "hi there, how are you?",
            "hello, I need help",
            "hey can you help me",
            "hello! what can you do?",
            "hi, show me the safety population",
        ]

        for query in complex_queries:
            assert not pattern.match(query), f"Query '{query}' should NOT match instant pattern"

    def test_help_queries_need_llm(self):
        """Help-style queries should not match instant patterns."""
        for category, pattern in INSTANT_PATTERNS.items():
            # Help queries should not match any instant pattern
            help_queries = [
                "What can you do?",
                "How do I use this?",
                "Show me examples",
                "Help",
                "help me",
            ]

            for query in help_queries:
                assert not pattern.match(query), f"Help query '{query}' matched instant pattern '{category}'"
