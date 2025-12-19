# Tests for Query Response Cache (Step 2)
"""
Test suite for the query response caching layer.

These tests verify that:
- Query results are properly cached
- Cache hits return results quickly
- Query normalization works correctly
- TTL expiration functions
- Cache statistics are tracked
"""

import pytest
import time
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.cache import QueryCache, get_query_cache, reset_query_cache


class TestQueryCacheBasics:
    """Test basic cache operations."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset global cache before each test."""
        reset_query_cache()

    def test_cache_creation(self):
        """Test cache creation with default settings."""
        cache = QueryCache()
        assert len(cache) == 0
        assert cache.max_size == 1000
        assert cache.default_ttl == 3600

    def test_cache_creation_custom_settings(self):
        """Test cache creation with custom settings."""
        cache = QueryCache(max_size=100, default_ttl=60)
        assert cache.max_size == 100
        assert cache.default_ttl == 60

    def test_set_and_get(self):
        """Test basic set and get operations."""
        cache = QueryCache()

        cache.set("How many patients?", {"answer": 42})
        result = cache.get("How many patients?")

        assert result is not None
        assert result['answer'] == 42

    def test_cache_miss(self):
        """Test cache miss returns None."""
        cache = QueryCache()
        result = cache.get("Unknown query")
        assert result is None

    def test_cache_overwrite(self):
        """Test that setting same query overwrites previous value."""
        cache = QueryCache()

        cache.set("How many?", {"count": 10})
        cache.set("How many?", {"count": 20})

        result = cache.get("How many?")
        assert result['count'] == 20


class TestQueryNormalization:
    """Test query normalization for consistent cache keys."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset global cache before each test."""
        reset_query_cache()

    def test_case_insensitive(self):
        """Queries should be case-insensitive."""
        cache = QueryCache()

        cache.set("How many patients?", {"count": 42})

        # All variations should hit the same cache entry
        assert cache.get("how many patients?") is not None
        assert cache.get("HOW MANY PATIENTS?") is not None
        assert cache.get("How Many Patients?") is not None

    def test_whitespace_normalization(self):
        """Extra whitespace should be normalized."""
        cache = QueryCache()

        cache.set("How many patients?", {"count": 42})

        # Whitespace variations should hit same entry
        assert cache.get("  How many patients?  ") is not None
        assert cache.get("How  many  patients?") is not None
        assert cache.get("How\tmany\npatients?") is not None

    def test_punctuation_normalization(self):
        """Trailing punctuation should be normalized."""
        cache = QueryCache()

        cache.set("How many patients?", {"count": 42})

        # Punctuation variations should hit same entry
        assert cache.get("How many patients") is not None
        assert cache.get("How many patients?") is not None
        assert cache.get("How many patients!") is not None
        assert cache.get("How many patients.") is not None

    def test_different_queries_different_keys(self):
        """Different queries should have different cache keys."""
        cache = QueryCache()

        cache.set("How many patients?", {"count": 100})
        cache.set("How many adverse events?", {"count": 500})

        assert cache.get("How many patients?")['count'] == 100
        assert cache.get("How many adverse events?")['count'] == 500


class TestCacheTTL:
    """Test cache TTL (time-to-live) expiration."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset global cache before each test."""
        reset_query_cache()

    def test_entry_expires_after_ttl(self):
        """Entries should expire after TTL."""
        cache = QueryCache(default_ttl=1)  # 1 second TTL

        cache.set("Short-lived query", {"data": "test"})

        # Should be available immediately
        assert cache.get("Short-lived query") is not None

        # Wait for expiration
        time.sleep(1.1)

        # Should be expired
        assert cache.get("Short-lived query") is None

    def test_custom_ttl_per_entry(self):
        """Individual entries can have custom TTL."""
        cache = QueryCache(default_ttl=3600)

        cache.set("Short-lived", {"data": 1}, ttl=1)
        cache.set("Long-lived", {"data": 2}, ttl=3600)

        time.sleep(1.1)

        assert cache.get("Short-lived") is None  # Expired
        assert cache.get("Long-lived") is not None  # Still valid

    def test_cleanup_expired(self):
        """cleanup_expired should remove all expired entries."""
        cache = QueryCache(default_ttl=1)

        cache.set("Query 1", {"data": 1})
        cache.set("Query 2", {"data": 2})
        cache.set("Query 3", {"data": 3})

        assert len(cache) == 3

        time.sleep(1.1)

        removed = cache.cleanup_expired()

        assert removed == 3
        assert len(cache) == 0


class TestCacheStatistics:
    """Test cache statistics tracking."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset global cache before each test."""
        reset_query_cache()

    def test_hit_count_tracked(self):
        """Cache hits should be counted."""
        cache = QueryCache()

        cache.set("Query", {"data": "test"})

        # Access multiple times
        cache.get("Query")
        cache.get("Query")
        cache.get("Query")

        stats = cache.get_stats()
        assert stats['hits'] == 3
        assert stats['misses'] == 0

    def test_miss_count_tracked(self):
        """Cache misses should be counted."""
        cache = QueryCache()

        cache.get("Unknown 1")
        cache.get("Unknown 2")

        stats = cache.get_stats()
        assert stats['misses'] == 2
        assert stats['hits'] == 0

    def test_hit_rate_calculation(self):
        """Hit rate should be calculated correctly."""
        cache = QueryCache()

        cache.set("Query", {"data": "test"})

        # 3 hits
        cache.get("Query")
        cache.get("Query")
        cache.get("Query")

        # 1 miss
        cache.get("Unknown")

        stats = cache.get_stats()
        assert stats['hit_rate'] == 75.0

    def test_eviction_count_tracked(self):
        """Evictions should be counted."""
        cache = QueryCache(max_size=3)

        cache.set("Query 1", {"data": 1})
        cache.set("Query 2", {"data": 2})
        cache.set("Query 3", {"data": 3})
        cache.set("Query 4", {"data": 4})  # Should trigger eviction

        stats = cache.get_stats()
        assert stats['evictions'] == 1
        assert stats['size'] == 3

    def test_clear_resets_stats(self):
        """Clearing cache should reset statistics."""
        cache = QueryCache()

        cache.set("Query", {"data": "test"})
        cache.get("Query")
        cache.get("Unknown")

        cache.clear()

        stats = cache.get_stats()
        assert stats['hits'] == 0
        assert stats['misses'] == 0
        assert stats['size'] == 0


class TestCacheCapacity:
    """Test cache capacity and eviction."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset global cache before each test."""
        reset_query_cache()

    def test_max_size_respected(self):
        """Cache should not exceed max_size."""
        cache = QueryCache(max_size=5)

        for i in range(10):
            cache.set(f"Query {i}", {"data": i})

        assert len(cache) == 5

    def test_oldest_evicted_first(self):
        """Oldest entries should be evicted first (LRU-style)."""
        cache = QueryCache(max_size=3)

        cache.set("Query 1", {"data": 1})
        time.sleep(0.01)
        cache.set("Query 2", {"data": 2})
        time.sleep(0.01)
        cache.set("Query 3", {"data": 3})
        time.sleep(0.01)
        cache.set("Query 4", {"data": 4})  # Evicts Query 1

        assert cache.get("Query 1") is None  # Evicted
        assert cache.get("Query 2") is not None
        assert cache.get("Query 3") is not None
        assert cache.get("Query 4") is not None


class TestCacheInvalidation:
    """Test cache invalidation."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset global cache before each test."""
        reset_query_cache()

    def test_invalidate_specific_query(self):
        """Should be able to invalidate a specific query."""
        cache = QueryCache()

        cache.set("Query 1", {"data": 1})
        cache.set("Query 2", {"data": 2})

        result = cache.invalidate("Query 1")

        assert result is True
        assert cache.get("Query 1") is None
        assert cache.get("Query 2") is not None

    def test_invalidate_nonexistent(self):
        """Invalidating nonexistent entry should return False."""
        cache = QueryCache()
        result = cache.invalidate("Unknown")
        assert result is False

    def test_clear_all(self):
        """Should be able to clear all entries."""
        cache = QueryCache()

        cache.set("Query 1", {"data": 1})
        cache.set("Query 2", {"data": 2})

        cache.clear()

        assert len(cache) == 0
        assert cache.get("Query 1") is None
        assert cache.get("Query 2") is None


class TestGlobalCache:
    """Test global cache singleton pattern."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset global cache before each test."""
        reset_query_cache()

    def test_get_query_cache_creates_singleton(self):
        """get_query_cache should return same instance."""
        cache1 = get_query_cache()
        cache2 = get_query_cache()

        assert cache1 is cache2

    def test_reset_query_cache(self):
        """reset_query_cache should clear and reset."""
        cache1 = get_query_cache()
        cache1.set("Query", {"data": "test"})

        reset_query_cache()

        cache2 = get_query_cache()
        assert cache2.get("Query") is None

    def test_custom_settings_on_first_call(self):
        """First call to get_query_cache sets configuration."""
        reset_query_cache()

        cache = get_query_cache(max_size=50, default_ttl=120)
        assert cache.max_size == 50
        assert cache.default_ttl == 120


class TestCacheThreadSafety:
    """Test thread safety of cache operations."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset global cache before each test."""
        reset_query_cache()

    def test_concurrent_access(self):
        """Cache should handle concurrent access safely."""
        import threading

        cache = QueryCache()
        errors = []

        def writer():
            try:
                for i in range(100):
                    cache.set(f"Writer query {i}", {"data": i})
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for i in range(100):
                    cache.get(f"Writer query {i}")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=reader),
            threading.Thread(target=writer),
            threading.Thread(target=reader),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0


class TestPipelineCacheIntegration:
    """Test cache integration with the pipeline."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset global cache before each test."""
        reset_query_cache()

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a mock pipeline with caching enabled."""
        from core.engine.pipeline import InferencePipeline, PipelineConfig

        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,
            available_tables=mock_available_tables,
            enable_cache=True,
            cache_ttl_seconds=3600
        )
        return InferencePipeline(config)

    @pytest.fixture
    def mock_pipeline_no_cache(self, mock_available_tables):
        """Create a mock pipeline with caching disabled."""
        from core.engine.pipeline import InferencePipeline, PipelineConfig

        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,
            available_tables=mock_available_tables,
            enable_cache=False
        )
        return InferencePipeline(config)

    def test_pipeline_has_cache(self, mock_pipeline):
        """Pipeline should have cache when enabled."""
        assert mock_pipeline.cache is not None

    def test_pipeline_no_cache_when_disabled(self, mock_pipeline_no_cache):
        """Pipeline should not have cache when disabled."""
        assert mock_pipeline_no_cache.cache is None

    def test_first_query_not_cached(self, mock_pipeline):
        """First query should not be a cache hit."""
        result = mock_pipeline.process("How many patients had headaches?")

        # First query - not from cache
        assert result.metadata.get('cache_hit') is not True

    def test_second_query_cached(self, mock_pipeline):
        """Second identical query should hit cache."""
        # First query
        mock_pipeline.process("How many patients had headaches?")

        # Second query - should hit cache
        result2 = mock_pipeline.process("How many patients had headaches?")

        assert result2.metadata.get('cache_hit') is True

    def test_cache_hit_is_fast(self, mock_pipeline):
        """Cached query should return in under 500ms."""
        # First query - populate cache
        mock_pipeline.process("How many patients had headaches?")

        # Second query - should be fast from cache
        start = time.time()
        result = mock_pipeline.process("How many patients had headaches?")
        elapsed_ms = (time.time() - start) * 1000

        assert result.metadata.get('cache_hit') is True
        assert elapsed_ms < 500, f"Cache hit took {elapsed_ms:.1f}ms, should be < 500ms"

    def test_normalized_queries_hit_same_cache(self, mock_pipeline):
        """Normalized variations should hit same cache entry."""
        # First query
        mock_pipeline.process("How many patients had headaches?")

        # Variations - should all hit cache
        result1 = mock_pipeline.process("how many patients had headaches")
        result2 = mock_pipeline.process("HOW MANY PATIENTS HAD HEADACHES?")
        result3 = mock_pipeline.process("  How many patients had headaches?  ")

        assert result1.metadata.get('cache_hit') is True
        assert result2.metadata.get('cache_hit') is True
        assert result3.metadata.get('cache_hit') is True

    @pytest.mark.live_api
    def test_non_clinical_queries_not_cached(self, live_pipeline):
        """Non-clinical queries should not populate cache.

        NOTE: With LLM-based intent classification, this test requires
        a live Claude API connection to work properly.
        """
        # Non-clinical query
        result = live_pipeline.process("Hi")

        assert result.success is True
        # Non-clinical queries should not use SQL pipeline
        assert result.metadata.get('pipeline_used', True) is False

        # Cache should still be empty (non-clinical not cached)
        if live_pipeline.cache is not None:
            assert len(live_pipeline.cache) == 0

    def test_different_queries_different_cache_entries(self, mock_pipeline):
        """Different queries should create different cache entries."""
        mock_pipeline.process("How many patients had headaches?")
        mock_pipeline.process("How many patients had nausea?")

        assert len(mock_pipeline.cache) == 2
