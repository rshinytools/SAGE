# Test Cache Enhancements
"""
Tests for enhanced caching features in cache.py

These tests verify that:
1. get_detailed_stats() returns comprehensive stats
2. Cache normalization works correctly
3. Cache TTL expiration works
4. Cache hit rate tracking is accurate
5. Data version tracking works
"""

import time
import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.cache import QueryCache, get_query_cache, reset_query_cache


class TestCacheBasics:
    """Test basic cache operations."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset cache before each test."""
        reset_query_cache()
        yield
        reset_query_cache()

    def test_cache_set_and_get(self):
        """Basic set and get should work."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        cache.set("How many patients?", {"answer": 42, "row_count": 1})
        result = cache.get("How many patients?")

        assert result is not None
        assert result["answer"] == 42

    def test_cache_miss(self):
        """Cache miss should return None."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        result = cache.get("nonexistent query")

        assert result is None

    def test_cache_normalization(self):
        """Similar queries should hit the same cache entry."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        cache.set("how many patients had nausea", {"answer": 42})

        # These should all hit the same entry
        assert cache.get("How many patients had nausea?") is not None
        assert cache.get("HOW MANY PATIENTS HAD NAUSEA") is not None
        assert cache.get("  how many patients had nausea  ") is not None
        assert cache.get("how many patients had nausea???") is not None

    def test_cache_clear(self):
        """Cache clear should remove all entries."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        cache.set("query1", {"a": 1})
        cache.set("query2", {"b": 2})
        cache.set("query3", {"c": 3})

        assert len(cache) == 3

        cache.clear()

        assert len(cache) == 0
        assert cache.get("query1") is None


class TestCacheStats:
    """Test cache statistics tracking."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset cache before each test."""
        reset_query_cache()
        yield
        reset_query_cache()

    def test_stats_tracking_hits_and_misses(self):
        """Stats should track hits and misses correctly."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        # 2 misses
        cache.get("query1")
        cache.get("query2")

        # Set one
        cache.set("query1", {"answer": "test"})

        # 1 hit
        cache.get("query1")

        stats = cache.get_stats()

        assert stats['hits'] == 1
        assert stats['misses'] == 2
        assert stats['hit_rate'] == pytest.approx(33.3, rel=0.1)

    def test_stats_hit_rate_calculation(self):
        """Hit rate should be calculated correctly."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        # Set 2 queries
        cache.set("query1", {"a": 1})
        cache.set("query2", {"b": 2})

        # Hit both twice
        cache.get("query1")
        cache.get("query1")
        cache.get("query2")
        cache.get("query2")

        # Miss once
        cache.get("query3")

        stats = cache.get_stats()

        # 4 hits, 1 miss = 80% hit rate
        assert stats['hits'] == 4
        assert stats['misses'] == 1
        assert stats['hit_rate'] == 80.0

    def test_get_stats_includes_size(self):
        """Stats should include cache size."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        cache.set("query1", {"a": 1})
        cache.set("query2", {"b": 2})

        stats = cache.get_stats()

        assert stats['size'] == 2
        assert stats['max_size'] == 100


class TestCacheDetailedStats:
    """Test the new get_detailed_stats() method."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset cache before each test."""
        reset_query_cache()
        yield
        reset_query_cache()

    def test_detailed_stats_includes_age_info(self):
        """Detailed stats should include entry age info."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        cache.set("query1", {"a": 1})
        time.sleep(0.1)  # Small delay to get measurable age
        cache.set("query2", {"b": 2})

        stats = cache.get_detailed_stats()

        assert 'oldest_entry_age_seconds' in stats
        assert 'newest_entry_age_seconds' in stats
        assert 'avg_entry_age_seconds' in stats
        assert stats['oldest_entry_age_seconds'] >= stats['newest_entry_age_seconds']

    def test_detailed_stats_includes_ttl(self):
        """Detailed stats should include TTL info."""
        cache = QueryCache(max_size=100, default_ttl=7200)

        stats = cache.get_detailed_stats()

        assert 'default_ttl_seconds' in stats
        assert stats['default_ttl_seconds'] == 7200

    def test_detailed_stats_includes_hit_rate_string(self):
        """Detailed stats should include formatted hit rate."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        cache.set("query1", {"a": 1})
        cache.get("query1")  # 1 hit
        cache.get("query2")  # 1 miss

        stats = cache.get_detailed_stats()

        assert 'hit_rate' in stats
        assert "%" in stats['hit_rate']

    def test_detailed_stats_empty_cache(self):
        """Detailed stats should handle empty cache gracefully."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        stats = cache.get_detailed_stats()

        assert stats['size'] == 0
        assert stats['oldest_entry_age_seconds'] == 0
        assert stats['newest_entry_age_seconds'] == 0
        assert stats['avg_entry_age_seconds'] == 0

    def test_detailed_stats_includes_evictions(self):
        """Detailed stats should track evictions."""
        cache = QueryCache(max_size=3, default_ttl=3600)

        # Fill cache past capacity
        cache.set("query1", {"a": 1})
        cache.set("query2", {"b": 2})
        cache.set("query3", {"c": 3})
        cache.set("query4", {"d": 4})  # Should trigger eviction

        stats = cache.get_detailed_stats()

        assert stats['evictions'] >= 1


class TestCacheTTL:
    """Test TTL expiration."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset cache before each test."""
        reset_query_cache()
        yield
        reset_query_cache()

    def test_expired_entry_not_returned(self):
        """Expired entries should not be returned."""
        cache = QueryCache(max_size=100, default_ttl=1)  # 1 second TTL

        cache.set("query", {"answer": "test"})

        # Should be available immediately
        assert cache.get("query") is not None

        # Wait for expiration
        time.sleep(1.5)

        # Should be expired
        assert cache.get("query") is None

    def test_expiration_tracked_in_stats(self):
        """Expirations should be tracked in stats."""
        cache = QueryCache(max_size=100, default_ttl=1)

        cache.set("query", {"answer": "test"})
        time.sleep(1.5)
        cache.get("query")  # This triggers expiration check

        stats = cache.get_stats()

        assert stats['expirations'] >= 1


class TestCacheNormalization:
    """Test query normalization for cache keys."""

    def test_normalize_lowercase(self):
        """Normalization should convert to lowercase."""
        cache = QueryCache()
        normalized = cache._normalize("HOW MANY PATIENTS")
        assert normalized == "how many patients"

    def test_normalize_strip_whitespace(self):
        """Normalization should strip whitespace."""
        cache = QueryCache()
        normalized = cache._normalize("  query with spaces  ")
        assert normalized == "query with spaces"

    def test_normalize_collapse_whitespace(self):
        """Normalization should collapse multiple spaces."""
        cache = QueryCache()
        normalized = cache._normalize("query   with   multiple   spaces")
        assert normalized == "query with multiple spaces"

    def test_normalize_remove_trailing_punctuation(self):
        """Normalization should remove trailing punctuation."""
        cache = QueryCache()

        assert cache._normalize("query?") == "query"
        assert cache._normalize("query!") == "query"
        assert cache._normalize("query.") == "query"
        assert cache._normalize("query???") == "query"

    def test_normalize_preserve_internal_punctuation(self):
        """Normalization should preserve internal punctuation."""
        cache = QueryCache()

        # Hyphens and apostrophes should be preserved
        normalized = cache._normalize("what's the count of grade-3 events?")
        assert "what's" in normalized
        assert "grade-3" in normalized


class TestGlobalCacheInstance:
    """Test the global cache instance functions."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset cache before each test."""
        reset_query_cache()
        yield
        reset_query_cache()

    def test_get_query_cache_creates_singleton(self):
        """get_query_cache should return the same instance."""
        cache1 = get_query_cache(max_size=100, default_ttl=3600)
        cache2 = get_query_cache(max_size=200, default_ttl=7200)  # Different params

        # Should be the same instance (params ignored after first call)
        assert cache1 is cache2

    def test_reset_query_cache(self):
        """reset_query_cache should clear and reset the singleton."""
        cache1 = get_query_cache()
        cache1.set("test", {"value": 1})

        reset_query_cache()

        cache2 = get_query_cache()

        # Should be a new instance with no data
        assert cache2.get("test") is None


class TestCacheWithSessionId:
    """Test cache with session ID isolation."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset cache before each test."""
        reset_query_cache()
        yield
        reset_query_cache()

    def test_session_isolation(self):
        """Same query with different sessions should have separate cache entries."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        # Set for session 1
        cache.set("list them", {"data": [1, 2, 3]}, session_id="session-1")

        # Set for session 2
        cache.set("list them", {"data": [4, 5, 6]}, session_id="session-2")

        # Get for session 1 should return session 1's data
        result1 = cache.get("list them", session_id="session-1")
        assert result1["data"] == [1, 2, 3]

        # Get for session 2 should return session 2's data
        result2 = cache.get("list them", session_id="session-2")
        assert result2["data"] == [4, 5, 6]

    def test_session_vs_no_session(self):
        """Query with session should be separate from query without session."""
        cache = QueryCache(max_size=100, default_ttl=3600)

        cache.set("query", {"value": "no-session"})
        cache.set("query", {"value": "with-session"}, session_id="session-1")

        # Without session should get no-session value
        assert cache.get("query")["value"] == "no-session"

        # With session should get with-session value
        assert cache.get("query", session_id="session-1")["value"] == "with-session"
