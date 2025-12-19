# SAGE - Query Response Cache
# ============================
"""
Query and SQL caching for SAGE inference pipeline.

This module provides a two-tier caching system:
1. Full query cache: Caches complete PipelineResult objects
2. SQL cache: Caches generated SQL for similar query patterns

Features:
- TTL-based expiration (default 1 hour)
- Data version tracking - auto-invalidates when DuckDB data changes
- Manual invalidation via API endpoint

This significantly reduces response time for repeated queries.
"""

import time
import hashlib
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from threading import Lock
from pathlib import Path

logger = logging.getLogger(__name__)


# =============================================================================
# Data Version Tracker
# =============================================================================

class DataVersionTracker:
    """
    Tracks the version of clinical data to detect changes.

    When data changes (new files loaded, tables updated), the version
    changes and the cache can be automatically invalidated.

    Version is computed from:
    - DuckDB file modification time
    - Row counts of key tables (ADSL, ADAE, etc.)
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the data version tracker.

        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = db_path
        self._current_version: Optional[str] = None
        self._last_check: float = 0
        self._check_interval: float = 60.0  # Check at most every 60 seconds
        self._lock = Lock()

    def set_db_path(self, db_path: str) -> None:
        """Set or update the database path."""
        with self._lock:
            self.db_path = db_path
            self._current_version = None  # Force recompute

    def get_version(self, force: bool = False) -> Optional[str]:
        """
        Get the current data version hash.

        Args:
            force: Force recomputation even if recently checked

        Returns:
            Version hash string or None if unable to compute
        """
        with self._lock:
            now = time.time()

            # Return cached version if recently checked
            if not force and self._current_version and (now - self._last_check) < self._check_interval:
                return self._current_version

            # Compute new version
            self._current_version = self._compute_version()
            self._last_check = now
            return self._current_version

    def _compute_version(self) -> Optional[str]:
        """Compute version hash from database state."""
        if not self.db_path:
            return None

        try:
            version_parts = []

            # 1. File modification time
            db_file = Path(self.db_path)
            if db_file.exists():
                mtime = db_file.stat().st_mtime
                version_parts.append(f"mtime:{mtime}")

            # 2. Row counts from key tables (if duckdb available)
            try:
                import duckdb
                conn = duckdb.connect(str(db_file), read_only=True)
                try:
                    # Get list of tables
                    tables = conn.execute("SHOW TABLES").fetchall()
                    table_names = [t[0] for t in tables]

                    # Get row counts for each table
                    # Import validation at function level to avoid circular imports
                    from .sql_security import validate_table_name
                    for table in sorted(table_names):
                        try:
                            # Validate table name before use in SQL
                            if validate_table_name(table):
                                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                                version_parts.append(f"{table}:{count}")
                            else:
                                logger.debug(f"Skipping invalid table name: {table}")
                        except Exception as e:
                            logger.debug(f"Could not count rows in {table}: {e}")
                finally:
                    conn.close()
            except ImportError:
                # DuckDB not available, use file mtime only
                pass
            except Exception as e:
                logger.debug(f"Could not query DuckDB for version: {e}")

            if not version_parts:
                return None

            # Create hash from all parts
            version_string = "|".join(version_parts)
            version_hash = hashlib.md5(version_string.encode()).hexdigest()[:12]

            logger.debug(f"Computed data version: {version_hash}")
            return version_hash

        except Exception as e:
            logger.warning(f"Failed to compute data version: {e}")
            return None

    def get_current_version(self) -> Optional[str]:
        """Get the current version (for stats display)."""
        return self.get_version()

    def has_changed(self, previous_version: Optional[str]) -> bool:
        """
        Check if data has changed since the given version.

        Args:
            previous_version: Previously recorded version

        Returns:
            True if data has changed (or version unknown)
        """
        if previous_version is None:
            return False  # No previous version to compare

        current = self.get_version()
        if current is None:
            return False  # Can't determine, assume no change

        return current != previous_version


@dataclass
class CacheEntry:
    """A cached result with TTL and hit tracking."""
    result: Dict[str, Any]
    created_at: float
    ttl_seconds: int = 3600  # 1 hour default
    hit_count: int = 0
    query_hash: str = ""
    data_version: Optional[str] = None  # Version of data when cached

    def is_expired(self) -> bool:
        """Check if this entry has expired."""
        return time.time() - self.created_at > self.ttl_seconds

    def touch(self) -> None:
        """Record a cache hit."""
        self.hit_count += 1


class QueryCache:
    """
    In-memory cache for query results.

    Features:
    - Query normalization for consistent cache keys
    - TTL-based expiration
    - Thread-safe operations
    - Hit/miss statistics
    - LRU eviction when cache is full

    Example:
        cache = QueryCache(max_size=1000, default_ttl=3600)

        # First query - cache miss
        result = cache.get("How many patients had nausea?")
        if result is None:
            result = process_query(...)
            cache.set("How many patients had nausea?", result)

        # Second query - cache hit
        result = cache.get("how many patients had nausea")  # Same normalized form
    """

    def __init__(self, max_size: int = 1000, default_ttl: int = 3600, db_path: Optional[str] = None):
        """
        Initialize the query cache.

        Args:
            max_size: Maximum number of entries to store
            default_ttl: Default time-to-live in seconds
            db_path: Path to DuckDB database for data version tracking
        """
        self._cache: Dict[str, CacheEntry] = {}
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._lock = Lock()
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'expirations': 0,
            'data_invalidations': 0
        }
        # Data version tracking
        self._version_tracker = DataVersionTracker(db_path)
        self._last_known_version: Optional[str] = None

    def _normalize(self, query: str) -> str:
        """
        Normalize query for consistent hashing.

        Normalizations:
        - Convert to lowercase
        - Strip whitespace
        - Collapse multiple spaces
        - Remove trailing punctuation

        Args:
            query: Raw query string

        Returns:
            Normalized query string
        """
        if not query:
            return ""

        # Lowercase and strip
        normalized = query.lower().strip()

        # Collapse whitespace
        normalized = ' '.join(normalized.split())

        # Remove trailing punctuation (but keep internal punctuation like hyphens)
        while normalized and normalized[-1] in '?!.,;:':
            normalized = normalized[:-1]

        return normalized

    def _hash(self, query: str, session_id: Optional[str] = None) -> str:
        """
        Create cache key from normalized query and optional session ID.

        When session_id is provided, the cache key includes it to ensure
        session isolation for context-dependent queries (e.g., "list them").

        Args:
            query: Query string (will be normalized)
            session_id: Optional session ID for session-scoped caching

        Returns:
            16-character hash string
        """
        normalized = self._normalize(query)
        if session_id:
            # Include session_id in hash for session-scoped queries
            cache_input = f"{session_id}:{normalized}"
        else:
            cache_input = normalized
        return hashlib.sha256(cache_input.encode()).hexdigest()[:16]

    def get(self, query: str, session_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get cached result if exists, not expired, and data hasn't changed.

        Args:
            query: Query string
            session_id: Optional session ID for session-scoped caching

        Returns:
            Cached result dict or None if not found/expired/stale
        """
        key = self._hash(query, session_id)

        with self._lock:
            # Check if data version changed - invalidate entire cache if so
            current_version = self._version_tracker.get_version()
            if current_version and self._last_known_version and current_version != self._last_known_version:
                logger.info(f"Data version changed ({self._last_known_version} -> {current_version}), clearing cache")
                self._cache.clear()
                self.stats['data_invalidations'] += 1
                self._last_known_version = current_version
                self.stats['misses'] += 1
                return None

            # Update last known version if not set
            if current_version and not self._last_known_version:
                self._last_known_version = current_version

            entry = self._cache.get(key)

            if entry:
                if entry.is_expired():
                    # Remove expired entry
                    del self._cache[key]
                    self.stats['expirations'] += 1
                    self.stats['misses'] += 1
                    logger.debug(f"Cache EXPIRED for query (key={key[:8]})")
                    return None

                # Check if entry was created with different data version
                if entry.data_version and current_version and entry.data_version != current_version:
                    del self._cache[key]
                    self.stats['data_invalidations'] += 1
                    self.stats['misses'] += 1
                    logger.info(f"Cache STALE for query (key={key[:8]}, data changed)")
                    return None

                # Cache hit
                entry.touch()
                self.stats['hits'] += 1
                logger.info(f"Cache HIT for query (key={key[:8]}, hits={entry.hit_count})")
                return entry.result

            # Cache miss
            self.stats['misses'] += 1
            return None

    def set(self, query: str, result: Dict[str, Any], ttl: Optional[int] = None, session_id: Optional[str] = None) -> None:
        """
        Store result in cache with current data version.

        Args:
            query: Query string
            result: Result dictionary to cache
            ttl: Optional custom TTL (uses default if not specified)
            session_id: Optional session ID for session-scoped caching
        """
        key = self._hash(query, session_id)

        with self._lock:
            # Get current data version
            current_version = self._version_tracker.get_version()
            if current_version:
                self._last_known_version = current_version

            # Create cache entry with data version
            self._cache[key] = CacheEntry(
                result=result,
                created_at=time.time(),
                ttl_seconds=ttl or self.default_ttl,
                query_hash=key,
                data_version=current_version
            )
            logger.info(f"Cache SET for query (key={key[:8]}, version={current_version or 'none'})")

            # Evict if over capacity (LRU-style)
            if len(self._cache) > self.max_size:
                self._evict_oldest()

    def _evict_oldest(self) -> None:
        """Evict the oldest entry (by creation time)."""
        if not self._cache:
            return

        oldest_key = min(
            self._cache.items(),
            key=lambda x: x[1].created_at
        )[0]

        del self._cache[oldest_key]
        self.stats['evictions'] += 1
        logger.debug(f"Cache EVICT oldest entry (key={oldest_key[:8]})")

    def invalidate(self, query: str) -> bool:
        """
        Invalidate a specific query's cache entry.

        Args:
            query: Query string to invalidate

        Returns:
            True if entry was found and removed
        """
        key = self._hash(query)

        with self._lock:
            if key in self._cache:
                del self._cache[key]
                logger.info(f"Cache INVALIDATE for query (key={key[:8]})")
                return True
            return False

    def set_db_path(self, db_path: str) -> None:
        """
        Set or update the database path for version tracking.

        Args:
            db_path: Path to DuckDB database
        """
        self._version_tracker.set_db_path(db_path)
        logger.info(f"Cache DB path set to: {db_path}")

    def clear(self) -> None:
        """Clear all cached entries and reset data version tracking."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._last_known_version = None  # Force version recheck
            self.stats = {
                'hits': 0,
                'misses': 0,
                'evictions': 0,
                'expirations': 0,
                'data_invalidations': 0
            }
            logger.info(f"Cache CLEARED ({count} entries removed)")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics including data version info.

        Returns:
            Dictionary with cache stats including hit rate and data version
        """
        with self._lock:
            total = self.stats['hits'] + self.stats['misses']
            hit_rate = (self.stats['hits'] / total * 100) if total > 0 else 0

            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'hits': self.stats['hits'],
                'misses': self.stats['misses'],
                'evictions': self.stats['evictions'],
                'expirations': self.stats['expirations'],
                'data_invalidations': self.stats['data_invalidations'],
                'hit_rate': round(hit_rate, 1),
                'hit_rate_str': f"{hit_rate:.1f}%",
                'data_version': self._last_known_version,
                'db_path': self._version_tracker.db_path
            }

    def get_detailed_stats(self) -> Dict[str, Any]:
        """
        Get detailed cache statistics including age distribution.

        Returns:
            Comprehensive stats including age distribution and version info
        """
        with self._lock:
            total = self.stats['hits'] + self.stats['misses']
            hit_rate = (self.stats['hits'] / total * 100) if total > 0 else 0

            # Calculate age distribution
            now = time.time()
            ages = [now - entry.created_at for entry in self._cache.values()]

            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'hits': self.stats['hits'],
                'misses': self.stats['misses'],
                'hit_rate': f"{hit_rate:.1f}%",
                'evictions': self.stats.get('evictions', 0),
                'expirations': self.stats.get('expirations', 0),
                'data_invalidations': self.stats.get('data_invalidations', 0),
                'oldest_entry_age_seconds': round(max(ages), 1) if ages else 0,
                'newest_entry_age_seconds': round(min(ages), 1) if ages else 0,
                'avg_entry_age_seconds': round(sum(ages) / len(ages), 1) if ages else 0,
                'default_ttl_seconds': self.default_ttl,
                'data_version': self._version_tracker.get_current_version() if self._version_tracker else None
            }

    def get_entries(self) -> List[Dict[str, Any]]:
        """
        Get information about all cache entries.

        Returns:
            List of entry info dicts
        """
        with self._lock:
            entries = []
            for key, entry in self._cache.items():
                entries.append({
                    'key': key,
                    'created_at': entry.created_at,
                    'ttl_seconds': entry.ttl_seconds,
                    'hit_count': entry.hit_count,
                    'is_expired': entry.is_expired(),
                    'age_seconds': int(time.time() - entry.created_at)
                })
            return sorted(entries, key=lambda x: x['created_at'], reverse=True)

    def cleanup_expired(self) -> int:
        """
        Remove all expired entries.

        Returns:
            Number of entries removed
        """
        removed = 0
        with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items()
                if entry.is_expired()
            ]
            for key in expired_keys:
                del self._cache[key]
                removed += 1
                self.stats['expirations'] += 1

        if removed > 0:
            logger.info(f"Cache CLEANUP removed {removed} expired entries")

        return removed

    def __len__(self) -> int:
        """Return number of entries in cache."""
        return len(self._cache)

    def __contains__(self, query: str) -> bool:
        """Check if query is in cache (and not expired)."""
        return self.get(query) is not None


# =============================================================================
# Global Cache Instance
# =============================================================================

_query_cache: Optional[QueryCache] = None
_cache_lock = Lock()


def get_query_cache(max_size: int = 1000, default_ttl: int = 3600, db_path: Optional[str] = None) -> QueryCache:
    """
    Get or create the global query cache instance.

    Args:
        max_size: Maximum cache size (only used on first call)
        default_ttl: Default TTL in seconds (only used on first call)
        db_path: Path to DuckDB database for data version tracking

    Returns:
        Global QueryCache instance
    """
    global _query_cache

    with _cache_lock:
        if _query_cache is None:
            _query_cache = QueryCache(max_size=max_size, default_ttl=default_ttl, db_path=db_path)
            logger.info(f"Created global query cache (max_size={max_size}, ttl={default_ttl}s)")
        elif db_path and _query_cache._version_tracker.db_path != db_path:
            # Update db path if provided and different
            _query_cache.set_db_path(db_path)

        return _query_cache


def reset_query_cache() -> None:
    """Reset the global query cache (for testing)."""
    global _query_cache

    with _cache_lock:
        if _query_cache is not None:
            _query_cache.clear()
        _query_cache = None
        logger.info("Global query cache reset")
