#!/usr/bin/env python
# SAGE - Factory 3: Dictionary Plant
# ===================================
# Builds fuzzy matching indexes for clinical data
"""
Dictionary Factory Pipeline (Factory 3)

This script orchestrates the dictionary building pipeline:
1. Scan unique values from DuckDB (Factory 1 output)
2. Build RapidFuzz index for string matching
3. Generate schema_map.json for column lookups

Note: Semantic/embedding search has been intentionally excluded.
Clinical data requires exact terminology matching (MedDRA controlled vocabulary).
Synonym matching is handled via MedDRA hierarchy, not AI embeddings.

Usage:
    python scripts/factory3_dictionary.py
    python scripts/factory3_dictionary.py --rebuild
    python scripts/factory3_dictionary.py --table AE DM
    python scripts/factory3_dictionary.py --test
"""

import argparse
import logging
import sys
import json
import time
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.dictionary import (
    ValueScanner,
    FuzzyMatcher,
    SchemaMapper,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DictionaryPipeline:
    """
    Factory 3: Dictionary Building Pipeline.

    Builds search indexes by combining:
    - Unique values from DuckDB (Factory 1)
    - Variable definitions from metadata (Factory 2)

    Outputs:
    - fuzzy_index.pkl for RapidFuzz matching
    - schema_map.json for column lookups
    - dictionary_status.json for status tracking
    """

    def __init__(
        self,
        db_path: str = "data/database/clinical.duckdb",
        metadata_path: str = "knowledge/golden_metadata.json",
        knowledge_dir: str = "knowledge",
    ):
        """
        Initialize the dictionary pipeline.

        Args:
            db_path: Path to clinical.duckdb
            metadata_path: Path to golden_metadata.json
            knowledge_dir: Directory for output files
        """
        self.db_path = Path(db_path)
        self.metadata_path = Path(metadata_path)
        self.knowledge_dir = Path(knowledge_dir)

        # Output paths
        self.fuzzy_index_path = self.knowledge_dir / "fuzzy_index.pkl"
        self.schema_map_path = self.knowledge_dir / "schema_map.json"
        self.status_path = self.knowledge_dir / "dictionary_status.json"

        # Ensure directories exist
        self.knowledge_dir.mkdir(parents=True, exist_ok=True)

    def verify_inputs(self) -> bool:
        """Verify required input files exist."""
        if not self.db_path.exists():
            logger.error(f"Database not found: {self.db_path}")
            logger.info("Run Factory 1 first: python scripts/factory1_data.py")
            return False

        if not self.metadata_path.exists():
            logger.warning(f"Metadata not found: {self.metadata_path}")
            logger.info("Running without metadata descriptions.")

        return True

    def scan_values(
        self,
        tables: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Scan unique values from DuckDB tables.

        Args:
            tables: Optional list of tables to scan

        Returns:
            Nested dict: {table: {column: ScanResult}}
        """
        logger.info("=" * 60)
        logger.info("STEP 1: Scanning unique values from DuckDB")
        logger.info("=" * 60)

        metadata_path = str(self.metadata_path) if self.metadata_path.exists() else None

        scanner = ValueScanner(str(self.db_path), metadata_path)
        try:
            # Get tables to scan
            if tables:
                scan_tables = tables
            else:
                scan_tables = scanner.get_tables()

            logger.info(f"Scanning {len(scan_tables)} tables...")

            # Scan with progress
            def progress(table, col, current, total):
                logger.debug(f"  {table}.{col} ({current}/{total})")

            results = scanner.scan_all_tables(scan_tables, progress)
            stats = scanner.get_statistics(results)

            logger.info(f"Scanned {stats.tables_scanned} tables, "
                       f"{stats.columns_scanned} columns, "
                       f"{stats.total_unique_values} unique values")

            return results
        finally:
            scanner.close()

    def build_fuzzy_index(
        self,
        scan_results: Dict[str, Dict[str, Any]]
    ) -> FuzzyMatcher:
        """
        Build RapidFuzz index from scanned values.

        Args:
            scan_results: {table: {column: ScanResult}}

        Returns:
            Configured FuzzyMatcher
        """
        logger.info("=" * 60)
        logger.info("STEP 2: Building fuzzy matching index")
        logger.info("=" * 60)

        # Transform scan results to format expected by FuzzyMatcher
        # {table: {column: ScanResult}} -> {table: {column: [values]}}
        values = {}
        for table, columns in scan_results.items():
            values[table] = {}
            for column, result in columns.items():
                values[table][column] = result.values

        matcher = FuzzyMatcher()
        entry_count = matcher.build_index(values)

        logger.info(f"Built fuzzy index with {entry_count} entries")

        # Save index
        matcher.save(str(self.fuzzy_index_path))
        logger.info(f"Saved fuzzy index to {self.fuzzy_index_path}")

        return matcher

    def build_schema_map(self) -> int:
        """
        Build schema_map.json from database.

        Returns:
            Number of tables in schema map
        """
        logger.info("=" * 60)
        logger.info("STEP 3: Building schema map")
        logger.info("=" * 60)

        metadata_path = str(self.metadata_path) if self.metadata_path.exists() else None

        mapper = SchemaMapper(str(self.db_path), metadata_path)
        try:
            schema_map = mapper.build_schema_map()
            mapper.save_schema_map(schema_map, str(self.schema_map_path))

            logger.info(f"Built schema map with {len(schema_map.tables)} tables, "
                       f"{len(schema_map.columns)} columns")

            return len(schema_map.tables)
        finally:
            mapper.close()

    def save_status(
        self,
        fuzzy_count: int,
        schema_tables: int,
        duration: float
    ) -> None:
        """Save build status for API/UI."""
        status = {
            "last_build": datetime.now().isoformat(),
            "build_duration_seconds": round(duration, 2),
            "fuzzy_index": {
                "entries": fuzzy_count,
                "path": str(self.fuzzy_index_path)
            },
            "schema_map": {
                "tables": schema_tables,
                "path": str(self.schema_map_path)
            },
            "inputs": {
                "database": str(self.db_path),
                "metadata": str(self.metadata_path)
            }
        }

        with open(self.status_path, 'w') as f:
            json.dump(status, f, indent=2)

        logger.info(f"Saved build status to {self.status_path}")

    def run(
        self,
        rebuild: bool = False,
        tables: Optional[List[str]] = None,
    ) -> bool:
        """
        Run the complete dictionary building pipeline.

        Args:
            rebuild: Clear and rebuild all indexes
            tables: Optional list of tables to process

        Returns:
            True if successful
        """
        start_time = time.time()

        logger.info("=" * 60)
        logger.info("SAGE Factory 3: Dictionary Plant")
        logger.info("=" * 60)

        # Verify inputs
        if not self.verify_inputs():
            return False

        # Step 1: Scan values
        scan_results = self.scan_values(tables)

        if not scan_results:
            logger.error("No values scanned. Check database content.")
            return False

        # Step 2: Build fuzzy index
        matcher = self.build_fuzzy_index(scan_results)
        fuzzy_count = len(matcher)

        # Step 3: Build schema map
        schema_tables = self.build_schema_map()

        duration = time.time() - start_time

        # Save status
        self.save_status(fuzzy_count, schema_tables, duration)

        logger.info("=" * 60)
        logger.info("DICTIONARY BUILD COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Fuzzy index entries: {fuzzy_count}")
        logger.info(f"Schema map tables: {schema_tables}")
        logger.info(f"Total duration: {duration:.1f}s")
        logger.info("=" * 60)

        return True


def test_fuzzy_matching():
    """Quick test of fuzzy matching."""
    logger.info("Testing fuzzy matching...")

    index_path = Path("knowledge/fuzzy_index.pkl")
    if not index_path.exists():
        logger.error("Fuzzy index not found. Run build first.")
        return False

    matcher = FuzzyMatcher.load(str(index_path))

    logger.info(f"Loaded index with {len(matcher)} entries")
    logger.info("")

    test_queries = [
        ("headche", "Typo for HEADACHE"),
        ("hypertention", "Typo for HYPERTENSION"),
        ("nausea", "Should match exactly"),
        ("diabetis", "Typo for DIABETES"),
        ("Myocardial infarction", "Should match exactly"),
    ]

    all_passed = True
    for query, description in test_queries:
        matches = matcher.match(query, threshold=65.0, limit=3)
        logger.info(f"Query: '{query}' ({description})")
        if matches:
            for m in matches:
                logger.info(f"  -> {m.value} (score: {m.score:.1f}%, {m.table}.{m.column})")
        else:
            logger.info(f"  -> No matches found")
            if "exactly" in description:
                all_passed = False
        logger.info("")

    return all_passed


def show_statistics():
    """Show dictionary statistics."""
    index_path = Path("knowledge/fuzzy_index.pkl")
    schema_path = Path("knowledge/schema_map.json")
    status_path = Path("knowledge/dictionary_status.json")

    logger.info("=" * 60)
    logger.info("DICTIONARY STATISTICS")
    logger.info("=" * 60)

    # Status
    if status_path.exists():
        with open(status_path, 'r') as f:
            status = json.load(f)
        logger.info(f"Last build: {status.get('last_build', 'Unknown')}")
        logger.info(f"Build duration: {status.get('build_duration_seconds', 0):.2f}s")
    else:
        logger.warning("Status file not found")

    # Fuzzy index
    if index_path.exists():
        matcher = FuzzyMatcher.load(str(index_path))
        stats = matcher.get_statistics()
        logger.info("")
        logger.info("Fuzzy Index:")
        logger.info(f"  Total entries: {stats['total_entries']}")
        logger.info(f"  Unique values: {stats['unique_values']}")
        logger.info(f"  Tables: {stats['tables']}")
        logger.info(f"  Columns: {stats['columns']}")
    else:
        logger.warning("Fuzzy index not found")

    # Schema map
    if schema_path.exists():
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        logger.info("")
        logger.info("Schema Map:")
        logger.info(f"  Tables: {len(schema.get('tables', {}))}")
        logger.info(f"  Columns: {len(schema.get('columns', {}))}")
        logger.info(f"  Generated: {schema.get('generated_at', 'Unknown')}")
    else:
        logger.warning("Schema map not found")

    logger.info("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="SAGE Factory 3: Dictionary Plant",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/factory3_dictionary.py                    # Full build
  python scripts/factory3_dictionary.py --rebuild          # Force rebuild
  python scripts/factory3_dictionary.py --table AE DM      # Specific tables
  python scripts/factory3_dictionary.py --test             # Test fuzzy matching
  python scripts/factory3_dictionary.py --stats            # Show statistics
        """
    )

    parser.add_argument(
        "--db-path",
        default="data/database/clinical.duckdb",
        help="Path to DuckDB database"
    )

    parser.add_argument(
        "--metadata-path",
        default="knowledge/golden_metadata.json",
        help="Path to golden metadata JSON"
    )

    parser.add_argument(
        "--knowledge-dir",
        default="knowledge",
        help="Directory for output files"
    )

    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Clear and rebuild all indexes"
    )

    parser.add_argument(
        "--table",
        nargs="+",
        help="Specific tables to process"
    )

    parser.add_argument(
        "--test",
        action="store_true",
        help="Run fuzzy matching test"
    )

    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show dictionary statistics"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.test:
        success = test_fuzzy_matching()
        sys.exit(0 if success else 1)

    if args.stats:
        show_statistics()
        sys.exit(0)

    # Run pipeline
    pipeline = DictionaryPipeline(
        db_path=args.db_path,
        metadata_path=args.metadata_path,
        knowledge_dir=args.knowledge_dir,
    )

    success = pipeline.run(
        rebuild=args.rebuild,
        tables=args.table,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
