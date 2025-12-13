# SAGE Dictionary - Value Scanner
# ================================
"""
Scans clinical data tables in DuckDB and extracts unique values
from key columns for fuzzy matching and semantic search.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable, Set
from dataclasses import dataclass, field

import duckdb

logger = logging.getLogger(__name__)


# Columns to scan by domain - these are the key searchable columns
# NOTE: AETERM is excluded as it's free-text verbatim term, not standardized
# NOTE: Flag columns (Y/N) are excluded to prevent false fuzzy matches
SCANNABLE_COLUMNS = {
    # SDTM Domains
    "AE": ["AEDECOD", "AEBODSYS", "AESOC", "AEHLT", "AELLT"],  # Removed AETERM (free text), AESER/AEREL (flags)
    "CM": ["CMTRT", "CMDECOD", "CMCLAS", "CMCAT", "CMINDC"],
    "LB": ["LBTEST", "LBTESTCD", "LBCAT", "LBSCAT", "LBSPEC"],
    "VS": ["VSTEST", "VSTESTCD", "VSPOS", "VSLOC"],
    "DM": ["SEX", "RACE", "ETHNIC", "COUNTRY", "ARM", "ARMCD", "ACTARM"],
    "EX": ["EXTRT", "EXDOSE", "EXDOSU", "EXROUTE", "EXDOSFRM"],
    "MH": ["MHDECOD", "MHBODSYS", "MHCAT"],  # Removed MHTERM (free text)
    "PE": ["PETEST", "PETESTCD", "PEBODSYS", "PELOC"],
    "EG": ["EGTEST", "EGTESTCD", "EGCAT"],

    # ADaM Domains - removed flag columns (Y/N values)
    "ADSL": ["SEX", "RACE", "ETHNIC", "TRT01P", "TRT01A", "ARM", "ACTARM", "AGEGR1"],
    "ADAE": ["AEDECOD", "AEBODSYS", "AESOC", "AEHLT", "AELLT", "AEACN", "AEOUT", "AETOXGR"],
    "ADLB": ["PARAM", "PARAMCD", "PARCAT1", "AVISIT", "ANRIND", "BNRIND"],
    "ADVS": ["PARAM", "PARAMCD", "PARCAT1", "AVISIT"],
    "ADCM": ["CMDECOD", "CMCLAS", "CMINDC"],
}

# Maximum unique values to store per column (to avoid memory issues)
MAX_VALUES_PER_COLUMN = 10000

# Minimum cardinality threshold (skip columns with too few unique values)
MIN_CARDINALITY = 2

# Maximum cardinality threshold (skip columns like IDs with too many unique values)
MAX_CARDINALITY_RATIO = 0.8  # Skip if unique/total > 80%


@dataclass
class ScanResult:
    """Result of scanning a single column."""
    table: str
    column: str
    values: List[str]
    total_rows: int
    unique_count: int
    null_count: int
    sample_values: List[str] = field(default_factory=list)


@dataclass
class ScanStatistics:
    """Overall scan statistics."""
    tables_scanned: int = 0
    columns_scanned: int = 0
    total_unique_values: int = 0
    total_rows_processed: int = 0
    skipped_columns: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class ValueScanner:
    """
    Scans clinical data tables for unique values.

    Extracts unique values from key columns (AETERM, CMTRT, LBTEST, etc.)
    that are useful for fuzzy matching and semantic search.
    """

    def __init__(self,
                 db_path: str,
                 metadata_path: Optional[str] = None,
                 max_values_per_column: int = MAX_VALUES_PER_COLUMN):
        """
        Initialize the value scanner.

        Args:
            db_path: Path to clinical_data.duckdb
            metadata_path: Optional path to golden_metadata.json for descriptions
            max_values_per_column: Maximum unique values to store per column
        """
        self.db_path = Path(db_path)
        self.metadata_path = Path(metadata_path) if metadata_path else None
        self.max_values = max_values_per_column
        self._metadata: Optional[Dict] = None
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = duckdb.connect(str(self.db_path))
        return self._conn

    def _load_metadata(self) -> Dict:
        """Load golden metadata if available."""
        if self._metadata is not None:
            return self._metadata

        if self.metadata_path and self.metadata_path.exists():
            with open(self.metadata_path, 'r') as f:
                self._metadata = json.load(f)
        else:
            self._metadata = {}
        return self._metadata

    def get_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        conn = self._get_connection()
        result = conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Get column info for a table."""
        conn = self._get_connection()
        result = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
        return [{"name": row[0], "type": row[1]} for row in result]

    def get_scannable_columns(self, table_name: str) -> List[str]:
        """
        Get list of columns worth scanning for a table.

        Uses predefined list if available, otherwise scans VARCHAR columns
        with appropriate cardinality.
        """
        table_upper = table_name.upper()

        # Check predefined list first
        if table_upper in SCANNABLE_COLUMNS:
            predefined = SCANNABLE_COLUMNS[table_upper]
            # Verify columns exist in table
            actual_columns = {col["name"].upper() for col in self.get_table_columns(table_name)}
            return [col for col in predefined if col.upper() in actual_columns]

        # Fallback: scan VARCHAR columns with reasonable cardinality
        columns = self.get_table_columns(table_name)
        conn = self._get_connection()

        scannable = []
        for col in columns:
            if "VARCHAR" not in col["type"].upper() and "TEXT" not in col["type"].upper():
                continue

            col_name = col["name"]

            # Skip ID-like columns
            if any(suffix in col_name.upper() for suffix in ["ID", "SEQ", "NUM", "DTC", "DT", "TM"]):
                continue

            # Check cardinality
            try:
                result = conn.execute(f'''
                    SELECT
                        COUNT(*) as total,
                        COUNT(DISTINCT "{col_name}") as unique_count
                    FROM "{table_name}"
                ''').fetchone()

                total, unique = result
                if total > 0 and unique >= MIN_CARDINALITY:
                    if unique / total <= MAX_CARDINALITY_RATIO:
                        scannable.append(col_name)
            except Exception as e:
                logger.warning(f"Error checking cardinality for {table_name}.{col_name}: {e}")

        return scannable

    def scan_column(self, table_name: str, column_name: str) -> ScanResult:
        """
        Scan a single column for unique values.

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            ScanResult with unique values and statistics
        """
        conn = self._get_connection()

        # Get statistics
        stats = conn.execute(f'''
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT "{column_name}") as unique_count,
                COUNT(*) - COUNT("{column_name}") as null_count
            FROM "{table_name}"
        ''').fetchone()

        total_rows, unique_count, null_count = stats

        # Get unique values (limited)
        result = conn.execute(f'''
            SELECT DISTINCT "{column_name}"
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            ORDER BY "{column_name}"
            LIMIT {self.max_values}
        ''').fetchall()

        values = [str(row[0]).strip() for row in result if row[0] is not None]
        values = [v for v in values if v]  # Remove empty strings

        # Get sample values for preview
        sample_values = values[:10] if values else []

        return ScanResult(
            table=table_name.upper(),
            column=column_name.upper(),
            values=values,
            total_rows=total_rows,
            unique_count=unique_count,
            null_count=null_count,
            sample_values=sample_values
        )

    def scan_table(self,
                   table_name: str,
                   progress_callback: Optional[Callable[[str, int, int], None]] = None
                   ) -> Dict[str, ScanResult]:
        """
        Scan all scannable columns in a table.

        Args:
            table_name: Name of the table to scan
            progress_callback: Optional callback(column, current, total)

        Returns:
            Dict mapping column names to ScanResults
        """
        columns = self.get_scannable_columns(table_name)
        results = {}

        for i, col in enumerate(columns):
            if progress_callback:
                progress_callback(col, i + 1, len(columns))

            try:
                result = self.scan_column(table_name, col)
                if result.values:  # Only include columns with values
                    results[col.upper()] = result
                    logger.debug(f"Scanned {table_name}.{col}: {result.unique_count} unique values")
            except Exception as e:
                logger.error(f"Error scanning {table_name}.{col}: {e}")

        return results

    def scan_all_tables(self,
                        tables: Optional[List[str]] = None,
                        progress_callback: Optional[Callable[[str, str, int, int], None]] = None
                        ) -> Dict[str, Dict[str, ScanResult]]:
        """
        Scan all tables for unique values.

        Args:
            tables: Optional list of tables to scan (defaults to all)
            progress_callback: Optional callback(table, column, current, total)

        Returns:
            Nested dict: {table: {column: ScanResult}}
        """
        if tables is None:
            tables = self.get_tables()

        all_results = {}
        total_tables = len(tables)

        for i, table in enumerate(tables):
            logger.info(f"Scanning table {table} ({i+1}/{total_tables})")

            def table_progress(col, cur, tot):
                if progress_callback:
                    progress_callback(table, col, cur, tot)

            results = self.scan_table(table, table_progress)
            if results:
                all_results[table.upper()] = results

        return all_results

    def get_flat_values(self,
                        scan_results: Dict[str, Dict[str, ScanResult]]
                        ) -> List[Dict[str, Any]]:
        """
        Flatten scan results into a list of value records.

        Returns list of dicts with: value, table, column, count info
        """
        flat = []
        for table, columns in scan_results.items():
            for column, result in columns.items():
                for value in result.values:
                    flat.append({
                        "value": value,
                        "table": table,
                        "column": column,
                        "id": f"{table}.{column}.{value}"
                    })
        return flat

    def get_statistics(self,
                       scan_results: Dict[str, Dict[str, ScanResult]]
                       ) -> ScanStatistics:
        """Calculate overall statistics from scan results."""
        stats = ScanStatistics()

        stats.tables_scanned = len(scan_results)

        for table, columns in scan_results.items():
            stats.columns_scanned += len(columns)
            for column, result in columns.items():
                stats.total_unique_values += len(result.values)
                stats.total_rows_processed += result.total_rows

        return stats

    def get_column_description(self, table: str, column: str) -> str:
        """Get variable description from metadata if available."""
        metadata = self._load_metadata()

        if not metadata:
            return ""

        # Search in domains
        domains = metadata.get("domains", {})
        table_upper = table.upper()

        if table_upper in domains:
            variables = domains[table_upper].get("variables", {})
            column_upper = column.upper()
            if column_upper in variables:
                return variables[column_upper].get("description", "")

        return ""

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def scan_database(db_path: str,
                  metadata_path: Optional[str] = None,
                  tables: Optional[List[str]] = None) -> Dict[str, Dict[str, List[str]]]:
    """
    Convenience function to scan database and return simple value dict.

    Returns:
        {table: {column: [values]}}
    """
    with ValueScanner(db_path, metadata_path) as scanner:
        results = scanner.scan_all_tables(tables)

        # Simplify to just values
        simple = {}
        for table, columns in results.items():
            simple[table] = {}
            for column, result in columns.items():
                simple[table][column] = result.values

        return simple
