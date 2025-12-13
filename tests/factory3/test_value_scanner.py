# Tests for ValueScanner
# =======================

import pytest
import tempfile
import os
from pathlib import Path

import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import duckdb
from core.dictionary.value_scanner import (
    ValueScanner,
    ScanResult,
    ScanStatistics,
    scan_database,
    SCANNABLE_COLUMNS
)


class TestValueScannerBasic:
    """Basic ValueScanner tests."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary DuckDB database with test data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            conn = duckdb.connect(db_path)

            # Create test tables
            conn.execute("""
                CREATE TABLE AE (
                    USUBJID VARCHAR,
                    AETERM VARCHAR,
                    AEDECOD VARCHAR,
                    AESEV VARCHAR
                )
            """)
            conn.execute("""
                INSERT INTO AE VALUES
                ('SUBJ001', 'HEADACHE', 'Headache', 'MILD'),
                ('SUBJ002', 'NAUSEA', 'Nausea', 'MODERATE'),
                ('SUBJ003', 'HEADACHE', 'Headache', 'SEVERE'),
                ('SUBJ004', 'FATIGUE', 'Fatigue', 'MILD')
            """)

            conn.execute("""
                CREATE TABLE CM (
                    USUBJID VARCHAR,
                    CMTRT VARCHAR,
                    CMDECOD VARCHAR
                )
            """)
            conn.execute("""
                INSERT INTO CM VALUES
                ('SUBJ001', 'TYLENOL', 'ACETAMINOPHEN'),
                ('SUBJ002', 'ASPIRIN', 'ACETYLSALICYLIC ACID'),
                ('SUBJ003', 'TYLENOL', 'ACETAMINOPHEN')
            """)

            conn.close()
            yield db_path

    def test_scanner_initialization(self, temp_db):
        """Test scanner initialization."""
        scanner = ValueScanner(temp_db)
        assert scanner.db_path.exists()
        scanner.close()

    def test_scanner_initialization_missing_db(self):
        """Test initialization with missing database."""
        with pytest.raises(FileNotFoundError):
            ValueScanner("/nonexistent/path.duckdb")

    def test_get_tables(self, temp_db):
        """Test getting table list."""
        with ValueScanner(temp_db) as scanner:
            tables = scanner.get_tables()
            assert "AE" in tables
            assert "CM" in tables

    def test_get_table_columns(self, temp_db):
        """Test getting column info."""
        with ValueScanner(temp_db) as scanner:
            columns = scanner.get_table_columns("AE")
            col_names = [c["name"] for c in columns]
            assert "AETERM" in col_names
            assert "AEDECOD" in col_names


class TestValueScannerScanning:
    """ValueScanner scanning tests."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary DuckDB database with test data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            conn = duckdb.connect(db_path)

            conn.execute("""
                CREATE TABLE AE (
                    USUBJID VARCHAR,
                    AETERM VARCHAR,
                    AEDECOD VARCHAR,
                    AEBODSYS VARCHAR
                )
            """)
            conn.execute("""
                INSERT INTO AE VALUES
                ('SUBJ001', 'HEADACHE', 'Headache', 'Nervous system disorders'),
                ('SUBJ002', 'NAUSEA', 'Nausea', 'Gastrointestinal disorders'),
                ('SUBJ003', 'HEADACHE', 'Headache', 'Nervous system disorders'),
                ('SUBJ004', 'FATIGUE', 'Fatigue', 'General disorders')
            """)

            conn.close()
            yield db_path

    def test_scan_column(self, temp_db):
        """Test scanning a single column."""
        with ValueScanner(temp_db) as scanner:
            result = scanner.scan_column("AE", "AETERM")

            assert isinstance(result, ScanResult)
            assert result.table == "AE"
            assert result.column == "AETERM"
            assert result.total_rows == 4
            assert result.unique_count == 3  # HEADACHE, NAUSEA, FATIGUE
            assert "HEADACHE" in result.values
            assert "NAUSEA" in result.values
            assert "FATIGUE" in result.values

    def test_scan_table(self, temp_db):
        """Test scanning all columns in a table."""
        with ValueScanner(temp_db) as scanner:
            results = scanner.scan_table("AE")

            # Should have scanned predefined columns
            assert "AETERM" in results
            assert "AEDECOD" in results
            assert "AEBODSYS" in results

    def test_scan_all_tables(self, temp_db):
        """Test scanning all tables."""
        with ValueScanner(temp_db) as scanner:
            results = scanner.scan_all_tables()

            assert "AE" in results
            assert "AETERM" in results["AE"]

    def test_get_flat_values(self, temp_db):
        """Test flattening scan results."""
        with ValueScanner(temp_db) as scanner:
            results = scanner.scan_all_tables()
            flat = scanner.get_flat_values(results)

            assert len(flat) > 0
            assert all("value" in item for item in flat)
            assert all("table" in item for item in flat)
            assert all("column" in item for item in flat)
            assert all("id" in item for item in flat)

    def test_get_statistics(self, temp_db):
        """Test calculating statistics."""
        with ValueScanner(temp_db) as scanner:
            results = scanner.scan_all_tables()
            stats = scanner.get_statistics(results)

            assert isinstance(stats, ScanStatistics)
            assert stats.tables_scanned > 0
            assert stats.columns_scanned > 0
            assert stats.total_unique_values > 0


class TestValueScannerColumnFiltering:
    """Test column filtering logic."""

    @pytest.fixture
    def temp_db_with_ids(self):
        """Create database with ID columns that should be skipped."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            conn = duckdb.connect(db_path)

            conn.execute("""
                CREATE TABLE CUSTOM (
                    CUSTOMID VARCHAR,
                    CUSTOMSEQ INTEGER,
                    CUSTOMTERM VARCHAR,
                    CUSTOMCAT VARCHAR,
                    CUSTOMDTC VARCHAR
                )
            """)
            conn.execute("""
                INSERT INTO CUSTOM VALUES
                ('ID001', 1, 'VALUE1', 'CAT1', '2024-01-01'),
                ('ID002', 2, 'VALUE2', 'CAT2', '2024-01-02')
            """)

            conn.close()
            yield db_path

    def test_scannable_columns_predefined(self, temp_db_with_ids):
        """Test that predefined columns are used for known domains."""
        with ValueScanner(temp_db_with_ids) as scanner:
            # AE is a known domain
            columns = scanner.get_scannable_columns("CUSTOM")
            # Should filter out ID-like columns
            # CUSTOMID, CUSTOMSEQ, CUSTOMDTC should be excluded
            assert "CUSTOMID" not in columns
            assert "CUSTOMDTC" not in columns


class TestScanDatabaseFunction:
    """Test the convenience scan_database function."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary DuckDB database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            conn = duckdb.connect(db_path)

            conn.execute("""
                CREATE TABLE AE (
                    AETERM VARCHAR
                )
            """)
            conn.execute("""
                INSERT INTO AE VALUES ('HEADACHE'), ('NAUSEA')
            """)

            conn.close()
            yield db_path

    def test_scan_database_function(self, temp_db):
        """Test the convenience function."""
        result = scan_database(temp_db)

        assert isinstance(result, dict)
        assert "AE" in result
        assert "AETERM" in result["AE"]
        assert "HEADACHE" in result["AE"]["AETERM"]


class TestScannableColumnsConfig:
    """Test SCANNABLE_COLUMNS configuration."""

    def test_scannable_columns_has_common_domains(self):
        """Test that common domains are configured."""
        assert "AE" in SCANNABLE_COLUMNS
        assert "CM" in SCANNABLE_COLUMNS
        assert "LB" in SCANNABLE_COLUMNS
        assert "VS" in SCANNABLE_COLUMNS
        assert "DM" in SCANNABLE_COLUMNS
        assert "ADSL" in SCANNABLE_COLUMNS

    def test_scannable_columns_ae_has_key_columns(self):
        """Test AE domain has key columns."""
        ae_cols = SCANNABLE_COLUMNS["AE"]
        assert "AETERM" in ae_cols
        assert "AEDECOD" in ae_cols
        assert "AEBODSYS" in ae_cols

    def test_scannable_columns_cm_has_key_columns(self):
        """Test CM domain has key columns."""
        cm_cols = SCANNABLE_COLUMNS["CM"]
        assert "CMTRT" in cm_cols
        assert "CMDECOD" in cm_cols
