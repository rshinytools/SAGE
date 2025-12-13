"""
Tests for DuckDBLoader - Database loading component.
"""

import os
import pytest
import pandas as pd
import tempfile

from core.data import DuckDBLoader


class TestDuckDBLoader:
    """Test suite for DuckDBLoader class."""

    def test_loader_initialization(self, duckdb_loader):
        """Test that DuckDBLoader initializes correctly."""
        assert duckdb_loader is not None
        assert hasattr(duckdb_loader, 'load_dataframe')
        assert hasattr(duckdb_loader, 'list_tables')
        assert hasattr(duckdb_loader, 'get_table_info')

    def test_load_dataframe_basic(self, duckdb_loader, sample_df):
        """Test loading a basic DataFrame."""
        result = duckdb_loader.load_dataframe(sample_df, 'test_table')
        assert result.success is True
        assert result.rows_loaded == 3

    def test_load_dataframe_with_numeric(self, duckdb_loader, sample_df_numeric):
        """Test loading a DataFrame with numeric data."""
        result = duckdb_loader.load_dataframe(sample_df_numeric, 'numeric_table')
        assert result.success is True
        assert result.rows_loaded == 3

    def test_list_tables_empty(self, temp_duckdb_path):
        """Test listing tables on fresh database."""
        loader = DuckDBLoader(temp_duckdb_path)
        try:
            tables = loader.list_tables()
            assert isinstance(tables, list)
            assert len(tables) == 0
        finally:
            loader.close()

    def test_list_tables_after_load(self, duckdb_loader, sample_df):
        """Test listing tables after loading data."""
        duckdb_loader.load_dataframe(sample_df, 'listed_table')
        tables = duckdb_loader.list_tables()
        assert 'LISTED_TABLE' in tables

    def test_get_table_info(self, duckdb_loader, sample_df):
        """Test getting table information."""
        duckdb_loader.load_dataframe(sample_df, 'info_table')
        info = duckdb_loader.get_table_info('info_table')
        assert info is not None
        assert info.row_count == 3
        assert info.column_count == 4

    def test_get_table_info_nonexistent(self, duckdb_loader):
        """Test getting info for non-existent table."""
        info = duckdb_loader.get_table_info('nonexistent_table')
        assert info is None

    def test_load_overwrites_existing(self, duckdb_loader, sample_df):
        """Test that loading to existing table overwrites data by default."""
        # Load initial data
        duckdb_loader.load_dataframe(sample_df, 'overwrite_table')

        # Load new data
        new_df = pd.DataFrame({
            'USUBJID': ['NEW001', 'NEW002'],
            'AGE': [99, 99],
            'SEX': ['F', 'F'],
            'RACE': ['OTHER', 'OTHER']
        })
        result = duckdb_loader.load_dataframe(new_df, 'overwrite_table')
        assert result.success is True
        assert result.rows_loaded == 2

        # Verify new data
        info = duckdb_loader.get_table_info('overwrite_table')
        assert info.row_count == 2


class TestDuckDBLoaderQueries:
    """Test query functionality."""

    def test_query_loaded_data(self, duckdb_loader, sample_df):
        """Test querying loaded data."""
        duckdb_loader.load_dataframe(sample_df, 'query_table')

        result = duckdb_loader.query("SELECT * FROM QUERY_TABLE")
        assert result is not None
        assert len(result) == len(sample_df)

    def test_query_with_filter(self, duckdb_loader, sample_df):
        """Test querying with WHERE clause."""
        duckdb_loader.load_dataframe(sample_df, 'filter_table')

        result = duckdb_loader.query("SELECT * FROM FILTER_TABLE WHERE AGE > 40")
        assert result is not None
        # Original data has ages 45, 32, 58 - so 2 should match
        assert len(result) == 2

    def test_query_aggregation(self, duckdb_loader, sample_df):
        """Test query with aggregation."""
        duckdb_loader.load_dataframe(sample_df, 'agg_table')

        result = duckdb_loader.query("SELECT AVG(AGE) as avg_age FROM AGG_TABLE")
        assert result is not None
        assert len(result) == 1


class TestDuckDBLoaderDataTypes:
    """Test handling of various data types."""

    def test_load_with_dates(self, duckdb_loader, sample_df_with_dates):
        """Test loading DataFrame with date columns."""
        result = duckdb_loader.load_dataframe(sample_df_with_dates, 'date_table')
        assert result.success is True

        info = duckdb_loader.get_table_info('date_table')
        assert info is not None

    def test_load_with_nulls(self, duckdb_loader):
        """Test loading DataFrame with null values."""
        df_with_nulls = pd.DataFrame({
            'ID': [1, 2, 3, 4],
            'VALUE': [10.0, None, 30.0, None],
            'NAME': ['A', None, 'C', 'D']
        })
        result = duckdb_loader.load_dataframe(df_with_nulls, 'null_table')
        assert result.success is True

    def test_load_mixed_types(self, duckdb_loader):
        """Test loading DataFrame with mixed column types."""
        df_mixed = pd.DataFrame({
            'INT_COL': [1, 2, 3],
            'FLOAT_COL': [1.1, 2.2, 3.3],
            'STR_COL': ['a', 'b', 'c'],
            'BOOL_COL': [True, False, True]
        })
        result = duckdb_loader.load_dataframe(df_mixed, 'mixed_table')
        assert result.success is True


class TestDuckDBLoaderLargeData:
    """Test handling of larger datasets."""

    def test_load_large_dataframe(self, duckdb_loader):
        """Test loading a larger DataFrame."""
        large_df = pd.DataFrame({
            'ID': range(100000),
            'VALUE': [f'value_{i}' for i in range(100000)],
            'NUMBER': [float(i) * 0.5 for i in range(100000)]
        })
        result = duckdb_loader.load_dataframe(large_df, 'large_table')
        assert result.success is True
        assert result.rows_loaded == 100000

        info = duckdb_loader.get_table_info('large_table')
        assert info.row_count == 100000

    def test_load_wide_dataframe(self, duckdb_loader):
        """Test loading a DataFrame with many columns."""
        wide_data = {'ID': range(100)}
        for i in range(100):
            wide_data[f'COL_{i}'] = [f'val_{j}_{i}' for j in range(100)]

        wide_df = pd.DataFrame(wide_data)
        result = duckdb_loader.load_dataframe(wide_df, 'wide_table')
        assert result.success is True


class TestDuckDBLoaderTableManagement:
    """Test table management functionality."""

    def test_drop_table(self, duckdb_loader, sample_df):
        """Test dropping a table."""
        duckdb_loader.load_dataframe(sample_df, 'drop_me')

        result = duckdb_loader.drop_table('drop_me')
        assert result is True

        tables = duckdb_loader.list_tables()
        assert 'DROP_ME' not in tables

    def test_table_exists_via_info(self, duckdb_loader, sample_df):
        """Test checking if table exists via get_table_info."""
        duckdb_loader.load_dataframe(sample_df, 'exists_table')

        # Table should exist
        assert duckdb_loader.get_table_info('exists_table') is not None
        # Non-existent table should return None
        assert duckdb_loader.get_table_info('nonexistent') is None

    def test_get_sample_data(self, duckdb_loader, sample_df):
        """Test getting sample data from table."""
        duckdb_loader.load_dataframe(sample_df, 'sample_table')

        sample = duckdb_loader.get_sample_data('sample_table', limit=2)
        assert len(sample) == 2


class TestDuckDBLoaderValidation:
    """Test validation functionality."""

    def test_validate_table(self, duckdb_loader, sample_df):
        """Test table validation."""
        duckdb_loader.load_dataframe(sample_df, 'validate_table')

        validation = duckdb_loader.validate_table('validate_table', expected_rows=3)
        assert validation.is_valid is True

    def test_validate_table_wrong_count(self, duckdb_loader, sample_df):
        """Test validation with wrong expected row count."""
        duckdb_loader.load_dataframe(sample_df, 'wrong_count_table')

        validation = duckdb_loader.validate_table('wrong_count_table', expected_rows=999)
        assert validation.is_valid is False
        assert len(validation.errors) > 0

    def test_validate_nonexistent_table(self, duckdb_loader):
        """Test validation of non-existent table."""
        validation = duckdb_loader.validate_table('nonexistent_table')
        assert validation.is_valid is False


class TestDuckDBLoaderPersistence:
    """Test database persistence."""

    def test_persistence_across_instances(self, temp_duckdb_path, sample_df):
        """Test that data persists across loader instances."""
        # Create first instance and load data
        loader1 = DuckDBLoader(temp_duckdb_path)
        loader1.load_dataframe(sample_df, 'persist_table')
        loader1.close()

        # Create second instance and verify data persists
        loader2 = DuckDBLoader(temp_duckdb_path)
        try:
            tables = loader2.list_tables()
            assert 'PERSIST_TABLE' in tables
        finally:
            loader2.close()

    def test_database_file_created(self, temp_duckdb_path, sample_df):
        """Test that database file is created."""
        loader = DuckDBLoader(temp_duckdb_path)
        loader.load_dataframe(sample_df, 'file_test_table')
        loader.close()

        # Check file exists
        assert os.path.exists(temp_duckdb_path)


class TestDuckDBLoaderParquet:
    """Test Parquet loading functionality."""

    def test_load_parquet_file(self, duckdb_loader, temp_parquet_file):
        """Test loading Parquet file directly."""
        result = duckdb_loader.load_parquet(temp_parquet_file, 'parquet_table')
        assert result.success is True
        assert result.rows_loaded == 3

    def test_load_parquet_nonexistent(self, duckdb_loader):
        """Test loading non-existent Parquet file."""
        result = duckdb_loader.load_parquet('/nonexistent/path.parquet', 'test')
        assert result.success is False
        assert 'not found' in result.error.lower()


class TestDuckDBLoaderExport:
    """Test export functionality."""

    def test_export_to_parquet(self, duckdb_loader, sample_df):
        """Test exporting table to Parquet."""
        duckdb_loader.load_dataframe(sample_df, 'export_table')

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name

        try:
            result = duckdb_loader.export_to_parquet('export_table', output_path)
            assert result is True
            assert os.path.exists(output_path)

            # Verify exported data
            exported_df = pd.read_parquet(output_path)
            assert len(exported_df) == 3
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)
