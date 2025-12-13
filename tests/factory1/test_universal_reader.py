"""
Tests for UniversalReader - Multi-format data reading component.
"""

import os
import pytest
import pandas as pd
import tempfile
from pathlib import Path

from core.data import UniversalReader, DataFormat


class TestUniversalReader:
    """Test suite for UniversalReader class."""

    def test_reader_initialization(self, universal_reader):
        """Test that UniversalReader initializes correctly."""
        assert universal_reader is not None
        assert hasattr(universal_reader, 'read_file')
        assert hasattr(universal_reader, 'detect_format')
        assert hasattr(universal_reader, 'extract_schema_info')

    def test_detect_format_csv(self, universal_reader, temp_csv_file):
        """Test CSV format detection."""
        detected = universal_reader.detect_format(temp_csv_file)
        assert detected == DataFormat.CSV

    def test_detect_format_parquet(self, universal_reader, temp_parquet_file):
        """Test Parquet format detection."""
        detected = universal_reader.detect_format(temp_parquet_file)
        assert detected == DataFormat.PARQUET

    def test_detect_format_unknown(self, universal_reader):
        """Test unknown format detection."""
        with tempfile.NamedTemporaryFile(suffix='.xyz', delete=False) as f:
            f.write(b'random content')
            temp_path = f.name
        try:
            detected = universal_reader.detect_format(temp_path)
            assert detected == DataFormat.UNKNOWN
        finally:
            os.unlink(temp_path)

    def test_read_csv(self, universal_reader, temp_csv_file, sample_df):
        """Test reading CSV files."""
        result = universal_reader.read_file(temp_csv_file)
        assert result.success is True
        assert isinstance(result.dataframe, pd.DataFrame)
        assert len(result.dataframe) == len(sample_df)
        assert set(result.dataframe.columns) == set(sample_df.columns)

    def test_read_parquet(self, universal_reader, temp_parquet_file, sample_df):
        """Test reading Parquet files."""
        result = universal_reader.read_file(temp_parquet_file)
        assert result.success is True
        assert isinstance(result.dataframe, pd.DataFrame)
        assert len(result.dataframe) == len(sample_df)
        assert set(result.dataframe.columns) == set(sample_df.columns)

    def test_read_nonexistent_file(self, universal_reader):
        """Test reading a file that doesn't exist."""
        result = universal_reader.read_file('/nonexistent/path/file.csv')
        assert result.success is False
        assert result.error is not None
        assert 'not found' in result.error.lower()

    def test_read_result_has_schema_info(self, universal_reader, temp_csv_file):
        """Test that ReadResult includes schema information."""
        result = universal_reader.read_file(temp_csv_file)
        assert result.success is True
        assert result.schema_info is not None
        assert result.schema_info.row_count > 0
        assert result.schema_info.column_count > 0

    def test_read_result_has_file_metadata(self, universal_reader, temp_csv_file):
        """Test that ReadResult includes file metadata."""
        result = universal_reader.read_file(temp_csv_file)
        assert result.success is True
        assert result.file_metadata is not None
        assert result.file_metadata.size_bytes > 0
        assert result.file_metadata.format == DataFormat.CSV

    def test_extract_schema_info(self, universal_reader, sample_df):
        """Test schema info extraction from DataFrame."""
        schema_info = universal_reader.extract_schema_info(sample_df)
        assert schema_info is not None
        assert schema_info.row_count == 3
        assert schema_info.column_count == 4
        assert len(schema_info.columns) == 4

    def test_extract_schema_contains_columns(self, universal_reader, sample_df):
        """Test that extracted schema contains expected columns."""
        schema_info = universal_reader.extract_schema_info(sample_df)
        column_names = [c['name'] for c in schema_info.columns]
        expected_cols = ['USUBJID', 'AGE', 'SEX', 'RACE']
        for col in expected_cols:
            assert col in column_names, f"Column {col} not found in schema"

    def test_table_name_generation(self, universal_reader, temp_csv_file):
        """Test that table name is generated correctly."""
        result = universal_reader.read_file(temp_csv_file)
        assert result.success is True
        assert result.table_name is not None
        # Table name should be uppercase stem of filename
        assert result.table_name == result.table_name.upper()


class TestUniversalReaderEdgeCases:
    """Edge case tests for UniversalReader."""

    def test_read_empty_csv(self, universal_reader):
        """Test reading an empty CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('col1,col2,col3\n')  # Headers only
            temp_path = f.name
        try:
            result = universal_reader.read_file(temp_path)
            assert result.success is True
            assert isinstance(result.dataframe, pd.DataFrame)
            assert len(result.dataframe) == 0
            assert list(result.dataframe.columns) == ['col1', 'col2', 'col3']
        finally:
            os.unlink(temp_path)

    def test_read_csv_with_special_characters(self, universal_reader):
        """Test reading CSV with special characters in data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write('NAME,VALUE\n')
            f.write('Test & Co,100\n')
            f.write('"Quoted, Value",200\n')
            temp_path = f.name
        try:
            result = universal_reader.read_file(temp_path)
            assert result.success is True
            assert len(result.dataframe) == 2
            assert result.dataframe.iloc[0]['NAME'] == 'Test & Co'
        finally:
            os.unlink(temp_path)

    def test_read_csv_with_missing_values(self, universal_reader):
        """Test reading CSV with missing values."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('A,B,C\n')
            f.write('1,,3\n')
            f.write(',2,\n')
            f.write('1,2,3\n')
            temp_path = f.name
        try:
            result = universal_reader.read_file(temp_path)
            assert result.success is True
            assert len(result.dataframe) == 3
        finally:
            os.unlink(temp_path)

    def test_large_dataframe_read(self, universal_reader):
        """Test reading a larger DataFrame."""
        large_df = pd.DataFrame({
            'ID': range(10000),
            'VALUE': [f'val_{i}' for i in range(10000)],
            'NUMBER': [float(i) * 1.5 for i in range(10000)]
        })
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            parquet_path = f.name
        large_df.to_parquet(parquet_path, index=False)
        try:
            result = universal_reader.read_file(parquet_path)
            assert result.success is True
            assert len(result.dataframe) == 10000
            assert list(result.dataframe.columns) == ['ID', 'VALUE', 'NUMBER']
        finally:
            os.unlink(parquet_path)

    def test_get_supported_formats(self, universal_reader):
        """Test getting list of supported formats."""
        formats = universal_reader.get_supported_formats()
        assert isinstance(formats, list)
        assert len(formats) > 0
        # Should include common formats
        format_names = [f['format'] for f in formats]
        assert 'csv' in format_names
        assert 'parquet' in format_names

    def test_scan_directory(self, universal_reader, temp_csv_file):
        """Test scanning a directory for data files."""
        # Get the directory containing the temp file
        directory = os.path.dirname(temp_csv_file)
        files = universal_reader.scan_directory(directory)
        assert isinstance(files, list)
        # Should find at least our temp file
        filenames = [f.filename for f in files]
        assert os.path.basename(temp_csv_file) in filenames
