"""
Integration tests for Factory 1 - End-to-end workflow tests.
"""

import os
import pytest
import pandas as pd
import tempfile
import hashlib
import uuid
from pathlib import Path
from datetime import datetime

from core.data import (
    UniversalReader,
    SchemaTracker,
    FileStore,
    DuckDBLoader,
    FileRecord,
    FileStatus
)


def create_file_record(filename: str, filepath: str = '/tmp/test.csv',
                       file_format: str = 'csv', file_size: int = 1000,
                       status: FileStatus = FileStatus.PENDING) -> FileRecord:
    """Helper to create a FileRecord for testing."""
    file_hash = hashlib.sha256(filepath.encode()).hexdigest()
    return FileRecord(
        id=str(uuid.uuid4()),
        filename=filename,
        table_name=Path(filename).stem.upper(),
        file_format=file_format,
        file_size=file_size,
        file_hash=file_hash,
        status=status,
        uploaded_at=datetime.now().isoformat()
    )


class TestEndToEndWorkflow:
    """End-to-end workflow tests for Factory 1."""

    def test_full_pipeline_csv(self, universal_reader, schema_tracker, file_store, duckdb_loader, temp_csv_file):
        """Test complete pipeline: read CSV -> track schema -> store file -> load to DuckDB."""
        # Step 1: Read the file
        read_result = universal_reader.read_file(temp_csv_file)
        assert read_result.success is True
        df = read_result.dataframe
        assert len(df) > 0

        # Step 2: Track the schema
        table_name = 'csv_pipeline_test'
        version = schema_tracker.record_version(
            table_name=table_name,
            df=df,
            source_file=temp_csv_file
        )
        assert version is not None

        # Step 3: Store file record
        record = create_file_record(
            filename=os.path.basename(temp_csv_file),
            filepath=temp_csv_file,
            file_size=os.path.getsize(temp_csv_file)
        )
        file_store.save(record)

        # Step 4: Load to DuckDB
        load_result = duckdb_loader.load_dataframe(df, table_name)
        assert load_result.success is True

        # Step 5: Update file status
        file_store.update_status(record.id, FileStatus.COMPLETED)

        # Verify final state
        retrieved = file_store.get(record.id)
        assert retrieved is not None
        assert retrieved.status == FileStatus.COMPLETED

        tables = duckdb_loader.list_tables()
        assert table_name.upper() in tables

    def test_full_pipeline_parquet(self, universal_reader, schema_tracker, file_store, duckdb_loader, temp_parquet_file):
        """Test complete pipeline with Parquet file."""
        # Step 1: Read the file
        read_result = universal_reader.read_file(temp_parquet_file)
        assert read_result.success is True
        df = read_result.dataframe

        # Step 2: Track the schema
        table_name = 'parquet_pipeline_test'
        version = schema_tracker.record_version(
            table_name=table_name,
            df=df,
            source_file=temp_parquet_file
        )
        assert version is not None

        # Step 3: Store file record
        record = create_file_record(
            filename=os.path.basename(temp_parquet_file),
            filepath=temp_parquet_file,
            file_format='parquet'
        )
        file_store.save(record)

        # Step 4: Load to DuckDB
        load_result = duckdb_loader.load_dataframe(df, table_name)
        assert load_result.success is True

        # Verify
        tables = duckdb_loader.list_tables()
        assert table_name.upper() in tables


class TestSchemaEvolution:
    """Test schema evolution scenarios."""

    def test_schema_evolution_add_column(self, schema_tracker, duckdb_loader, temp_csv_file):
        """Test handling of schema evolution when column is added."""
        # Initial schema
        df1 = pd.DataFrame({
            'ID': [1, 2, 3],
            'NAME': ['A', 'B', 'C']
        })

        schema_tracker.record_version(
            table_name='evolving_table',
            df=df1,
            source_file=temp_csv_file
        )
        duckdb_loader.load_dataframe(df1, 'evolving_table')

        # Evolved schema with new column
        df2 = pd.DataFrame({
            'ID': [4, 5, 6],
            'NAME': ['D', 'E', 'F'],
            'NEW_FIELD': ['X', 'Y', 'Z']
        })

        # Compare with previous
        diff = schema_tracker.compare_with_previous('evolving_table', df2)
        assert diff.has_changes is True
        assert len(diff.added_columns) == 1

        # Record new version
        schema_tracker.record_version(
            table_name='evolving_table',
            df=df2,
            source_file=temp_csv_file
        )
        duckdb_loader.load_dataframe(df2, 'evolving_table')

        # Verify
        info = duckdb_loader.get_table_info('evolving_table')
        assert info is not None
        assert info.column_count == 3


class TestMultipleFilesWorkflow:
    """Test workflows involving multiple files."""

    def test_process_multiple_files(self, universal_reader, schema_tracker, file_store, duckdb_loader, sample_df):
        """Test processing multiple files in sequence."""
        files_data = []

        # Create multiple test files
        for i in range(3):
            df = sample_df.copy()
            df['BATCH'] = i

            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
                parquet_path = f.name
            df.to_parquet(parquet_path, index=False)
            files_data.append((parquet_path, f'batch_table_{i}'))

        try:
            # Process each file
            for filepath, table_name in files_data:
                read_result = universal_reader.read_file(filepath)
                assert read_result.success is True

                schema_tracker.record_version(
                    table_name=table_name,
                    df=read_result.dataframe,
                    source_file=filepath
                )
                duckdb_loader.load_dataframe(read_result.dataframe, table_name)

                record = create_file_record(
                    filename=os.path.basename(filepath),
                    filepath=filepath,
                    file_format='parquet',
                    file_size=os.path.getsize(filepath)
                )
                file_store.save(record)

            # Verify all tables exist
            tables = duckdb_loader.list_tables()
            for _, table_name in files_data:
                assert table_name.upper() in tables

        finally:
            # Cleanup
            for filepath, _ in files_data:
                if os.path.exists(filepath):
                    os.unlink(filepath)


class TestErrorHandling:
    """Test error handling in workflows."""

    def test_missing_file_handling(self, universal_reader):
        """Test handling of missing files."""
        result = universal_reader.read_file('/nonexistent/path/to/file.csv')
        assert result.success is False
        assert result.error is not None

    def test_empty_dataframe_handling(self, schema_tracker, duckdb_loader, temp_csv_file):
        """Test handling of empty DataFrames."""
        empty_df = pd.DataFrame({'A': [], 'B': []})

        # Should handle empty DataFrame
        version = schema_tracker.record_version(
            table_name='empty_table',
            df=empty_df,
            source_file=temp_csv_file
        )
        assert version is not None
        assert version.row_count == 0

        result = duckdb_loader.load_dataframe(empty_df, 'empty_table')
        assert result.success is True
        assert result.rows_loaded == 0


class TestConcurrentOperations:
    """Test concurrent operation scenarios."""

    def test_concurrent_schema_tracking(self, temp_db_path, sample_df, temp_csv_file):
        """Test concurrent schema tracking operations."""
        tracker1 = SchemaTracker(temp_db_path)
        tracker2 = SchemaTracker(temp_db_path)

        # Both should be able to record schemas
        v1 = tracker1.record_version(
            table_name='concurrent_table',
            df=sample_df,
            source_file=temp_csv_file
        )

        modified_df = sample_df.copy()
        modified_df['EXTRA'] = 'value'
        v2 = tracker2.record_version(
            table_name='concurrent_table',
            df=modified_df,
            source_file=temp_csv_file
        )

        # Both should succeed
        assert v1 is not None
        assert v2 is not None
        assert v2.version_number > v1.version_number

    def test_concurrent_file_store_operations(self, temp_db_path):
        """Test concurrent file store operations."""
        store1 = FileStore(temp_db_path)
        store2 = FileStore(temp_db_path)

        # Both should be able to save
        record1 = create_file_record('concurrent_1.csv')
        record2 = create_file_record('concurrent_2.csv')

        store1.save(record1)
        store2.save(record2)

        # Both should see the files
        assert store1.get(record2.id) is not None
        assert store2.get(record1.id) is not None


class TestDataQuality:
    """Test data quality checks in workflow."""

    def test_schema_consistency_check(self, universal_reader, schema_tracker, sample_df, temp_csv_file):
        """Test schema consistency checking."""
        # Record initial schema
        schema_tracker.record_version(
            table_name='quality_table',
            df=sample_df,
            source_file=temp_csv_file
        )

        # Read the same file
        read_result = universal_reader.read_file(temp_csv_file)
        df = read_result.dataframe

        # Columns should match
        assert set(df.columns) == set(sample_df.columns)

    def test_null_handling_in_pipeline(self, universal_reader, schema_tracker, duckdb_loader, temp_csv_file):
        """Test null value handling throughout pipeline."""
        df_with_nulls = pd.DataFrame({
            'ID': [1, 2, 3, 4],
            'VALUE': [10.0, None, 30.0, None],
            'NAME': ['A', None, 'C', None]
        })

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            parquet_path = f.name
        df_with_nulls.to_parquet(parquet_path, index=False)

        try:
            # Read back
            read_result = universal_reader.read_file(parquet_path)
            assert read_result.success is True
            df = read_result.dataframe

            # Should preserve nulls
            assert df['VALUE'].isna().sum() == 2
            assert df['NAME'].isna().sum() == 2

            # Track and load
            schema_tracker.record_version(
                table_name='null_test_table',
                df=df,
                source_file=parquet_path
            )
            duckdb_loader.load_dataframe(df, 'null_test_table')

            # Query to verify
            result = duckdb_loader.query("SELECT COUNT(*) FROM NULL_TEST_TABLE WHERE VALUE IS NULL")
            assert result.iloc[0, 0] == 2
        finally:
            os.unlink(parquet_path)


class TestVersionTracking:
    """Test version tracking across components."""

    def test_schema_version_history(self, schema_tracker, temp_csv_file):
        """Test that schema versions are properly tracked."""
        base_df = pd.DataFrame({
            'ID': [1, 2, 3],
            'NAME': ['A', 'B', 'C']
        })

        # Create multiple versions, each adding a column to the previous
        df = base_df.copy()
        for i in range(3):
            df[f'COL_{i}'] = f'v{i}'
            schema_tracker.record_version(
                table_name='version_test',
                df=df.copy(),
                source_file=temp_csv_file
            )

        # Get history
        history = schema_tracker.get_version_history('version_test')
        assert len(history) == 3

        # Latest should have most columns (ID, NAME, COL_0, COL_1, COL_2)
        current = schema_tracker.get_current_version('version_test')
        assert current.version_number == 3
        assert current.column_count == 5  # ID, NAME, COL_0, COL_1, COL_2

    def test_file_store_history(self, file_store):
        """Test that file history is tracked per table."""
        # Upload multiple files for same table
        for i in range(3):
            record = create_file_record(f'dm_v{i}.csv')
            record.table_name = 'DM'
            file_store.save(record)

        # Get files for table
        dm_files = file_store.get_by_table('DM')
        assert len(dm_files) >= 3
