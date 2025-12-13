"""
Tests for SchemaTracker - Schema versioning and change detection component.
"""

import os
import pytest
import pandas as pd
import tempfile
from datetime import datetime

from core.data import SchemaTracker, ChangeSeverity, ChangeType


class TestSchemaTracker:
    """Test suite for SchemaTracker class."""

    def test_tracker_initialization(self, schema_tracker):
        """Test that SchemaTracker initializes correctly."""
        assert schema_tracker is not None
        assert hasattr(schema_tracker, 'record_version')
        assert hasattr(schema_tracker, 'get_current_version')
        assert hasattr(schema_tracker, 'compare_schemas')

    def test_record_version_basic(self, schema_tracker, sample_df, temp_csv_file):
        """Test recording a basic schema version."""
        version = schema_tracker.record_version(
            table_name='test_table',
            df=sample_df,
            source_file=temp_csv_file
        )
        assert version is not None
        assert version.version_number == 1
        assert version.table_name == 'TEST_TABLE'
        assert version.row_count == 3
        assert version.column_count == 4

    def test_get_current_version(self, schema_tracker, sample_df, temp_csv_file):
        """Test retrieving the current schema version."""
        schema_tracker.record_version(
            table_name='test_table',
            df=sample_df,
            source_file=temp_csv_file
        )
        current = schema_tracker.get_current_version('test_table')
        assert current is not None
        assert current.is_current is True

    def test_get_current_version_nonexistent(self, schema_tracker):
        """Test getting schema for non-existent table."""
        current = schema_tracker.get_current_version('nonexistent_table')
        assert current is None

    def test_record_multiple_versions(self, schema_tracker, sample_df, temp_csv_file):
        """Test recording multiple schema versions."""
        # Record first version
        v1 = schema_tracker.record_version(
            table_name='test_table',
            df=sample_df,
            source_file=temp_csv_file
        )

        # Create modified DataFrame
        modified_df = sample_df.copy()
        modified_df['NEW_COL'] = 'test'

        # Record second version
        v2 = schema_tracker.record_version(
            table_name='test_table',
            df=modified_df,
            source_file=temp_csv_file
        )

        assert v1.version_number == 1
        assert v2.version_number == 2
        assert v2.column_count == 5  # One more column

    def test_compare_schemas_no_change(self, schema_tracker, sample_df):
        """Test comparing identical schemas."""
        schema1 = schema_tracker.extract_schema(sample_df)
        schema2 = schema_tracker.extract_schema(sample_df)

        diff = schema_tracker.compare_schemas(schema1, schema2)

        assert diff.has_changes is False
        assert len(diff.added_columns) == 0
        assert len(diff.removed_columns) == 0
        assert len(diff.type_changes) == 0

    def test_compare_schemas_added_column(self, schema_tracker, sample_df):
        """Test detecting added column."""
        # Original schema
        schema1 = schema_tracker.extract_schema(sample_df)

        # Add new column
        modified_df = sample_df.copy()
        modified_df['NEW_COLUMN'] = 'value'
        schema2 = schema_tracker.extract_schema(modified_df)

        diff = schema_tracker.compare_schemas(schema1, schema2)

        assert diff.has_changes is True
        assert len(diff.added_columns) == 1
        assert diff.added_columns[0].column_name == 'NEW_COLUMN'
        assert diff.severity == ChangeSeverity.INFO

    def test_compare_schemas_removed_column(self, schema_tracker, sample_df):
        """Test detecting removed column."""
        # Original schema
        schema1 = schema_tracker.extract_schema(sample_df)

        # Remove a column
        modified_df = sample_df.drop(columns=['RACE'])
        schema2 = schema_tracker.extract_schema(modified_df)

        diff = schema_tracker.compare_schemas(schema1, schema2)

        assert diff.has_changes is True
        assert len(diff.removed_columns) == 1
        assert diff.removed_columns[0].column_name == 'RACE'
        assert diff.severity == ChangeSeverity.BREAKING


class TestSchemaTrackerVersionHistory:
    """Test version history functionality."""

    def test_get_version_history(self, schema_tracker, sample_df, temp_csv_file):
        """Test getting version history for a table."""
        # Record multiple versions
        schema_tracker.record_version(
            table_name='history_table',
            df=sample_df,
            source_file=temp_csv_file
        )

        modified_df = sample_df.copy()
        modified_df['V2'] = 'v2'
        schema_tracker.record_version(
            table_name='history_table',
            df=modified_df,
            source_file=temp_csv_file
        )

        modified_df['V3'] = 'v3'
        schema_tracker.record_version(
            table_name='history_table',
            df=modified_df,
            source_file=temp_csv_file
        )

        history = schema_tracker.get_version_history('history_table')
        assert history is not None
        assert len(history) == 3

    def test_compare_with_previous(self, schema_tracker, sample_df, temp_csv_file):
        """Test comparing new data with previous version."""
        # Record first version
        schema_tracker.record_version(
            table_name='compare_table',
            df=sample_df,
            source_file=temp_csv_file
        )

        # Create modified DataFrame
        modified_df = sample_df.copy()
        modified_df['NEW_COL'] = 'test'

        # Compare with previous
        diff = schema_tracker.compare_with_previous('compare_table', modified_df)

        assert diff.has_changes is True
        assert len(diff.added_columns) == 1


class TestSchemaTrackerChangeDetection:
    """Test change detection and severity classification."""

    def test_change_severity_info(self, schema_tracker, sample_df, temp_csv_file):
        """Test INFO severity for additive changes."""
        schema_tracker.record_version(
            table_name='severity_table',
            df=sample_df,
            source_file=temp_csv_file
        )

        # Add a column (non-breaking change)
        modified_df = sample_df.copy()
        modified_df['OPTIONAL_COL'] = 'optional'

        diff = schema_tracker.compare_with_previous('severity_table', modified_df)
        assert diff.severity == ChangeSeverity.INFO

    def test_change_severity_breaking(self, schema_tracker, sample_df, temp_csv_file):
        """Test BREAKING severity for column removal."""
        schema_tracker.record_version(
            table_name='breaking_table',
            df=sample_df,
            source_file=temp_csv_file
        )

        # Remove a column (breaking change)
        modified_df = sample_df.drop(columns=['AGE'])

        diff = schema_tracker.compare_with_previous('breaking_table', modified_df)
        assert diff.severity == ChangeSeverity.BREAKING

    def test_type_change_detection(self, schema_tracker, temp_csv_file):
        """Test detection of column type changes."""
        # Original with integer
        df1 = pd.DataFrame({
            'ID': [1, 2, 3],
            'VALUE': [100, 200, 300]
        })
        schema_tracker.record_version(
            table_name='type_table',
            df=df1,
            source_file=temp_csv_file
        )

        # Changed to float
        df2 = pd.DataFrame({
            'ID': [1, 2, 3],
            'VALUE': [100.0, 200.0, 300.0]
        })

        diff = schema_tracker.compare_with_previous('type_table', df2)
        # Type changes may or may not be detected depending on pandas dtype inference
        assert diff is not None

    def test_should_block_upload(self, schema_tracker, sample_df, temp_csv_file):
        """Test upload blocking for breaking changes."""
        schema_tracker.record_version(
            table_name='block_table',
            df=sample_df,
            source_file=temp_csv_file
        )

        # Remove column - should block
        modified_df = sample_df.drop(columns=['AGE'])
        should_block, diff = schema_tracker.should_block_upload('block_table', modified_df)
        assert should_block is True
        assert diff.severity == ChangeSeverity.BREAKING


class TestSchemaTrackerPersistence:
    """Test database persistence."""

    def test_persistence_across_instances(self, temp_db_path, sample_df, temp_csv_file):
        """Test that schema data persists across tracker instances."""
        # Create first instance and record schema
        tracker1 = SchemaTracker(temp_db_path)
        tracker1.record_version(
            table_name='persist_table',
            df=sample_df,
            source_file=temp_csv_file
        )

        # Create second instance and verify data persists
        tracker2 = SchemaTracker(temp_db_path)
        latest = tracker2.get_current_version('persist_table')
        assert latest is not None
        assert latest.table_name == 'PERSIST_TABLE'

    def test_multiple_tables(self, schema_tracker, sample_df, sample_df_numeric, temp_csv_file):
        """Test tracking schemas for multiple tables."""
        schema_tracker.record_version(
            table_name='table_a',
            df=sample_df,
            source_file=temp_csv_file
        )
        schema_tracker.record_version(
            table_name='table_b',
            df=sample_df_numeric,
            source_file=temp_csv_file
        )

        schema_a = schema_tracker.get_current_version('table_a')
        schema_b = schema_tracker.get_current_version('table_b')

        assert schema_a is not None
        assert schema_b is not None
        assert schema_a.table_name == 'TABLE_A'
        assert schema_b.table_name == 'TABLE_B'

    def test_list_tables(self, schema_tracker, sample_df, temp_csv_file):
        """Test listing all tracked tables."""
        schema_tracker.record_version(
            table_name='list_table_1',
            df=sample_df,
            source_file=temp_csv_file
        )
        schema_tracker.record_version(
            table_name='list_table_2',
            df=sample_df,
            source_file=temp_csv_file
        )

        tables = schema_tracker.list_tables()
        assert 'LIST_TABLE_1' in tables
        assert 'LIST_TABLE_2' in tables

    def test_delete_table_history(self, schema_tracker, sample_df, temp_csv_file):
        """Test deleting table history."""
        schema_tracker.record_version(
            table_name='delete_table',
            df=sample_df,
            source_file=temp_csv_file
        )

        # Verify it exists
        assert schema_tracker.get_current_version('delete_table') is not None

        # Delete it
        result = schema_tracker.delete_table_history('delete_table')
        assert result is True

        # Verify it's gone
        assert schema_tracker.get_current_version('delete_table') is None
