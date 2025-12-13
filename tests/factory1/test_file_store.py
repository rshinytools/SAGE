"""
Tests for FileStore - File status tracking component.
"""

import os
import pytest
import tempfile
import hashlib
import uuid
from datetime import datetime
from pathlib import Path

from core.data import FileStore, FileRecord, FileStatus, ProcessingStep


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


class TestFileStore:
    """Test suite for FileStore class."""

    def test_store_initialization(self, file_store):
        """Test that FileStore initializes correctly."""
        assert file_store is not None
        assert hasattr(file_store, 'save')
        assert hasattr(file_store, 'get')
        assert hasattr(file_store, 'update_status')

    def test_save_file_record(self, file_store, temp_csv_file):
        """Test saving a file record."""
        record = create_file_record(
            filename=os.path.basename(temp_csv_file),
            filepath=temp_csv_file,
            file_size=os.path.getsize(temp_csv_file)
        )
        result = file_store.save(record)
        assert result is not None
        assert result.id == record.id

    def test_get_file_record(self, file_store, temp_csv_file):
        """Test retrieving a file record."""
        record = create_file_record(
            filename=os.path.basename(temp_csv_file),
            filepath=temp_csv_file
        )
        file_store.save(record)

        retrieved = file_store.get(record.id)
        assert retrieved is not None
        assert retrieved.filename == record.filename

    def test_get_nonexistent_file(self, file_store):
        """Test getting a non-existent file record."""
        record = file_store.get('nonexistent-id-12345')
        assert record is None

    def test_update_status(self, file_store, temp_csv_file):
        """Test updating file status."""
        record = create_file_record(
            filename=os.path.basename(temp_csv_file),
            filepath=temp_csv_file
        )
        file_store.save(record)

        # Update status
        updated = file_store.update_status(record.id, FileStatus.COMPLETED)

        assert updated is not None
        assert updated.status == FileStatus.COMPLETED
        assert updated.processed_at is not None

    def test_update_status_with_error(self, file_store, temp_csv_file):
        """Test updating file status with error message."""
        record = create_file_record(
            filename=os.path.basename(temp_csv_file),
            filepath=temp_csv_file
        )
        file_store.save(record)

        # Update status with error
        updated = file_store.update_status(
            record.id,
            FileStatus.FAILED,
            error_message="Test error message"
        )

        assert updated is not None
        assert updated.status == FileStatus.FAILED
        assert updated.error_message == "Test error message"


class TestFileStoreListingAndFiltering:
    """Test listing and filtering functionality."""

    def test_list_all_files(self, file_store):
        """Test listing all files."""
        # Save multiple files
        for i in range(3):
            record = create_file_record(f'test_file_{i}.csv')
            file_store.save(record)

        files = file_store.list_all()
        assert len(files) >= 3

    def test_list_all_with_status_filter(self, file_store):
        """Test listing files with status filter."""
        # Save files with different statuses
        pending_record = create_file_record('pending.csv', status=FileStatus.PENDING)
        completed_record = create_file_record('completed.csv', status=FileStatus.COMPLETED)

        file_store.save(pending_record)
        file_store.save(completed_record)

        # List only completed
        completed_files = file_store.list_all(status=FileStatus.COMPLETED)
        assert all(f.status == FileStatus.COMPLETED for f in completed_files)

    def test_get_by_table(self, file_store):
        """Test getting files by table name."""
        record1 = create_file_record('dm.csv')
        record2 = create_file_record('dm_v2.csv')
        record2.table_name = 'DM'  # Same table

        file_store.save(record1)
        file_store.save(record2)

        dm_files = file_store.get_by_table('DM')
        assert len(dm_files) >= 1

    def test_get_current(self, file_store):
        """Test getting current (completed) file for table."""
        record = create_file_record('current_test.csv', status=FileStatus.COMPLETED)
        file_store.save(record)

        current = file_store.get_current(record.table_name)
        assert current is not None
        assert current.status == FileStatus.COMPLETED

    def test_get_by_hash(self, file_store):
        """Test getting file by hash."""
        record = create_file_record('hash_test.csv')
        file_store.save(record)

        found = file_store.get_by_hash(record.file_hash)
        assert found is not None
        assert found.file_hash == record.file_hash


class TestFileStoreStatistics:
    """Test statistics functionality."""

    def test_get_statistics(self, file_store):
        """Test getting file statistics."""
        # Add some test files
        for i in range(5):
            record = create_file_record(f'stat_file_{i}.csv', file_size=1000 * (i + 1))
            file_store.save(record)

        stats = file_store.get_statistics()
        assert stats is not None
        assert isinstance(stats, dict)
        assert 'total_files' in stats
        assert 'status_counts' in stats

    def test_get_table_summary(self, file_store):
        """Test getting table summary."""
        record = create_file_record('summary_test.csv', status=FileStatus.COMPLETED)
        record.row_count = 100
        record.column_count = 10
        file_store.save(record)

        summary = file_store.get_table_summary()
        assert isinstance(summary, list)


class TestFileStoreDeletion:
    """Test deletion functionality."""

    def test_delete_file_record(self, file_store):
        """Test deleting a file record."""
        record = create_file_record('to_delete.csv')
        file_store.save(record)

        # Verify it exists
        assert file_store.get(record.id) is not None

        # Delete it
        result = file_store.delete(record.id)
        assert result is True

        # Verify it's gone
        assert file_store.get(record.id) is None

    def test_delete_nonexistent(self, file_store):
        """Test deleting a non-existent file record."""
        result = file_store.delete('nonexistent-id')
        assert result is False


class TestFileStoreArchiving:
    """Test archiving functionality."""

    def test_archive_previous(self, file_store):
        """Test archiving previous files for a table."""
        # Create multiple completed files for same table
        record1 = create_file_record('archive_1.csv', status=FileStatus.COMPLETED)
        record1.table_name = 'ARCHIVE_TEST'
        record2 = create_file_record('archive_2.csv', status=FileStatus.COMPLETED)
        record2.table_name = 'ARCHIVE_TEST'

        file_store.save(record1)
        file_store.save(record2)

        # Archive previous (except record2)
        archived_count = file_store.archive_previous('ARCHIVE_TEST', record2.id)

        # Verify record1 is now archived
        updated_record1 = file_store.get(record1.id)
        assert updated_record1.status == FileStatus.ARCHIVED

        # Verify record2 is still completed
        updated_record2 = file_store.get(record2.id)
        assert updated_record2.status == FileStatus.COMPLETED


class TestFileStoreProcessingSteps:
    """Test processing step tracking."""

    def test_update_processing_step(self, file_store):
        """Test updating a processing step."""
        record = create_file_record('step_test.csv')
        file_store.save(record)

        # Add a processing step
        step = ProcessingStep(
            step_name='validate',
            status='running',
            started_at=datetime.now().isoformat()
        )

        updated = file_store.update_processing_step(record.id, step)
        assert updated is not None
        assert len(updated.processing_steps) == 1
        assert updated.processing_steps[0].step_name == 'validate'

    def test_update_existing_processing_step(self, file_store):
        """Test updating an existing processing step."""
        record = create_file_record('step_update_test.csv')
        file_store.save(record)

        # Add initial step
        step1 = ProcessingStep(
            step_name='validate',
            status='running',
            started_at=datetime.now().isoformat()
        )
        file_store.update_processing_step(record.id, step1)

        # Update the same step
        step2 = ProcessingStep(
            step_name='validate',
            status='completed',
            started_at=step1.started_at,
            completed_at=datetime.now().isoformat()
        )
        updated = file_store.update_processing_step(record.id, step2)

        # Should still have only one step, but updated
        assert len(updated.processing_steps) == 1
        assert updated.processing_steps[0].status == 'completed'


class TestFileStorePersistence:
    """Test database persistence."""

    def test_persistence_across_instances(self, temp_db_path):
        """Test that file data persists across store instances."""
        # Create first instance and save file
        store1 = FileStore(temp_db_path)
        record = create_file_record('persist_test.csv')
        store1.save(record)

        # Create second instance and verify data persists
        store2 = FileStore(temp_db_path)
        retrieved = store2.get(record.id)
        assert retrieved is not None
        assert retrieved.filename == record.filename

    def test_concurrent_access(self, temp_db_path):
        """Test concurrent access to file store."""
        store1 = FileStore(temp_db_path)
        store2 = FileStore(temp_db_path)

        # Both instances should be able to save
        record1 = create_file_record('concurrent_1.csv')
        record2 = create_file_record('concurrent_2.csv')

        store1.save(record1)
        store2.save(record2)

        # Both should see both files
        assert store1.get(record2.id) is not None
        assert store2.get(record1.id) is not None
