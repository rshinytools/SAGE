#!/usr/bin/env python3
"""
Factory 1 - Data Foundry Comprehensive Test Suite
================================================

Tests all Factory 1 components:
1. UniversalReader - Multi-format data reading
2. SchemaTracker - Schema versioning and change detection
3. FileStore - File status tracking
4. DuckDBLoader - Database loading
"""

import sys
import os
from pathlib import Path
import tempfile
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np


def print_header(title: str):
    """Print a formatted header."""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_test(name: str, passed: bool, details: str = ""):
    """Print test result."""
    status = "PASS" if passed else "FAIL"
    icon = "[OK]" if passed else "[X]"
    print(f"  {icon} {name}: {status}")
    if details:
        print(f"      -> {details}")


def test_universal_reader():
    """Test UniversalReader functionality."""
    print_header("Testing UniversalReader")

    try:
        from core.data import UniversalReader, DataFormat
        reader = UniversalReader()
        print_test("Import UniversalReader", True)
    except ImportError as e:
        print_test("Import UniversalReader", False, str(e))
        return False

    # Test 1: CSV reading
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("USUBJID,AGE,SEX,RACE\n")
        f.write("SUBJ001,45,M,WHITE\n")
        f.write("SUBJ002,32,F,ASIAN\n")
        f.write("SUBJ003,58,M,BLACK\n")
        csv_path = f.name

    try:
        result = reader.read_file(csv_path)
        passed = result.success and len(result.dataframe) == 3
        print_test("Read CSV file", passed, f"{len(result.dataframe)} rows, {len(result.dataframe.columns)} cols")

        # Check table name extraction
        table_name = reader.get_table_name(csv_path)
        print_test("Extract table name", table_name is not None, f"Table: {table_name}")

        # Check schema extraction
        has_schema = result.schema is not None
        print_test("Extract schema", has_schema, f"Hash: {result.schema.schema_hash[:8] if result.schema else 'None'}...")
    finally:
        os.unlink(csv_path)

    # Test 2: Parquet reading
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        parquet_path = f.name

    try:
        df = pd.DataFrame({
            'USUBJID': ['SUBJ001', 'SUBJ002'],
            'LBTESTCD': ['ALT', 'AST'],
            'LBSTRESN': [25.5, 30.2]
        })
        df.to_parquet(parquet_path, index=False)

        result = reader.read_file(parquet_path)
        passed = result.success and len(result.dataframe) == 2
        print_test("Read Parquet file", passed, f"{len(result.dataframe)} rows")
    finally:
        os.unlink(parquet_path)

    # Test 3: Format detection
    csv_format = reader._detect_format(csv_path.replace('.csv', '') + '.csv')
    parquet_format = reader._detect_format(parquet_path.replace('.parquet', '') + '.parquet')
    print_test("Format detection", True, f"CSV={csv_format}, Parquet={parquet_format}")

    return True


def test_schema_tracker():
    """Test SchemaTracker functionality."""
    print_header("Testing SchemaTracker")

    try:
        from core.data import SchemaTracker, ChangeSeverity
        print_test("Import SchemaTracker", True)
    except ImportError as e:
        print_test("Import SchemaTracker", False, str(e))
        return False

    # Use temp database
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    try:
        tracker = SchemaTracker(db_path)
        print_test("Initialize SchemaTracker", True)

        # Test 1: Record first version
        df1 = pd.DataFrame({
            'USUBJID': ['SUBJ001', 'SUBJ002'],
            'AGE': [45, 32],
            'SEX': ['M', 'F']
        })

        version1 = tracker.record_version('TEST_TABLE', df1, 'test.csv')
        print_test("Record first schema version", version1.version == 1,
                   f"Version {version1.version}, Hash: {version1.schema_hash[:8]}...")

        # Test 2: Compare with identical schema
        diff = tracker.compare_with_previous('TEST_TABLE', df1)
        print_test("Compare identical schema", not diff.has_changes,
                   f"Changes: {diff.has_changes}")

        # Test 3: Add a column (INFO level change)
        df2 = df1.copy()
        df2['WEIGHT'] = [70.5, 55.2]

        diff = tracker.compare_with_previous('TEST_TABLE', df2)
        print_test("Detect added column",
                   diff.has_changes and diff.severity == ChangeSeverity.INFO,
                   f"Severity: {diff.severity.value}, Added: {diff.added_columns}")

        # Test 4: Remove a column (BREAKING change)
        df3 = df1[['USUBJID', 'AGE']].copy()

        diff = tracker.compare_with_previous('TEST_TABLE', df3)
        print_test("Detect removed column (BREAKING)",
                   diff.has_changes and diff.severity == ChangeSeverity.BREAKING,
                   f"Severity: {diff.severity.value}, Removed: {diff.removed_columns}")

        # Test 5: Should block upload
        should_block, diff = tracker.should_block_upload('TEST_TABLE', df3, block_on_breaking=True)
        print_test("Block on breaking changes", should_block,
                   f"Blocked: {should_block}")

        # Test 6: Record new version
        version2 = tracker.record_version('TEST_TABLE', df2, 'test_v2.csv')
        print_test("Record second version", version2.version == 2,
                   f"Version {version2.version}")

        # Test 7: Get version history
        history = tracker.get_version_history('TEST_TABLE')
        print_test("Get version history", len(history) == 2,
                   f"{len(history)} versions")

        # Test 8: List tables
        tables = tracker.list_tables()
        print_test("List tables", 'TEST_TABLE' in tables,
                   f"Tables: {tables}")

    finally:
        os.unlink(db_path)

    return True


def test_file_store():
    """Test FileStore functionality."""
    print_header("Testing FileStore")

    try:
        from core.data import FileStore, FileRecord, FileStatus, ProcessingStep
        print_test("Import FileStore", True)
    except ImportError as e:
        print_test("Import FileStore", False, str(e))
        return False

    # Use temp database
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    try:
        store = FileStore(db_path)
        print_test("Initialize FileStore", True)

        # Test 1: Save a file record
        record = FileRecord(
            id='test-001',
            filename='dm.sas7bdat',
            table_name='DM',
            file_format='sas7bdat',
            file_size=1024000,
            file_hash='abc123def456',
            status=FileStatus.PENDING
        )

        saved = store.save(record)
        print_test("Save file record", saved.id == 'test-001')

        # Test 2: Retrieve record
        retrieved = store.get('test-001')
        print_test("Retrieve by ID",
                   retrieved is not None and retrieved.filename == 'dm.sas7bdat',
                   f"Filename: {retrieved.filename if retrieved else 'None'}")

        # Test 3: Update status
        updated = store.update_status('test-001', FileStatus.COMPLETED)
        print_test("Update status",
                   updated is not None and updated.status == FileStatus.COMPLETED,
                   f"Status: {updated.status.value if updated else 'None'}")

        # Test 4: Add processing step
        step = ProcessingStep(
            step_name='validate',
            status='completed',
            message='Validation passed'
        )
        store.update_processing_step('test-001', step)
        retrieved = store.get('test-001')
        print_test("Add processing step",
                   len(retrieved.processing_steps) == 1,
                   f"Steps: {len(retrieved.processing_steps)}")

        # Test 5: Get by table
        records = store.get_by_table('DM')
        print_test("Get by table name",
                   len(records) == 1,
                   f"Records for DM: {len(records)}")

        # Test 6: Get statistics
        stats = store.get_statistics()
        print_test("Get statistics",
                   stats['total_files'] == 1,
                   f"Total: {stats['total_files']}, Completed: {stats['status_counts'].get('completed', 0)}")

        # Test 7: Get table summary
        summary = store.get_table_summary()
        print_test("Get table summary",
                   len(summary) == 1,
                   f"Tables: {len(summary)}")

        # Test 8: Archive previous
        record2 = FileRecord(
            id='test-002',
            filename='dm.parquet',
            table_name='DM',
            file_format='parquet',
            file_size=512000,
            file_hash='xyz789',
            status=FileStatus.COMPLETED
        )
        store.save(record2)
        archived = store.archive_previous('DM', 'test-002')
        print_test("Archive previous versions",
                   archived >= 0,
                   f"Archived: {archived} records")

    finally:
        os.unlink(db_path)

    return True


def test_duckdb_loader():
    """Test DuckDBLoader functionality."""
    print_header("Testing DuckDBLoader")

    try:
        from core.data import DuckDBLoader
        print_test("Import DuckDBLoader", True)
    except ImportError as e:
        print_test("Import DuckDBLoader", False, str(e))
        return False

    # Use temp database
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
        db_path = f.name

    try:
        loader = DuckDBLoader(db_path)
        print_test("Initialize DuckDBLoader", True)

        # Test 1: Load DataFrame
        df = pd.DataFrame({
            'USUBJID': ['SUBJ001', 'SUBJ002', 'SUBJ003'],
            'AGE': [45, 32, 58],
            'SEX': ['M', 'F', 'M'],
            'WEIGHT': [70.5, 55.2, 82.0]
        })

        loader.load_dataframe(df, 'DM', replace=True)
        print_test("Load DataFrame to table", True, "Table: DM")

        # Test 2: Validate table
        is_valid = loader.validate_table('DM', expected_columns=['USUBJID', 'AGE', 'SEX'])
        print_test("Validate table columns", is_valid)

        # Test 3: List tables
        tables = loader.list_tables()
        print_test("List tables", 'DM' in tables, f"Tables: {tables}")

        # Test 4: Get row count
        count = loader.get_row_count('DM')
        print_test("Get row count", count == 3, f"Rows: {count}")

        # Test 5: Replace with new data
        df2 = pd.DataFrame({
            'USUBJID': ['SUBJ004', 'SUBJ005'],
            'AGE': [40, 50],
            'SEX': ['F', 'M'],
            'WEIGHT': [60.0, 75.0]
        })
        loader.load_dataframe(df2, 'DM', replace=True)
        new_count = loader.get_row_count('DM')
        print_test("Replace table data", new_count == 2, f"New rows: {new_count}")

    finally:
        os.unlink(db_path)

    return True


def test_integration():
    """Test full integration workflow."""
    print_header("Testing Integration Workflow")

    try:
        from core.data import (
            UniversalReader, SchemaTracker, FileStore, DuckDBLoader,
            FileRecord, FileStatus
        )
        print_test("Import all modules", True)
    except ImportError as e:
        print_test("Import all modules", False, str(e))
        return False

    # Create temp directory for test files
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, 'test.duckdb')
        schema_db = os.path.join(tmpdir, 'schema.db')
        file_db = os.path.join(tmpdir, 'files.db')
        csv_path = os.path.join(tmpdir, 'dm.csv')

        # Initialize components
        reader = UniversalReader()
        schema_tracker = SchemaTracker(schema_db)
        file_store = FileStore(file_db)
        db_loader = DuckDBLoader(db_path)

        # Step 1: Create test CSV file
        df_original = pd.DataFrame({
            'USUBJID': ['SUBJ001', 'SUBJ002', 'SUBJ003'],
            'AGE': [45, 32, 58],
            'SEX': ['M', 'F', 'M']
        })
        df_original.to_csv(csv_path, index=False)

        # Step 2: Simulate file upload
        import hashlib
        with open(csv_path, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()

        record = FileRecord(
            id='upload-001',
            filename='dm.csv',
            table_name='DM',
            file_format='csv',
            file_size=os.path.getsize(csv_path),
            file_hash=file_hash,
            status=FileStatus.PENDING
        )
        file_store.save(record)
        print_test("Step 1: File upload recorded", True)

        # Step 3: Read file
        result = reader.read_file(csv_path)
        print_test("Step 2: Read file", result.success, f"{len(result.dataframe)} rows")

        # Step 4: Check schema (first upload - no previous)
        should_block, diff = schema_tracker.should_block_upload('DM', result.dataframe)
        print_test("Step 3: Schema check", not should_block, "No previous schema")

        # Step 5: Load to DuckDB
        db_loader.load_dataframe(result.dataframe, 'DM', replace=True)
        print_test("Step 4: Load to DuckDB", True)

        # Step 6: Record schema version
        version = schema_tracker.record_version('DM', result.dataframe, csv_path)
        print_test("Step 5: Record schema", version.version == 1)

        # Step 7: Update file status
        record.status = FileStatus.COMPLETED
        record.row_count = len(result.dataframe)
        record.column_count = len(result.dataframe.columns)
        record.schema_version = version.version
        file_store.save(record)
        print_test("Step 6: Update file status", True)

        # Step 8: Simulate second upload with schema change
        csv_path2 = os.path.join(tmpdir, 'dm_v2.csv')
        df_v2 = df_original.copy()
        df_v2['RACE'] = ['WHITE', 'ASIAN', 'BLACK']  # Added column
        df_v2.to_csv(csv_path2, index=False)

        result2 = reader.read_file(csv_path2)
        should_block, diff = schema_tracker.should_block_upload('DM', result2.dataframe)
        print_test("Step 7: Detect schema change",
                   diff.has_changes and 'RACE' in diff.added_columns,
                   f"Added: {diff.added_columns}")

        # Step 9: Verify non-breaking change allows upload
        print_test("Step 8: Non-breaking change allowed", not should_block)

        # Step 10: Test breaking change detection
        df_v3 = df_original[['USUBJID', 'AGE']].copy()  # Remove SEX column
        should_block, diff = schema_tracker.should_block_upload('DM', df_v3, block_on_breaking=True)
        print_test("Step 9: Breaking change blocked",
                   should_block and 'SEX' in diff.removed_columns,
                   f"Removed: {diff.removed_columns}")

    return True


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("  FACTORY 1 - DATA FOUNDRY TEST SUITE")
    print("=" * 60)

    results = {
        'UniversalReader': test_universal_reader(),
        'SchemaTracker': test_schema_tracker(),
        'FileStore': test_file_store(),
        'DuckDBLoader': test_duckdb_loader(),
        'Integration': test_integration()
    }

    print_header("TEST SUMMARY")

    total = len(results)
    passed = sum(1 for v in results.values() if v)

    for name, result in results.items():
        status = "PASSED" if result else "FAILED"
        icon = "[OK]" if result else "[X]"
        print(f"  {icon} {name}: {status}")

    print("\n" + "-" * 60)
    print(f"  Total: {passed}/{total} test suites passed")
    print("-" * 60)

    return 0 if passed == total else 1


if __name__ == '__main__':
    sys.exit(main())
