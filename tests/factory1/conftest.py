"""
Pytest fixtures for Factory 1 tests.
"""

import os
import sys
import tempfile
import hashlib
import uuid
import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.data import (
    UniversalReader,
    SchemaTracker,
    FileStore,
    DuckDBLoader,
    FileRecord,
    FileStatus
)


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'USUBJID': ['SUBJ001', 'SUBJ002', 'SUBJ003'],
        'AGE': [45, 32, 58],
        'SEX': ['M', 'F', 'M'],
        'RACE': ['WHITE', 'ASIAN', 'BLACK']
    })


@pytest.fixture
def sample_df_with_dates():
    """Create a sample DataFrame with date columns."""
    return pd.DataFrame({
        'USUBJID': ['SUBJ001', 'SUBJ002'],
        'RFSTDTC': ['2023-01-15', '2023-02-20'],
        'RFENDTC': ['2023-06-30', '2023-07-15'],
        'AGE': [45, 32]
    })


@pytest.fixture
def sample_df_numeric():
    """Create a sample DataFrame with numeric data."""
    return pd.DataFrame({
        'USUBJID': ['SUBJ001', 'SUBJ002', 'SUBJ003'],
        'LBTESTCD': ['ALT', 'AST', 'BILI'],
        'LBSTRESN': [25.5, 30.2, 0.8],
        'LBSTRESU': ['U/L', 'U/L', 'mg/dL']
    })


@pytest.fixture
def temp_csv_file(sample_df):
    """Create a temporary CSV file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_df.to_csv(f.name, index=False)
        yield f.name
    if os.path.exists(f.name):
        os.unlink(f.name)


@pytest.fixture
def temp_parquet_file(sample_df):
    """Create a temporary Parquet file."""
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        parquet_path = f.name
    sample_df.to_parquet(parquet_path, index=False)
    yield parquet_path
    if os.path.exists(parquet_path):
        os.unlink(parquet_path)


@pytest.fixture
def temp_db_path():
    """Create a temporary database path."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    # Remove the file so SQLite can create it fresh
    if os.path.exists(db_path):
        os.unlink(db_path)
    yield db_path
    if os.path.exists(db_path):
        try:
            os.unlink(db_path)
        except PermissionError:
            pass  # File may be locked on Windows


@pytest.fixture
def temp_duckdb_path():
    """Create a temporary DuckDB path."""
    db_path = tempfile.mktemp(suffix='.duckdb')
    yield db_path
    # Clean up any related files
    for ext in ['', '.wal']:
        path = db_path + ext if ext else db_path
        if os.path.exists(path):
            try:
                os.unlink(path)
            except PermissionError:
                pass  # File may be locked on Windows


@pytest.fixture
def universal_reader():
    """Create a UniversalReader instance."""
    return UniversalReader()


@pytest.fixture
def schema_tracker(temp_db_path):
    """Create a SchemaTracker instance with temp database."""
    return SchemaTracker(temp_db_path)


@pytest.fixture
def file_store(temp_db_path):
    """Create a FileStore instance with temp database."""
    return FileStore(temp_db_path)


@pytest.fixture
def duckdb_loader(temp_duckdb_path):
    """Create a DuckDBLoader instance with temp database."""
    loader = DuckDBLoader(temp_duckdb_path)
    yield loader
    # Close connection if method exists
    if hasattr(loader, 'close'):
        loader.close()


def create_file_record(filename: str, filepath: str, file_format: str = 'csv',
                       file_size: int = 1000, status: FileStatus = FileStatus.PENDING) -> FileRecord:
    """Helper to create a FileRecord for testing."""
    # Calculate file hash
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
