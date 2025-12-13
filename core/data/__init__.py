# SAGE Data Factory Module
# ==========================
# Factory 1: Data Foundry - Multi-format Data Pipeline
"""
Data processing functionality for clinical data ingestion.

This module provides:
- SASReader: Read SAS7BDAT files with proper encoding handling
- DateHandler: Standardize dates and handle partial date imputation
- DuckDBLoader: Load processed data into DuckDB with validation
- UniversalReader: Unified reader for SAS7BDAT, Parquet, CSV, XPT formats
- SchemaTracker: Version control and change detection for data schemas
"""

from .sas_reader import SASReader
from .date_handler import DateHandler
from .duckdb_loader import DuckDBLoader
from .universal_reader import UniversalReader, DataFormat, FileMetadata, SchemaInfo, ReadResult
from .schema_tracker import SchemaTracker, SchemaVersion, SchemaDiff, ColumnChange, ChangeType, ChangeSeverity
from .file_store import FileStore, FileRecord, FileStatus, ProcessingStep

__all__ = [
    # Original components
    'SASReader',
    'DateHandler',
    'DuckDBLoader',
    # Universal reader
    'UniversalReader',
    'DataFormat',
    'FileMetadata',
    'SchemaInfo',
    'ReadResult',
    # Schema tracking
    'SchemaTracker',
    'SchemaVersion',
    'SchemaDiff',
    'ColumnChange',
    'ChangeType',
    'ChangeSeverity',
    # File store
    'FileStore',
    'FileRecord',
    'FileStatus',
    'ProcessingStep',
]
