# SAGE - Universal Data Reader Module
# ====================================
# Unified reader supporting multiple data formats (SAS, Parquet, CSV, XPT)
"""
Universal data reader with support for:
- SAS7BDAT: Full encoding detection and date handling
- Parquet: Direct read (fastest)
- CSV: Delimiter detection and type inference
- XPT: SAS transport file support

All formats produce consistent output with standardized table naming.
"""

import os
import logging
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

import pandas as pd
import pyarrow.parquet as pq

from .sas_reader import SASReader, ReadResult as SASReadResult
from .date_handler import DateHandler

logger = logging.getLogger(__name__)


class DataFormat(Enum):
    """Supported data formats."""
    SAS7BDAT = "sas7bdat"
    PARQUET = "parquet"
    CSV = "csv"
    XPT = "xpt"
    UNKNOWN = "unknown"


@dataclass
class FileMetadata:
    """Metadata about a source file."""
    filename: str
    filepath: str
    format: DataFormat
    size_bytes: int
    size_mb: float
    created_at: datetime
    modified_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            'filename': self.filename,
            'filepath': self.filepath,
            'format': self.format.value,
            'size_bytes': self.size_bytes,
            'size_mb': self.size_mb,
            'created_at': self.created_at.isoformat(),
            'modified_at': self.modified_at.isoformat()
        }


@dataclass
class SchemaInfo:
    """Schema information for a dataset."""
    columns: List[Dict[str, Any]]
    row_count: int
    column_count: int
    schema_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            'columns': self.columns,
            'row_count': self.row_count,
            'column_count': self.column_count,
            'schema_hash': self.schema_hash
        }


@dataclass
class ReadResult:
    """Result of reading a data file."""
    success: bool
    dataframe: Optional[pd.DataFrame]
    table_name: str
    format: DataFormat
    file_metadata: Optional[FileMetadata]
    schema_info: Optional[SchemaInfo]
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    processing_time_seconds: float = 0.0

    @property
    def schema(self) -> Optional[SchemaInfo]:
        """Alias for schema_info for backward compatibility."""
        return self.schema_info

    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'table_name': self.table_name,
            'format': self.format.value,
            'file_metadata': self.file_metadata.to_dict() if self.file_metadata else None,
            'schema_info': self.schema_info.to_dict() if self.schema_info else None,
            'error': self.error,
            'warnings': self.warnings,
            'processing_time_seconds': self.processing_time_seconds
        }


class UniversalReader:
    """
    Universal data reader supporting multiple formats.

    Provides a unified interface to read:
    - SAS7BDAT files (with encoding detection)
    - Parquet files (fastest, native DuckDB support)
    - CSV files (with delimiter detection)
    - XPT files (SAS transport)

    All formats produce consistent output with:
    - Standardized table naming (filename stem → UPPERCASE)
    - Consistent metadata extraction
    - Schema hash for change detection

    Example:
        reader = UniversalReader()
        result = reader.read_file('data/dm.sas7bdat')
        if result.success:
            df = result.dataframe
            print(f"Table: {result.table_name}")  # Output: DM
    """

    # File extension to format mapping
    FORMAT_MAP = {
        '.sas7bdat': DataFormat.SAS7BDAT,
        '.parquet': DataFormat.PARQUET,
        '.pq': DataFormat.PARQUET,
        '.csv': DataFormat.CSV,
        '.tsv': DataFormat.CSV,
        '.txt': DataFormat.CSV,
        '.xpt': DataFormat.XPT,
        '.xport': DataFormat.XPT,
    }

    # Common CSV delimiters to try
    CSV_DELIMITERS = [',', '\t', '|', ';']

    def __init__(self,
                 standardize_dates: bool = True,
                 date_imputation_rule: str = 'FIRST'):
        """
        Initialize universal reader.

        Args:
            standardize_dates: Whether to standardize date columns
            date_imputation_rule: Rule for partial date imputation (FIRST, LAST, MIDDLE, NONE)
        """
        self.standardize_dates = standardize_dates
        self.date_imputation_rule = date_imputation_rule

        # Initialize sub-readers
        self._sas_reader = SASReader()
        self._date_handler = DateHandler(default_imputation=date_imputation_rule)

    def detect_format(self, filepath: str) -> DataFormat:
        """
        Detect file format from extension.

        Args:
            filepath: Path to the file

        Returns:
            Detected DataFormat
        """
        path = Path(filepath)
        ext = path.suffix.lower()
        return self.FORMAT_MAP.get(ext, DataFormat.UNKNOWN)

    def get_table_name(self, filepath: str) -> str:
        """
        Generate standardized table name from filename.

        Args:
            filepath: Path to the file

        Returns:
            Uppercase table name (e.g., 'dm.sas7bdat' → 'DM')
        """
        path = Path(filepath)
        return path.stem.upper()

    def get_file_metadata(self, filepath: str) -> FileMetadata:
        """
        Extract file metadata.

        Args:
            filepath: Path to the file

        Returns:
            FileMetadata object
        """
        path = Path(filepath)
        stat = path.stat()

        return FileMetadata(
            filename=path.name,
            filepath=str(path.absolute()),
            format=self.detect_format(filepath),
            size_bytes=stat.st_size,
            size_mb=round(stat.st_size / (1024 * 1024), 2),
            created_at=datetime.fromtimestamp(stat.st_ctime),
            modified_at=datetime.fromtimestamp(stat.st_mtime)
        )

    def calculate_schema_hash(self, df: pd.DataFrame) -> str:
        """
        Calculate a hash of the dataframe schema for change detection.

        Args:
            df: DataFrame to hash

        Returns:
            SHA256 hash of the schema
        """
        schema_str = '|'.join([
            f"{col}:{str(df[col].dtype)}"
            for col in sorted(df.columns)
        ])
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    def extract_schema_info(self, df: pd.DataFrame) -> SchemaInfo:
        """
        Extract schema information from a DataFrame.

        Args:
            df: DataFrame to analyze

        Returns:
            SchemaInfo object
        """
        columns = []
        for col in df.columns:
            col_info = {
                'name': col,
                'dtype': str(df[col].dtype),
                'null_count': int(df[col].isnull().sum()),
                'null_percent': round(df[col].isnull().sum() / len(df) * 100, 2) if len(df) > 0 else 0,
                'unique_count': int(df[col].nunique()),
            }

            # Add sample values for categorical-like columns
            if df[col].dtype == 'object' or df[col].nunique() < 50:
                sample = df[col].dropna().unique()[:5].tolist()
                col_info['sample_values'] = [str(v) for v in sample]

            columns.append(col_info)

        return SchemaInfo(
            columns=columns,
            row_count=len(df),
            column_count=len(df.columns),
            schema_hash=self.calculate_schema_hash(df)
        )

    def read_file(self, filepath: str,
                  format_override: Optional[DataFormat] = None,
                  encoding: Optional[str] = None,
                  delimiter: Optional[str] = None) -> ReadResult:
        """
        Read a data file in any supported format.

        Args:
            filepath: Path to the file
            format_override: Force a specific format (auto-detect if None)
            encoding: Encoding override (for SAS/CSV)
            delimiter: Delimiter override (for CSV)

        Returns:
            ReadResult with dataframe and metadata
        """
        start_time = datetime.now()
        path = Path(filepath)

        # Validate file exists
        if not path.exists():
            return ReadResult(
                success=False,
                dataframe=None,
                table_name="",
                format=DataFormat.UNKNOWN,
                file_metadata=None,
                schema_info=None,
                error=f"File not found: {filepath}"
            )

        # Detect format
        file_format = format_override or self.detect_format(filepath)
        if file_format == DataFormat.UNKNOWN:
            return ReadResult(
                success=False,
                dataframe=None,
                table_name="",
                format=DataFormat.UNKNOWN,
                file_metadata=None,
                schema_info=None,
                error=f"Unsupported file format: {path.suffix}"
            )

        # Get file metadata
        file_metadata = self.get_file_metadata(filepath)
        table_name = self.get_table_name(filepath)

        # Read based on format
        try:
            if file_format == DataFormat.SAS7BDAT:
                result = self._read_sas(filepath, encoding)
            elif file_format == DataFormat.PARQUET:
                result = self._read_parquet(filepath)
            elif file_format == DataFormat.CSV:
                result = self._read_csv(filepath, delimiter, encoding)
            elif file_format == DataFormat.XPT:
                result = self._read_xpt(filepath, encoding)
            else:
                return ReadResult(
                    success=False,
                    dataframe=None,
                    table_name=table_name,
                    format=file_format,
                    file_metadata=file_metadata,
                    schema_info=None,
                    error=f"Format not implemented: {file_format.value}"
                )

            if not result['success']:
                return ReadResult(
                    success=False,
                    dataframe=None,
                    table_name=table_name,
                    format=file_format,
                    file_metadata=file_metadata,
                    schema_info=None,
                    error=result.get('error', 'Unknown error'),
                    warnings=result.get('warnings', [])
                )

            df = result['dataframe']
            warnings = result.get('warnings', [])

            # Standardize dates if enabled (not for Parquet which is pre-processed)
            if self.standardize_dates and file_format != DataFormat.PARQUET:
                df, date_warnings = self._standardize_dates(df)
                warnings.extend(date_warnings)

            # Extract schema info
            schema_info = self.extract_schema_info(df)

            processing_time = (datetime.now() - start_time).total_seconds()

            logger.info(f"Read {filepath}: {schema_info.row_count} rows, "
                       f"{schema_info.column_count} columns in {processing_time:.2f}s")

            return ReadResult(
                success=True,
                dataframe=df,
                table_name=table_name,
                format=file_format,
                file_metadata=file_metadata,
                schema_info=schema_info,
                warnings=warnings,
                processing_time_seconds=processing_time
            )

        except Exception as e:
            logger.error(f"Error reading {filepath}: {e}")
            return ReadResult(
                success=False,
                dataframe=None,
                table_name=table_name,
                format=file_format,
                file_metadata=file_metadata,
                schema_info=None,
                error=str(e)
            )

    def _read_sas(self, filepath: str, encoding: Optional[str] = None) -> Dict[str, Any]:
        """Read SAS7BDAT file."""
        result = self._sas_reader.read_file(filepath, encoding)

        if not result.success:
            return {
                'success': False,
                'error': result.error,
                'warnings': result.warnings
            }

        return {
            'success': True,
            'dataframe': result.dataframe,
            'warnings': result.warnings
        }

    def _read_parquet(self, filepath: str) -> Dict[str, Any]:
        """Read Parquet file."""
        try:
            # Read with PyArrow for best compatibility
            table = pq.read_table(filepath)
            df = table.to_pandas()

            return {
                'success': True,
                'dataframe': df,
                'warnings': []
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Failed to read Parquet: {e}",
                'warnings': []
            }

    def _read_csv(self, filepath: str,
                  delimiter: Optional[str] = None,
                  encoding: Optional[str] = None) -> Dict[str, Any]:
        """Read CSV file with delimiter detection."""
        warnings = []

        # Try to detect delimiter if not specified
        if delimiter is None:
            delimiter = self._detect_csv_delimiter(filepath)
            if delimiter != ',':
                warnings.append(f"Auto-detected delimiter: '{repr(delimiter)}'")

        # Try different encodings
        encodings_to_try = [encoding] if encoding else ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1']

        df = None
        last_error = None
        used_encoding = None

        for enc in encodings_to_try:
            if enc is None:
                continue
            try:
                df = pd.read_csv(
                    filepath,
                    delimiter=delimiter,
                    encoding=enc,
                    low_memory=False,
                    on_bad_lines='warn'
                )
                used_encoding = enc
                break
            except Exception as e:
                last_error = str(e)
                continue

        if df is None:
            return {
                'success': False,
                'error': f"Failed to read CSV: {last_error}",
                'warnings': warnings
            }

        if used_encoding and used_encoding != 'utf-8':
            warnings.append(f"Used encoding: {used_encoding}")

        # Infer better types
        df = self._infer_csv_types(df)

        return {
            'success': True,
            'dataframe': df,
            'warnings': warnings
        }

    def _detect_csv_delimiter(self, filepath: str) -> str:
        """Detect CSV delimiter by analyzing first few lines."""
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                sample = f.read(8192)  # Read first 8KB

            # Count occurrences of each delimiter
            counts = {}
            for delim in self.CSV_DELIMITERS:
                counts[delim] = sample.count(delim)

            # Find most common delimiter that appears consistently
            lines = sample.split('\n')[:5]
            for delim in self.CSV_DELIMITERS:
                if all(delim in line for line in lines if line.strip()):
                    return delim

            # Fallback to most common
            return max(counts, key=counts.get)

        except Exception:
            return ','  # Default to comma

    def _infer_csv_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Infer better types for CSV columns."""
        for col in df.columns:
            if df[col].dtype == 'object':
                # Try to convert to numeric
                try:
                    numeric = pd.to_numeric(df[col], errors='coerce')
                    if numeric.notna().sum() > 0.9 * len(df):  # >90% converted
                        df[col] = numeric
                        continue
                except Exception:
                    pass

                # Try to convert to datetime
                try:
                    dt = pd.to_datetime(df[col], errors='coerce', format='mixed')
                    if dt.notna().sum() > 0.9 * len(df):  # >90% converted
                        df[col] = dt
                        continue
                except Exception:
                    pass

        return df

    def _read_xpt(self, filepath: str, encoding: Optional[str] = None) -> Dict[str, Any]:
        """Read SAS XPT (transport) file."""
        try:
            df = pd.read_sas(filepath, format='xport', encoding=encoding or 'utf-8')

            return {
                'success': True,
                'dataframe': df,
                'warnings': []
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Failed to read XPT: {e}",
                'warnings': []
            }

    def _standardize_dates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Standardize date columns in the DataFrame."""
        warnings = []

        # Find potential date columns
        date_columns = []
        for col in df.columns:
            col_upper = col.upper()
            if any(suffix in col_upper for suffix in ['DTC', 'DT', 'DTM', 'DATE']):
                date_columns.append(col)

        # Standardize each date column
        for col in date_columns:
            try:
                result = self._date_handler.standardize_column(df, col)
                if result.get('converted', 0) > 0:
                    df[col] = result['series']
                    if result.get('warnings'):
                        warnings.extend(result['warnings'])
            except Exception as e:
                warnings.append(f"Could not standardize date column {col}: {e}")

        return df, warnings

    def scan_directory(self, directory: str,
                       recursive: bool = False) -> List[FileMetadata]:
        """
        Scan a directory for supported data files.

        Args:
            directory: Directory to scan
            recursive: Whether to search subdirectories

        Returns:
            List of FileMetadata for found files
        """
        directory = Path(directory)
        if not directory.exists():
            return []

        files = []

        # Get all supported extensions
        extensions = list(self.FORMAT_MAP.keys())

        for ext in extensions:
            pattern = f"**/*{ext}" if recursive else f"*{ext}"
            for filepath in directory.glob(pattern):
                if filepath.is_file():
                    try:
                        files.append(self.get_file_metadata(str(filepath)))
                    except Exception as e:
                        logger.warning(f"Could not get metadata for {filepath}: {e}")

        return sorted(files, key=lambda x: x.filename)

    def get_supported_formats(self) -> List[Dict[str, str]]:
        """Get list of supported formats with descriptions."""
        return [
            {
                'format': DataFormat.SAS7BDAT.value,
                'extensions': ['.sas7bdat'],
                'description': 'SAS Dataset - Full encoding detection and date handling'
            },
            {
                'format': DataFormat.PARQUET.value,
                'extensions': ['.parquet', '.pq'],
                'description': 'Apache Parquet - Fastest, direct DuckDB support'
            },
            {
                'format': DataFormat.CSV.value,
                'extensions': ['.csv', '.tsv', '.txt'],
                'description': 'Delimited text - Auto delimiter detection'
            },
            {
                'format': DataFormat.XPT.value,
                'extensions': ['.xpt', '.xport'],
                'description': 'SAS Transport - Legacy format support'
            }
        ]
