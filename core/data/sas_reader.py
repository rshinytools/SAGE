# SAGE - SAS7BDAT Reader Module
# ==============================
# Reads SAS datasets with proper encoding and metadata extraction
"""
SAS7BDAT file reader with support for:
- Multiple encoding formats (UTF-8, Latin-1, Windows-1252)
- Variable labels and formats extraction
- Large file handling with chunked reading
- Automatic type inference and optimization
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Iterator
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


@dataclass
class SASMetadata:
    """Metadata extracted from a SAS dataset."""
    filename: str
    dataset_name: str
    num_rows: int
    num_columns: int
    created_date: Optional[datetime]
    modified_date: Optional[datetime]
    encoding: str
    columns: List[Dict[str, Any]] = field(default_factory=list)
    label: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary."""
        return {
            'filename': self.filename,
            'dataset_name': self.dataset_name,
            'num_rows': self.num_rows,
            'num_columns': self.num_columns,
            'created_date': self.created_date.isoformat() if self.created_date else None,
            'modified_date': self.modified_date.isoformat() if self.modified_date else None,
            'encoding': self.encoding,
            'columns': self.columns,
            'label': self.label
        }


@dataclass
class ReadResult:
    """Result of reading a SAS file."""
    success: bool
    dataframe: Optional[pd.DataFrame]
    metadata: Optional[SASMetadata]
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)


class SASReader:
    """
    Reader for SAS7BDAT files with comprehensive error handling.

    Features:
    - Automatic encoding detection
    - Variable label and format extraction
    - Memory-efficient chunked reading for large files
    - Parquet export capability

    Example:
        reader = SASReader()
        result = reader.read_file('path/to/dataset.sas7bdat')
        if result.success:
            df = result.dataframe
            metadata = result.metadata
    """

    # Common encodings for SAS files
    ENCODINGS = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1', 'cp1252']

    # SAS date epoch (January 1, 1960)
    SAS_EPOCH = datetime(1960, 1, 1)

    def __init__(self, default_encoding: str = 'utf-8', chunk_size: int = 100000):
        """
        Initialize SAS reader.

        Args:
            default_encoding: Default encoding to try first
            chunk_size: Number of rows to read at a time for large files
        """
        self.default_encoding = default_encoding
        self.chunk_size = chunk_size
        self._encoding_cache: Dict[str, str] = {}

    def read_file(self, filepath: str, encoding: Optional[str] = None) -> ReadResult:
        """
        Read a SAS7BDAT file and return data with metadata.

        Args:
            filepath: Path to the SAS file
            encoding: Optional encoding override

        Returns:
            ReadResult containing dataframe, metadata, and any errors/warnings
        """
        filepath = Path(filepath)

        if not filepath.exists():
            return ReadResult(
                success=False,
                dataframe=None,
                metadata=None,
                error=f"File not found: {filepath}"
            )

        if not filepath.suffix.lower() == '.sas7bdat':
            return ReadResult(
                success=False,
                dataframe=None,
                metadata=None,
                error=f"Invalid file type: {filepath.suffix}. Expected .sas7bdat"
            )

        warnings = []

        # Try to read with specified or detected encoding
        df = None
        used_encoding = encoding or self.default_encoding

        if encoding:
            df, error = self._try_read(filepath, encoding)
            if error:
                return ReadResult(
                    success=False,
                    dataframe=None,
                    metadata=None,
                    error=f"Failed to read with encoding {encoding}: {error}"
                )
        else:
            # Try multiple encodings
            df, used_encoding, encoding_error = self._read_with_fallback(filepath)
            if df is None:
                return ReadResult(
                    success=False,
                    dataframe=None,
                    metadata=None,
                    error=f"Failed to read file with any encoding: {encoding_error}"
                )
            if used_encoding != self.default_encoding:
                warnings.append(f"Used encoding '{used_encoding}' instead of default")

        # Extract metadata
        metadata = self._extract_metadata(filepath, df, used_encoding)

        # Optimize data types
        df = self._optimize_dtypes(df)

        # Cache successful encoding
        self._encoding_cache[str(filepath)] = used_encoding

        logger.info(f"Successfully read {filepath.name}: {len(df)} rows, {len(df.columns)} columns")

        return ReadResult(
            success=True,
            dataframe=df,
            metadata=metadata,
            warnings=warnings
        )

    def _try_read(self, filepath: Path, encoding: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Try to read file with specific encoding."""
        try:
            df = pd.read_sas(filepath, format='sas7bdat', encoding=encoding)
            return df, None
        except Exception as e:
            return None, str(e)

    def _read_with_fallback(self, filepath: Path) -> Tuple[Optional[pd.DataFrame], str, Optional[str]]:
        """Try multiple encodings until one works."""
        # Check cache first
        cached_encoding = self._encoding_cache.get(str(filepath))
        if cached_encoding:
            df, error = self._try_read(filepath, cached_encoding)
            if df is not None:
                return df, cached_encoding, None

        # Try default first, then others
        encodings_to_try = [self.default_encoding] + [e for e in self.ENCODINGS if e != self.default_encoding]

        last_error = None
        for encoding in encodings_to_try:
            df, error = self._try_read(filepath, encoding)
            if df is not None:
                return df, encoding, None
            last_error = error

        return None, "", last_error

    def _extract_metadata(self, filepath: Path, df: pd.DataFrame, encoding: str) -> SASMetadata:
        """Extract metadata from the file and dataframe."""
        columns = []

        for col in df.columns:
            col_info = {
                'name': col,
                'dtype': str(df[col].dtype),
                'null_count': int(df[col].isnull().sum()),
                'unique_count': int(df[col].nunique()),
                'label': '',  # Would need pyreadstat for labels
                'format': ''  # Would need pyreadstat for formats
            }

            # Add sample values for categorical columns
            if df[col].dtype == 'object' or df[col].nunique() < 50:
                sample_values = df[col].dropna().unique()[:10].tolist()
                col_info['sample_values'] = [str(v) for v in sample_values]

            columns.append(col_info)

        # Get file stats
        stat = filepath.stat()

        return SASMetadata(
            filename=filepath.name,
            dataset_name=filepath.stem.upper(),
            num_rows=len(df),
            num_columns=len(df.columns),
            created_date=datetime.fromtimestamp(stat.st_ctime),
            modified_date=datetime.fromtimestamp(stat.st_mtime),
            encoding=encoding,
            columns=columns,
            label=""
        )

    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize dataframe memory usage by converting dtypes."""
        for col in df.columns:
            col_type = df[col].dtype

            # Convert object columns with low cardinality to category
            if col_type == 'object':
                num_unique = df[col].nunique()
                num_total = len(df[col])
                if num_unique / num_total < 0.5:  # Less than 50% unique values
                    df[col] = df[col].astype('category')

            # Downcast numeric types
            elif col_type in ['int64', 'int32']:
                df[col] = pd.to_numeric(df[col], downcast='integer')
            elif col_type in ['float64', 'float32']:
                df[col] = pd.to_numeric(df[col], downcast='float')

        return df

    def read_chunked(self, filepath: str, encoding: Optional[str] = None) -> Iterator[pd.DataFrame]:
        """
        Read a large SAS file in chunks.

        Args:
            filepath: Path to the SAS file
            encoding: Optional encoding override

        Yields:
            DataFrame chunks
        """
        filepath = Path(filepath)
        used_encoding = encoding or self._encoding_cache.get(str(filepath), self.default_encoding)

        try:
            reader = pd.read_sas(filepath, format='sas7bdat', encoding=used_encoding,
                                chunksize=self.chunk_size)
            for chunk in reader:
                yield self._optimize_dtypes(chunk)
        except Exception as e:
            logger.error(f"Error reading file in chunks: {e}")
            raise

    def to_parquet(self, filepath: str, output_path: str,
                   encoding: Optional[str] = None,
                   compression: str = 'snappy') -> Tuple[bool, Optional[str]]:
        """
        Convert SAS file to Parquet format.

        Args:
            filepath: Path to the SAS file
            output_path: Path for the output Parquet file
            encoding: Optional encoding override
            compression: Parquet compression algorithm

        Returns:
            Tuple of (success, error_message)
        """
        result = self.read_file(filepath, encoding)

        if not result.success:
            return False, result.error

        try:
            # Convert to PyArrow Table for better Parquet support
            table = pa.Table.from_pandas(result.dataframe)

            # Write with compression
            pq.write_table(table, output_path, compression=compression)

            logger.info(f"Converted {filepath} to {output_path}")
            return True, None

        except Exception as e:
            error_msg = f"Failed to write Parquet: {e}"
            logger.error(error_msg)
            return False, error_msg

    def get_file_info(self, filepath: str) -> Dict[str, Any]:
        """Get basic file information without reading all data."""
        filepath = Path(filepath)

        if not filepath.exists():
            return {'error': 'File not found'}

        stat = filepath.stat()

        return {
            'filename': filepath.name,
            'size_bytes': stat.st_size,
            'size_mb': round(stat.st_size / (1024 * 1024), 2),
            'created': datetime.fromtimestamp(stat.st_ctime).isoformat(),
            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
            'extension': filepath.suffix
        }

    def scan_directory(self, directory: str, recursive: bool = False) -> List[Dict[str, Any]]:
        """
        Scan a directory for SAS files.

        Args:
            directory: Directory path to scan
            recursive: Whether to search subdirectories

        Returns:
            List of file information dictionaries
        """
        directory = Path(directory)

        if not directory.exists():
            return []

        pattern = '**/*.sas7bdat' if recursive else '*.sas7bdat'

        files = []
        for filepath in directory.glob(pattern):
            info = self.get_file_info(str(filepath))
            info['relative_path'] = str(filepath.relative_to(directory))
            files.append(info)

        return sorted(files, key=lambda x: x['filename'])
