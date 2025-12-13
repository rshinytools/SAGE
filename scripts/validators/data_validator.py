# SAGE - Data Validator Module
# =============================
# Validates data integrity between SAS source and DuckDB target
"""
Data validation for clinical trial data pipelines.

Features:
- Row count comparison (SAS vs DuckDB)
- Column type verification
- Null value reporting
- Data quality scoring
- Validation reports
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import json

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ColumnValidation:
    """Validation result for a single column."""
    name: str
    source_dtype: str
    target_dtype: str
    source_nulls: int
    target_nulls: int
    source_unique: int
    target_unique: int
    dtype_match: bool
    null_match: bool
    unique_match: bool
    issues: List[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return self.dtype_match and self.null_match and len(self.issues) == 0


@dataclass
class TableValidation:
    """Validation result for a table."""
    table_name: str
    source_file: str
    source_rows: int
    target_rows: int
    source_columns: int
    target_columns: int
    row_match: bool
    column_match: bool
    columns: List[ColumnValidation] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return (self.row_match and self.column_match and
                all(c.is_valid for c in self.columns) and
                len(self.issues) == 0)

    @property
    def quality_score(self) -> float:
        """Calculate data quality score (0-100)."""
        scores = []

        # Row count match (30 points)
        if self.row_match:
            scores.append(30)
        else:
            diff_pct = abs(self.source_rows - self.target_rows) / max(self.source_rows, 1)
            scores.append(max(0, 30 - diff_pct * 100))

        # Column count match (20 points)
        if self.column_match:
            scores.append(20)
        else:
            scores.append(10)

        # Column validations (50 points)
        if self.columns:
            valid_cols = sum(1 for c in self.columns if c.is_valid)
            scores.append(50 * valid_cols / len(self.columns))
        else:
            scores.append(50)

        return round(sum(scores), 1)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'table_name': self.table_name,
            'source_file': self.source_file,
            'source_rows': self.source_rows,
            'target_rows': self.target_rows,
            'source_columns': self.source_columns,
            'target_columns': self.target_columns,
            'row_match': self.row_match,
            'column_match': self.column_match,
            'is_valid': self.is_valid,
            'quality_score': self.quality_score,
            'issues': self.issues,
            'warnings': self.warnings,
            'columns': [
                {
                    'name': c.name,
                    'is_valid': c.is_valid,
                    'source_dtype': c.source_dtype,
                    'target_dtype': c.target_dtype,
                    'source_nulls': c.source_nulls,
                    'target_nulls': c.target_nulls,
                    'issues': c.issues
                }
                for c in self.columns
            ]
        }


@dataclass
class ValidationReport:
    """Complete validation report for a data load."""
    generated_at: datetime
    tables: List[TableValidation] = field(default_factory=list)
    overall_status: str = "pending"
    summary: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_valid(self) -> bool:
        return all(t.is_valid for t in self.tables)

    @property
    def overall_quality_score(self) -> float:
        if not self.tables:
            return 0.0
        return round(sum(t.quality_score for t in self.tables) / len(self.tables), 1)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'generated_at': self.generated_at.isoformat(),
            'overall_status': 'passed' if self.is_valid else 'failed',
            'overall_quality_score': self.overall_quality_score,
            'tables_validated': len(self.tables),
            'tables_passed': sum(1 for t in self.tables if t.is_valid),
            'tables': [t.to_dict() for t in self.tables],
            'summary': self.summary
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def save(self, path: str):
        """Save report to JSON file."""
        with open(path, 'w') as f:
            f.write(self.to_json())


class DataValidator:
    """
    Validator for clinical data pipelines.

    Compares source (SAS/DataFrame) data with target (DuckDB) data
    to ensure data integrity.

    Example:
        validator = DataValidator()

        # Validate a single table
        result = validator.validate_table(
            source_df=df,
            target_loader=duckdb_loader,
            table_name='DM'
        )

        # Generate report
        report = validator.create_report([result])
        report.save('validation_report.json')
    """

    # Expected SDTM domain columns
    SDTM_REQUIRED_COLUMNS = {
        'DM': ['STUDYID', 'USUBJID', 'SUBJID', 'SITEID'],
        'AE': ['STUDYID', 'USUBJID', 'AESEQ', 'AETERM'],
        'CM': ['STUDYID', 'USUBJID', 'CMSEQ', 'CMTRT'],
        'LB': ['STUDYID', 'USUBJID', 'LBSEQ', 'LBTESTCD', 'LBTEST'],
        'VS': ['STUDYID', 'USUBJID', 'VSSEQ', 'VSTESTCD', 'VSTEST'],
        'EX': ['STUDYID', 'USUBJID', 'EXSEQ', 'EXTRT'],
        'MH': ['STUDYID', 'USUBJID', 'MHSEQ', 'MHTERM'],
    }

    # Expected ADaM domain columns
    ADAM_REQUIRED_COLUMNS = {
        'ADSL': ['STUDYID', 'USUBJID', 'SUBJID', 'SITEID', 'TRT01P', 'TRT01A'],
        'ADAE': ['STUDYID', 'USUBJID', 'AESEQ', 'AEDECOD', 'TRTA'],
        'ADLB': ['STUDYID', 'USUBJID', 'PARAMCD', 'PARAM', 'AVAL'],
        'ADVS': ['STUDYID', 'USUBJID', 'PARAMCD', 'PARAM', 'AVAL'],
        'ADCM': ['STUDYID', 'USUBJID', 'CMSEQ', 'CMDECOD'],
    }

    def __init__(self, strict_mode: bool = False):
        """
        Initialize validator.

        Args:
            strict_mode: If True, warnings are treated as errors
        """
        self.strict_mode = strict_mode

    def validate_table(self, source_df: pd.DataFrame,
                       target_loader: Any,
                       table_name: str,
                       source_file: str = "") -> TableValidation:
        """
        Validate a loaded table against its source.

        Args:
            source_df: Original source DataFrame
            target_loader: DuckDB loader instance
            table_name: Name of the table to validate
            source_file: Path to original source file

        Returns:
            TableValidation result
        """
        table_name = table_name.upper()
        issues = []
        warnings = []
        columns = []

        # Get target data
        try:
            target_df = target_loader.query(f"SELECT * FROM {table_name}")
        except Exception as e:
            return TableValidation(
                table_name=table_name,
                source_file=source_file,
                source_rows=len(source_df),
                target_rows=0,
                source_columns=len(source_df.columns),
                target_columns=0,
                row_match=False,
                column_match=False,
                issues=[f"Cannot query target table: {e}"]
            )

        # Row count validation
        source_rows = len(source_df)
        target_rows = len(target_df)
        row_match = source_rows == target_rows

        if not row_match:
            issues.append(f"Row count mismatch: source={source_rows}, target={target_rows}")

        # Column count validation
        source_columns = len(source_df.columns)
        target_columns = len(target_df.columns)
        column_match = source_columns == target_columns

        if not column_match:
            issues.append(f"Column count mismatch: source={source_columns}, target={target_columns}")

        # Validate required columns for known domains
        domain = table_name.upper()
        required = self.SDTM_REQUIRED_COLUMNS.get(domain, [])
        required.extend(self.ADAM_REQUIRED_COLUMNS.get(domain, []))

        for req_col in required:
            if req_col not in target_df.columns:
                issues.append(f"Missing required column: {req_col}")

        # Column-level validation
        for col in source_df.columns:
            if col not in target_df.columns:
                warnings.append(f"Column '{col}' not in target")
                continue

            col_validation = self._validate_column(
                source_df[col],
                target_df[col],
                col
            )
            columns.append(col_validation)

        return TableValidation(
            table_name=table_name,
            source_file=source_file,
            source_rows=source_rows,
            target_rows=target_rows,
            source_columns=source_columns,
            target_columns=target_columns,
            row_match=row_match,
            column_match=column_match,
            columns=columns,
            issues=issues,
            warnings=warnings
        )

    def _validate_column(self, source_col: pd.Series,
                         target_col: pd.Series,
                         col_name: str) -> ColumnValidation:
        """Validate a single column."""
        issues = []

        source_dtype = str(source_col.dtype)
        target_dtype = str(target_col.dtype)

        source_nulls = int(source_col.isnull().sum())
        target_nulls = int(target_col.isnull().sum())

        source_unique = int(source_col.nunique())
        target_unique = int(target_col.nunique())

        # Type compatibility check (flexible)
        dtype_match = self._types_compatible(source_dtype, target_dtype)
        if not dtype_match:
            issues.append(f"Type mismatch: {source_dtype} vs {target_dtype}")

        # Null count check
        null_match = source_nulls == target_nulls
        if not null_match:
            issues.append(f"Null count mismatch: {source_nulls} vs {target_nulls}")

        # Unique count check (warning only)
        unique_match = source_unique == target_unique
        if not unique_match and abs(source_unique - target_unique) > 1:
            issues.append(f"Unique count mismatch: {source_unique} vs {target_unique}")

        return ColumnValidation(
            name=col_name,
            source_dtype=source_dtype,
            target_dtype=target_dtype,
            source_nulls=source_nulls,
            target_nulls=target_nulls,
            source_unique=source_unique,
            target_unique=target_unique,
            dtype_match=dtype_match,
            null_match=null_match,
            unique_match=unique_match,
            issues=issues
        )

    def _types_compatible(self, source_type: str, target_type: str) -> bool:
        """Check if two types are compatible."""
        # Normalize type names
        source = source_type.lower()
        target = target_type.lower()

        # Direct match
        if source == target:
            return True

        # Numeric types
        numeric_types = {'int', 'float', 'double', 'bigint', 'integer', 'smallint'}
        if any(t in source for t in numeric_types) and any(t in target for t in numeric_types):
            return True

        # String types
        string_types = {'object', 'string', 'varchar', 'category', 'str'}
        if any(t in source for t in string_types) and any(t in target for t in string_types):
            return True

        # Date/time types
        date_types = {'datetime', 'timestamp', 'date'}
        if any(t in source for t in date_types) and any(t in target for t in date_types):
            return True

        return False

    def validate_dataframe(self, df: pd.DataFrame,
                          domain: str) -> Tuple[bool, List[str], List[str]]:
        """
        Validate a DataFrame against expected schema.

        Args:
            df: DataFrame to validate
            domain: Domain name (e.g., 'DM', 'AE', 'ADSL')

        Returns:
            Tuple of (is_valid, errors, warnings)
        """
        errors = []
        warnings = []
        domain = domain.upper()

        # Check required columns
        required = self.SDTM_REQUIRED_COLUMNS.get(domain, [])
        required.extend(self.ADAM_REQUIRED_COLUMNS.get(domain, []))

        for col in required:
            if col not in df.columns:
                errors.append(f"Missing required column: {col}")

        # Check for empty DataFrame
        if len(df) == 0:
            warnings.append("DataFrame is empty")

        # Check for all-null columns
        for col in df.columns:
            if df[col].isnull().all():
                warnings.append(f"Column '{col}' is entirely null")

        # Check USUBJID format if present
        if 'USUBJID' in df.columns:
            null_usubjid = df['USUBJID'].isnull().sum()
            if null_usubjid > 0:
                errors.append(f"USUBJID has {null_usubjid} null values")

        is_valid = len(errors) == 0 if self.strict_mode else True

        return is_valid, errors, warnings

    def create_report(self, validations: List[TableValidation]) -> ValidationReport:
        """
        Create a validation report from table validations.

        Args:
            validations: List of TableValidation results

        Returns:
            ValidationReport
        """
        report = ValidationReport(
            generated_at=datetime.now(),
            tables=validations
        )

        # Generate summary
        report.summary = {
            'total_tables': len(validations),
            'passed_tables': sum(1 for v in validations if v.is_valid),
            'failed_tables': sum(1 for v in validations if not v.is_valid),
            'total_rows_source': sum(v.source_rows for v in validations),
            'total_rows_target': sum(v.target_rows for v in validations),
            'quality_score': report.overall_quality_score,
            'tables_by_quality': {
                'excellent': sum(1 for v in validations if v.quality_score >= 95),
                'good': sum(1 for v in validations if 80 <= v.quality_score < 95),
                'fair': sum(1 for v in validations if 60 <= v.quality_score < 80),
                'poor': sum(1 for v in validations if v.quality_score < 60)
            }
        }

        report.overall_status = 'passed' if report.is_valid else 'failed'

        return report

    def compare_row_counts(self, source_counts: Dict[str, int],
                          target_loader: Any) -> Dict[str, Dict[str, Any]]:
        """
        Compare row counts between source and target.

        Args:
            source_counts: Dict of {table_name: row_count} from source
            target_loader: DuckDB loader instance

        Returns:
            Dict with comparison results for each table
        """
        results = {}

        for table_name, source_count in source_counts.items():
            try:
                target_count = target_loader.query(
                    f"SELECT COUNT(*) as cnt FROM {table_name}"
                ).iloc[0]['cnt']

                match = source_count == target_count
                diff = target_count - source_count
                diff_pct = (diff / source_count * 100) if source_count > 0 else 0

                results[table_name] = {
                    'source_count': source_count,
                    'target_count': target_count,
                    'match': match,
                    'difference': diff,
                    'difference_percent': round(diff_pct, 2)
                }
            except Exception as e:
                results[table_name] = {
                    'source_count': source_count,
                    'target_count': None,
                    'match': False,
                    'error': str(e)
                }

        return results
