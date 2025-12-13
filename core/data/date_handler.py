# SAGE - Date Handler Module
# ===========================
# Standardizes dates and handles partial date imputation for clinical data
"""
Date handling for clinical trial data with support for:
- ISO 8601 date standardization
- Partial date imputation (missing day/month)
- SAS date conversion (days since 1960-01-01)
- Multiple input format detection
- Clinical trial date rules (SDTM/ADaM compliant)
"""

import re
import logging
from datetime import datetime, date, timedelta
from typing import Optional, Tuple, List, Dict, Any, Union
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class ImputationRule(Enum):
    """Rules for imputing missing date components."""
    FIRST = "first"      # Use first day/month (conservative for start dates)
    LAST = "last"        # Use last day/month (conservative for end dates)
    MIDDLE = "middle"    # Use middle day (15) / middle month (June)
    NONE = "none"        # Don't impute, keep partial


class DatePrecision(Enum):
    """Precision level of a date."""
    FULL = "full"        # YYYY-MM-DD
    MONTH = "month"      # YYYY-MM (day missing)
    YEAR = "year"        # YYYY (month and day missing)
    UNKNOWN = "unknown"  # Cannot determine


@dataclass
class ParsedDate:
    """Result of parsing a date value."""
    original: str
    year: Optional[int]
    month: Optional[int]
    day: Optional[int]
    precision: DatePrecision
    is_valid: bool
    error: Optional[str] = None

    @property
    def iso_partial(self) -> str:
        """Return ISO 8601 partial date string."""
        if not self.is_valid or self.year is None:
            return ""
        if self.precision == DatePrecision.YEAR:
            return f"{self.year:04d}"
        if self.precision == DatePrecision.MONTH:
            return f"{self.year:04d}-{self.month:02d}"
        if self.precision == DatePrecision.FULL:
            return f"{self.year:04d}-{self.month:02d}-{self.day:02d}"
        return ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'original': self.original,
            'year': self.year,
            'month': self.month,
            'day': self.day,
            'precision': self.precision.value,
            'is_valid': self.is_valid,
            'iso_partial': self.iso_partial,
            'error': self.error
        }


class DateHandler:
    """
    Handler for clinical trial date processing.

    Features:
    - Automatic format detection
    - Partial date imputation following clinical trial rules
    - SAS date (numeric) conversion
    - Batch processing for dataframes

    Example:
        handler = DateHandler()

        # Parse a single date
        parsed = handler.parse_date("2024-01-15")
        print(parsed.iso_partial)  # "2024-01-15"

        # Impute partial dates
        imputed = handler.impute_date(parsed, ImputationRule.FIRST)
        print(imputed)  # datetime(2024, 1, 15)

        # Process a dataframe column
        df = handler.standardize_column(df, 'AESTDTC')
    """

    # SAS epoch (January 1, 1960)
    SAS_EPOCH = date(1960, 1, 1)

    # Common date patterns in clinical data
    DATE_PATTERNS = [
        # ISO 8601 formats
        (r'^(\d{4})-(\d{2})-(\d{2})$', 'YYYY-MM-DD'),
        (r'^(\d{4})-(\d{2})$', 'YYYY-MM'),
        (r'^(\d{4})$', 'YYYY'),

        # ISO with time (extract date part)
        (r'^(\d{4})-(\d{2})-(\d{2})T', 'YYYY-MM-DDT...'),

        # US formats
        (r'^(\d{2})/(\d{2})/(\d{4})$', 'MM/DD/YYYY'),
        (r'^(\d{2})-(\d{2})-(\d{4})$', 'MM-DD-YYYY'),

        # European formats
        (r'^(\d{2})/(\d{2})/(\d{4})$', 'DD/MM/YYYY'),  # Ambiguous with US
        (r'^(\d{2})\.(\d{2})\.(\d{4})$', 'DD.MM.YYYY'),

        # Text month formats
        (r'^(\d{2})-([A-Za-z]{3})-(\d{4})$', 'DD-MON-YYYY'),
        (r'^(\d{2})([A-Za-z]{3})(\d{4})$', 'DDMONYYYY'),

        # Partial with UN (unknown) markers
        (r'^(\d{4})-(\d{2})-UN$', 'YYYY-MM-UN'),
        (r'^(\d{4})-UN-UN$', 'YYYY-UN-UN'),
        (r'^UN([A-Za-z]{3})(\d{4})$', 'UNMONYYYY'),
    ]

    MONTH_NAMES = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }

    def __init__(self, default_imputation: ImputationRule = ImputationRule.FIRST):
        """
        Initialize date handler.

        Args:
            default_imputation: Default rule for imputing missing components
        """
        self.default_imputation = default_imputation

    def parse_date(self, value: Any) -> ParsedDate:
        """
        Parse a date value from various formats.

        Args:
            value: Date value (string, datetime, numeric SAS date, etc.)

        Returns:
            ParsedDate object with components and precision
        """
        original = str(value) if value is not None else ""

        # Handle None/NaN/empty
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return ParsedDate(original="", year=None, month=None, day=None,
                            precision=DatePrecision.UNKNOWN, is_valid=False)

        if isinstance(value, str) and value.strip() == "":
            return ParsedDate(original="", year=None, month=None, day=None,
                            precision=DatePrecision.UNKNOWN, is_valid=False)

        # Handle datetime objects
        if isinstance(value, datetime):
            return ParsedDate(
                original=original,
                year=value.year,
                month=value.month,
                day=value.day,
                precision=DatePrecision.FULL,
                is_valid=True
            )

        if isinstance(value, date):
            return ParsedDate(
                original=original,
                year=value.year,
                month=value.month,
                day=value.day,
                precision=DatePrecision.FULL,
                is_valid=True
            )

        # Handle pandas Timestamp
        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return ParsedDate(original="", year=None, month=None, day=None,
                                precision=DatePrecision.UNKNOWN, is_valid=False)
            return ParsedDate(
                original=original,
                year=value.year,
                month=value.month,
                day=value.day,
                precision=DatePrecision.FULL,
                is_valid=True
            )

        # Handle numeric (SAS date)
        if isinstance(value, (int, float)) and not np.isnan(value):
            try:
                converted = self._from_sas_date(int(value))
                return ParsedDate(
                    original=original,
                    year=converted.year,
                    month=converted.month,
                    day=converted.day,
                    precision=DatePrecision.FULL,
                    is_valid=True
                )
            except Exception as e:
                return ParsedDate(
                    original=original,
                    year=None, month=None, day=None,
                    precision=DatePrecision.UNKNOWN,
                    is_valid=False,
                    error=f"Invalid SAS date: {e}"
                )

        # Handle string formats
        if isinstance(value, str):
            return self._parse_string_date(value.strip())

        return ParsedDate(
            original=original,
            year=None, month=None, day=None,
            precision=DatePrecision.UNKNOWN,
            is_valid=False,
            error=f"Unsupported type: {type(value)}"
        )

    def _parse_string_date(self, value: str) -> ParsedDate:
        """Parse a string date value."""
        if not value:
            return ParsedDate(original=value, year=None, month=None, day=None,
                            precision=DatePrecision.UNKNOWN, is_valid=False)

        # Try ISO 8601 first (most common in clinical data)
        iso_result = self._try_iso_format(value)
        if iso_result.is_valid:
            return iso_result

        # Try other patterns
        for pattern, fmt in self.DATE_PATTERNS:
            match = re.match(pattern, value, re.IGNORECASE)
            if match:
                try:
                    return self._parse_match(match, fmt, value)
                except Exception:
                    continue

        # Try pandas parser as fallback
        try:
            dt = pd.to_datetime(value)
            if not pd.isna(dt):
                return ParsedDate(
                    original=value,
                    year=dt.year,
                    month=dt.month,
                    day=dt.day,
                    precision=DatePrecision.FULL,
                    is_valid=True
                )
        except Exception:
            pass

        return ParsedDate(
            original=value,
            year=None, month=None, day=None,
            precision=DatePrecision.UNKNOWN,
            is_valid=False,
            error=f"Could not parse date: {value}"
        )

    def _try_iso_format(self, value: str) -> ParsedDate:
        """Try parsing ISO 8601 format."""
        # Full date: YYYY-MM-DD
        if re.match(r'^\d{4}-\d{2}-\d{2}$', value):
            parts = value.split('-')
            return ParsedDate(
                original=value,
                year=int(parts[0]),
                month=int(parts[1]),
                day=int(parts[2]),
                precision=DatePrecision.FULL,
                is_valid=True
            )

        # Year-month: YYYY-MM
        if re.match(r'^\d{4}-\d{2}$', value):
            parts = value.split('-')
            return ParsedDate(
                original=value,
                year=int(parts[0]),
                month=int(parts[1]),
                day=None,
                precision=DatePrecision.MONTH,
                is_valid=True
            )

        # Year only: YYYY
        if re.match(r'^\d{4}$', value):
            return ParsedDate(
                original=value,
                year=int(value),
                month=None,
                day=None,
                precision=DatePrecision.YEAR,
                is_valid=True
            )

        # ISO with time: YYYY-MM-DDTHH:MM:SS
        if re.match(r'^\d{4}-\d{2}-\d{2}T', value):
            date_part = value.split('T')[0]
            parts = date_part.split('-')
            return ParsedDate(
                original=value,
                year=int(parts[0]),
                month=int(parts[1]),
                day=int(parts[2]),
                precision=DatePrecision.FULL,
                is_valid=True
            )

        return ParsedDate(original=value, year=None, month=None, day=None,
                         precision=DatePrecision.UNKNOWN, is_valid=False)

    def _parse_match(self, match: re.Match, fmt: str, original: str) -> ParsedDate:
        """Parse a regex match based on format."""
        groups = match.groups()

        if fmt in ['YYYY-MM-DD', 'YYYY-MM-DDT...']:
            return ParsedDate(
                original=original,
                year=int(groups[0]),
                month=int(groups[1]),
                day=int(groups[2]),
                precision=DatePrecision.FULL,
                is_valid=True
            )

        if fmt == 'YYYY-MM':
            return ParsedDate(
                original=original,
                year=int(groups[0]),
                month=int(groups[1]),
                day=None,
                precision=DatePrecision.MONTH,
                is_valid=True
            )

        if fmt == 'YYYY':
            return ParsedDate(
                original=original,
                year=int(groups[0]),
                month=None,
                day=None,
                precision=DatePrecision.YEAR,
                is_valid=True
            )

        if fmt in ['DD-MON-YYYY', 'DDMONYYYY']:
            month = self.MONTH_NAMES.get(groups[1].lower())
            if month:
                return ParsedDate(
                    original=original,
                    year=int(groups[2]),
                    month=month,
                    day=int(groups[0]),
                    precision=DatePrecision.FULL,
                    is_valid=True
                )

        if fmt in ['MM/DD/YYYY', 'MM-DD-YYYY']:
            return ParsedDate(
                original=original,
                year=int(groups[2]),
                month=int(groups[0]),
                day=int(groups[1]),
                precision=DatePrecision.FULL,
                is_valid=True
            )

        if fmt == 'DD.MM.YYYY':
            return ParsedDate(
                original=original,
                year=int(groups[2]),
                month=int(groups[1]),
                day=int(groups[0]),
                precision=DatePrecision.FULL,
                is_valid=True
            )

        raise ValueError(f"Unsupported format: {fmt}")

    def _from_sas_date(self, sas_days: int) -> date:
        """Convert SAS date (days since 1960-01-01) to Python date."""
        return self.SAS_EPOCH + timedelta(days=sas_days)

    def _to_sas_date(self, dt: date) -> int:
        """Convert Python date to SAS date (days since 1960-01-01)."""
        return (dt - self.SAS_EPOCH).days

    def impute_date(self, parsed: ParsedDate, rule: Optional[ImputationRule] = None) -> Optional[date]:
        """
        Impute missing date components.

        Args:
            parsed: ParsedDate object
            rule: Imputation rule (uses default if not specified)

        Returns:
            Complete date or None if cannot impute
        """
        if not parsed.is_valid or parsed.year is None:
            return None

        rule = rule or self.default_imputation

        if rule == ImputationRule.NONE:
            if parsed.precision == DatePrecision.FULL:
                return date(parsed.year, parsed.month, parsed.day)
            return None

        year = parsed.year
        month = parsed.month
        day = parsed.day

        # Impute month if missing
        if month is None:
            if rule == ImputationRule.FIRST:
                month = 1
            elif rule == ImputationRule.LAST:
                month = 12
            elif rule == ImputationRule.MIDDLE:
                month = 6

        # Impute day if missing
        if day is None:
            if rule == ImputationRule.FIRST:
                day = 1
            elif rule == ImputationRule.LAST:
                # Get last day of month
                if month == 12:
                    day = 31
                else:
                    next_month = date(year, month + 1, 1)
                    day = (next_month - timedelta(days=1)).day
            elif rule == ImputationRule.MIDDLE:
                day = 15

        try:
            return date(year, month, day)
        except ValueError as e:
            logger.warning(f"Invalid date after imputation: {year}-{month}-{day}: {e}")
            return None

    def standardize_column(self, df: pd.DataFrame, column: str,
                          output_column: Optional[str] = None,
                          imputation_rule: Optional[ImputationRule] = None,
                          add_precision_column: bool = True) -> pd.DataFrame:
        """
        Standardize a date column in a dataframe.

        Args:
            df: Input dataframe
            column: Column name to process
            output_column: Output column name (defaults to column + '_ISO')
            imputation_rule: Rule for imputing partial dates
            add_precision_column: Whether to add a column showing date precision

        Returns:
            DataFrame with standardized date column(s)
        """
        df = df.copy()

        if column not in df.columns:
            logger.warning(f"Column '{column}' not found in dataframe")
            return df

        output_col = output_column or f"{column}_ISO"
        precision_col = f"{column}_PRECISION"

        iso_dates = []
        precisions = []
        imputed_dates = []

        rule = imputation_rule or self.default_imputation

        for value in df[column]:
            parsed = self.parse_date(value)
            iso_dates.append(parsed.iso_partial)
            precisions.append(parsed.precision.value)

            if rule != ImputationRule.NONE:
                imputed = self.impute_date(parsed, rule)
                imputed_dates.append(imputed)

        df[output_col] = iso_dates

        if add_precision_column:
            df[precision_col] = precisions

        if rule != ImputationRule.NONE:
            df[f"{column}_IMPUTED"] = imputed_dates

        return df

    def get_date_statistics(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        """
        Get statistics about dates in a column.

        Args:
            df: DataFrame
            column: Column name

        Returns:
            Dictionary with date statistics
        """
        if column not in df.columns:
            return {'error': f"Column '{column}' not found"}

        stats = {
            'total_rows': len(df),
            'null_count': 0,
            'valid_count': 0,
            'invalid_count': 0,
            'precision': {
                'full': 0,
                'month': 0,
                'year': 0,
                'unknown': 0
            },
            'date_range': {
                'min': None,
                'max': None
            },
            'sample_values': []
        }

        valid_dates = []

        for value in df[column]:
            parsed = self.parse_date(value)

            if parsed.original == "" or parsed.original == "None":
                stats['null_count'] += 1
            elif parsed.is_valid:
                stats['valid_count'] += 1
                stats['precision'][parsed.precision.value] += 1

                if parsed.precision == DatePrecision.FULL:
                    valid_dates.append(date(parsed.year, parsed.month, parsed.day))
            else:
                stats['invalid_count'] += 1
                if len(stats['sample_values']) < 5:
                    stats['sample_values'].append({
                        'value': parsed.original,
                        'error': parsed.error
                    })

        if valid_dates:
            stats['date_range']['min'] = min(valid_dates).isoformat()
            stats['date_range']['max'] = max(valid_dates).isoformat()

        return stats
