# SAGE - Clinical Naming Service
# ===============================
"""
Clinical Naming Service
=======================
Provides user-friendly names for clinical data elements.

Naming Priority:
1. Golden Metadata (Factory 2) - Study-specific, human-approved labels
2. CDISC Library - Standard definitions for SDTM/ADaM variables
3. Technical name - Fall back to original name if nothing found

This ensures responses use clinical-friendly terminology instead of
technical database names like "AEDECOD" or "SAFFL".
"""

import json
import logging
import threading
from pathlib import Path
from typing import Dict, Optional, Any
from functools import lru_cache

logger = logging.getLogger(__name__)


class ClinicalNamingService:
    """
    Service for translating technical names to clinical-friendly names.

    Uses a priority-based lookup:
    1. Golden Metadata (study-specific)
    2. CDISC Library (standard definitions)
    3. Original name (fallback)

    Example:
        naming = ClinicalNamingService(metadata_path, cdisc_db_path)
        friendly = naming.get_column_label("AEDECOD")
        # Returns: "Dictionary-Derived Adverse Event Term"
    """

    def __init__(self,
                 metadata_path: str = None,
                 cdisc_db_path: str = None):
        """
        Initialize the naming service.

        Args:
            metadata_path: Path to golden_metadata.json (Factory 2 output)
            cdisc_db_path: Path to CDISC library database
        """
        self.metadata_path = metadata_path
        self.cdisc_db_path = cdisc_db_path

        # Loaded data caches
        self._golden_metadata: Dict[str, Any] = {}
        self._cdisc_cache: Dict[str, str] = {}

        # Load golden metadata if available
        if metadata_path:
            self._load_golden_metadata(metadata_path)

        # Load CDISC library if available
        if cdisc_db_path:
            self._load_cdisc_library(cdisc_db_path)

    def _load_golden_metadata(self, path: str):
        """Load golden metadata from JSON file."""
        try:
            metadata_file = Path(path)
            if metadata_file.exists():
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # Index by variable name for fast lookup
                # Handle both flat and nested structures
                if 'variables' in data:
                    for var in data['variables']:
                        var_name = var.get('name', '').upper()
                        if var_name:
                            self._golden_metadata[var_name] = {
                                'label': var.get('label', ''),
                                'description': var.get('description', ''),
                                'table': var.get('table', var.get('dataset', ''))
                            }
                elif 'datasets' in data:
                    # Nested by dataset
                    for dataset_name, dataset_info in data.get('datasets', {}).items():
                        for var in dataset_info.get('variables', []):
                            var_name = var.get('name', '').upper()
                            if var_name:
                                self._golden_metadata[var_name] = {
                                    'label': var.get('label', ''),
                                    'description': var.get('description', ''),
                                    'table': dataset_name
                                }
                else:
                    # Flat structure keyed by variable name
                    for var_name, var_info in data.items():
                        if isinstance(var_info, dict):
                            self._golden_metadata[var_name.upper()] = {
                                'label': var_info.get('label', ''),
                                'description': var_info.get('description', ''),
                                'table': var_info.get('table', var_info.get('dataset', ''))
                            }

                logger.info(f"Loaded {len(self._golden_metadata)} variables from golden metadata")
        except Exception as e:
            logger.warning(f"Could not load golden metadata: {e}")

    def _load_cdisc_library(self, db_path: str):
        """Load CDISC library variable definitions."""
        try:
            import duckdb

            db_file = Path(db_path)
            if not db_file.exists():
                logger.warning(f"CDISC library not found at {db_path}")
                return

            conn = duckdb.connect(str(db_file), read_only=True)

            # Try to load from variables table
            try:
                # Check what tables exist
                tables = conn.execute("SHOW TABLES").fetchall()
                table_names = [t[0] for t in tables]

                # Look for variable definitions in common table names
                var_tables = ['variables', 'cdisc_variables', 'sdtm_variables', 'adam_variables']

                for table in var_tables:
                    if table in table_names:
                        try:
                            results = conn.execute(f"""
                                SELECT name, label, description
                                FROM {table}
                                WHERE name IS NOT NULL
                            """).fetchall()

                            for row in results:
                                var_name = row[0].upper() if row[0] else ''
                                if var_name and var_name not in self._cdisc_cache:
                                    # Prefer label, fall back to description
                                    self._cdisc_cache[var_name] = row[1] or row[2] or ''

                            logger.info(f"Loaded {len(results)} variables from CDISC {table}")
                        except Exception:
                            continue

            finally:
                conn.close()

        except Exception as e:
            logger.warning(f"Could not load CDISC library: {e}")

    def get_column_label(self, column_name: str, table_name: str = None) -> str:
        """
        Get user-friendly label for a column.

        Priority:
        1. Golden Metadata (study-specific)
        2. CDISC Library (standard)
        3. Original column name (fallback)

        Args:
            column_name: Technical column name (e.g., "AEDECOD")
            table_name: Optional table context for disambiguation

        Returns:
            User-friendly label
        """
        if not column_name:
            return column_name

        col_upper = column_name.upper()

        # 1. Check golden metadata first
        if col_upper in self._golden_metadata:
            meta = self._golden_metadata[col_upper]
            label = meta.get('label', '')
            if label:
                return label

        # 2. Check CDISC library
        if col_upper in self._cdisc_cache:
            label = self._cdisc_cache[col_upper]
            if label:
                return label

        # 3. Fall back to original name
        return column_name

    def get_column_description(self, column_name: str) -> str:
        """Get detailed description for a column."""
        if not column_name:
            return ""

        col_upper = column_name.upper()

        # Check golden metadata
        if col_upper in self._golden_metadata:
            return self._golden_metadata[col_upper].get('description', '')

        # Check CDISC cache (may have description as label)
        if col_upper in self._cdisc_cache:
            return self._cdisc_cache[col_upper]

        return ""

    def get_table_label(self, table_name: str) -> str:
        """
        Get user-friendly label for a table/dataset.

        Args:
            table_name: Technical table name (e.g., "ADAE")

        Returns:
            User-friendly label (e.g., "Adverse Events Analysis Dataset")
        """
        if not table_name:
            return table_name

        # Standard CDISC dataset names
        table_labels = {
            # ADaM datasets
            'ADAE': 'Adverse Events Analysis Dataset',
            'ADSL': 'Subject-Level Analysis Dataset',
            'ADLB': 'Laboratory Analysis Dataset',
            'ADCM': 'Concomitant Medications Analysis Dataset',
            'ADVS': 'Vital Signs Analysis Dataset',
            'ADEX': 'Exposure Analysis Dataset',
            'ADTTE': 'Time-to-Event Analysis Dataset',
            'ADEG': 'ECG Analysis Dataset',
            'ADMH': 'Medical History Analysis Dataset',
            'ADEFF': 'Efficacy Analysis Dataset',

            # SDTM datasets
            'AE': 'Adverse Events',
            'DM': 'Demographics',
            'LB': 'Laboratory Test Results',
            'CM': 'Concomitant Medications',
            'VS': 'Vital Signs',
            'EX': 'Exposure',
            'MH': 'Medical History',
            'EG': 'ECG Test Results',
            'DS': 'Disposition',
            'SV': 'Subject Visits',
            'SC': 'Subject Characteristics',
            'PE': 'Physical Examination',
            'QS': 'Questionnaires',
            'FA': 'Findings About',
            'IE': 'Inclusion/Exclusion Criteria',
        }

        return table_labels.get(table_name.upper(), table_name)

    def get_population_description(self, population_name: str, filter_sql: str = None) -> str:
        """
        Get user-friendly description for a population.

        Args:
            population_name: Population name (e.g., "Safety")
            filter_sql: Optional SQL filter for context

        Returns:
            User-friendly description
        """
        descriptions = {
            'Safety': 'Safety Population (all subjects who received study treatment)',
            'Safety Population': 'Safety Population (all subjects who received study treatment)',
            'ITT': 'Intent-to-Treat Population (all randomized subjects)',
            'Intent-to-Treat': 'Intent-to-Treat Population (all randomized subjects)',
            'mITT': 'Modified Intent-to-Treat Population',
            'Efficacy': 'Efficacy Evaluable Population',
            'Per-Protocol': 'Per-Protocol Population (subjects who completed without major deviations)',
            'PP': 'Per-Protocol Population (subjects who completed without major deviations)',
            'All': 'All Subjects',
            'Full Analysis Set': 'Full Analysis Set (all randomized subjects)',
            'FAS': 'Full Analysis Set (all randomized subjects)',
        }

        return descriptions.get(population_name, population_name)

    def get_filter_description(self, filter_sql: str) -> str:
        """
        Convert SQL filter to plain English description.

        Args:
            filter_sql: SQL WHERE clause fragment

        Returns:
            Plain English description
        """
        if not filter_sql:
            return ""

        # Common filter translations
        filter_map = {
            "SAFFL = 'Y'": "Subjects in the Safety Population",
            "ITTFL = 'Y'": "Subjects in the Intent-to-Treat Population",
            "EFFFL = 'Y'": "Subjects in the Efficacy Population",
            "PPROTFL = 'Y'": "Subjects in the Per-Protocol Population",
            "FASFL = 'Y'": "Subjects in the Full Analysis Set",
            "TRTEMFL = 'Y'": "Treatment-emergent events only",
            "AESER = 'Y'": "Serious adverse events only",
            "AOCCFL = 'Y'": "First occurrence of event",
            "ANL01FL = 'Y'": "Analysis records only",
        }

        # Check exact match first
        if filter_sql in filter_map:
            return filter_map[filter_sql]

        # Try to parse common patterns
        filter_upper = filter_sql.upper()

        if 'SAFFL' in filter_upper:
            return "Subjects in the Safety Population"
        if 'ITTFL' in filter_upper:
            return "Subjects in the Intent-to-Treat Population"
        if 'TRTEMFL' in filter_upper:
            return "Treatment-emergent events only"
        if 'AESER' in filter_upper:
            return "Serious adverse events only"

        # Return original if no translation
        return f"Filter: {filter_sql}"

    def format_columns_friendly(self, columns: list) -> list:
        """Convert a list of column names to friendly names."""
        return [self.get_column_label(col) for col in columns]

    def get_statistics(self) -> Dict[str, int]:
        """Get statistics about loaded naming data."""
        return {
            'golden_metadata_variables': len(self._golden_metadata),
            'cdisc_library_variables': len(self._cdisc_cache),
            'total_variables': len(self._golden_metadata) + len(self._cdisc_cache)
        }


# Global instance for convenience (thread-safe initialization)
_naming_service: Optional[ClinicalNamingService] = None
_naming_service_lock = threading.Lock()


def get_naming_service(metadata_path: str = None, cdisc_db_path: str = None) -> ClinicalNamingService:
    """
    Get or create the global naming service instance.

    Uses double-checked locking for thread-safe singleton initialization.
    """
    global _naming_service

    # Fast path: if already initialized, return immediately
    if _naming_service is not None:
        return _naming_service

    # Slow path: acquire lock and check again
    with _naming_service_lock:
        # Double-check after acquiring lock
        if _naming_service is None:
            _naming_service = ClinicalNamingService(
                metadata_path=metadata_path,
                cdisc_db_path=cdisc_db_path
            )

    return _naming_service


def get_friendly_name(column_name: str, table_name: str = None) -> str:
    """
    Convenience function to get friendly name for a column.

    Falls back to original name if naming service not initialized.
    """
    if _naming_service:
        return _naming_service.get_column_label(column_name, table_name)
    return column_name
