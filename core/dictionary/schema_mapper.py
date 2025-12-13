# SAGE Dictionary - Schema Mapper
# ================================
"""
Generates schema_map.json for quick column → table lookups.
Combines DuckDB schema with golden metadata for rich descriptions.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field, asdict

import duckdb

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """Information about a database column."""
    name: str
    tables: List[str]
    data_type: str
    is_key: bool = False
    description: str = ""
    codelist: Optional[str] = None
    unique_values_count: int = 0
    sample_values: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "tables": self.tables,
            "type": self.data_type,
            "is_key": self.is_key,
            "description": self.description,
            "codelist": self.codelist,
            "unique_values_count": self.unique_values_count,
            "sample_values": self.sample_values[:5]  # Limit sample size
        }


@dataclass
class TableInfo:
    """Information about a database table."""
    name: str
    columns: List[str]
    row_count: int
    description: str = ""
    domain_type: str = ""  # "SDTM", "ADaM", "custom"
    key_columns: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "columns": self.columns,
            "row_count": self.row_count,
            "description": self.description,
            "domain_type": self.domain_type,
            "key_columns": self.key_columns
        }


@dataclass
class SchemaMap:
    """Complete schema map with columns and tables."""
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)
    tables: Dict[str, TableInfo] = field(default_factory=dict)
    generated_at: str = ""
    version: str = "1.0"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "columns": {k: v.to_dict() for k, v in self.columns.items()},
            "tables": {k: v.to_dict() for k, v in self.tables.items()},
            "generated_at": self.generated_at,
            "version": self.version
        }


# Known key columns in CDISC standards
KEY_COLUMNS = {
    "USUBJID", "STUDYID", "DOMAIN", "SEQ", "SPDEVID",
    "SUBJID", "SITEID", "VISIT", "VISITNUM"
}

# Domain type detection patterns
SDTM_DOMAINS = {
    "DM", "AE", "CM", "LB", "VS", "EX", "MH", "PE", "EG",
    "DS", "SU", "SC", "FA", "QS", "TU", "TR", "RS", "EC"
}

ADAM_DOMAINS = {
    "ADSL", "ADAE", "ADLB", "ADVS", "ADCM", "ADMH", "ADEG",
    "ADEFF", "ADTTE", "ADPC", "ADPP", "ADEX"
}


class SchemaMapper:
    """
    Generates schema_map.json from DuckDB and metadata.

    Provides quick lookups for:
    - Column → tables mapping
    - Table → columns mapping
    - Variable descriptions
    - Key column identification
    """

    def __init__(self,
                 db_path: str,
                 metadata_path: Optional[str] = None):
        """
        Initialize schema mapper.

        Args:
            db_path: Path to clinical_data.duckdb
            metadata_path: Optional path to golden_metadata.json
        """
        self.db_path = Path(db_path)
        self.metadata_path = Path(metadata_path) if metadata_path else None
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._metadata: Optional[Dict] = None

        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = duckdb.connect(str(self.db_path))
        return self._conn

    def _load_metadata(self) -> Dict:
        """Load golden metadata if available."""
        if self._metadata is not None:
            return self._metadata

        if self.metadata_path and self.metadata_path.exists():
            with open(self.metadata_path, 'r') as f:
                self._metadata = json.load(f)
        else:
            self._metadata = {}

        return self._metadata

    def get_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        conn = self._get_connection()
        result = conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get column schema for a table."""
        conn = self._get_connection()
        result = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
        return [{"name": row[0], "type": row[1]} for row in result]

    def get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        conn = self._get_connection()
        result = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
        return result[0] if result else 0

    def get_unique_count(self, table_name: str, column_name: str) -> int:
        """Get unique value count for a column."""
        conn = self._get_connection()
        try:
            result = conn.execute(
                f'SELECT COUNT(DISTINCT "{column_name}") FROM "{table_name}"'
            ).fetchone()
            return result[0] if result else 0
        except Exception:
            return 0

    def get_sample_values(self,
                          table_name: str,
                          column_name: str,
                          limit: int = 5) -> List[str]:
        """Get sample values from a column."""
        conn = self._get_connection()
        try:
            result = conn.execute(f'''
                SELECT DISTINCT "{column_name}"
                FROM "{table_name}"
                WHERE "{column_name}" IS NOT NULL
                LIMIT {limit}
            ''').fetchall()
            return [str(row[0]) for row in result if row[0]]
        except Exception:
            return []

    def get_variable_description(self, table: str, column: str) -> str:
        """Get variable description from metadata."""
        metadata = self._load_metadata()

        if not metadata:
            return ""

        # Try domains section
        domains = metadata.get("domains", {})
        table_upper = table.upper()

        if table_upper in domains:
            variables = domains[table_upper].get("variables", {})
            column_upper = column.upper()
            if column_upper in variables:
                return variables[column_upper].get("description", "")

        return ""

    def get_codelist(self, table: str, column: str) -> Optional[str]:
        """Get codelist name from metadata."""
        metadata = self._load_metadata()

        if not metadata:
            return None

        domains = metadata.get("domains", {})
        table_upper = table.upper()

        if table_upper in domains:
            variables = domains[table_upper].get("variables", {})
            column_upper = column.upper()
            if column_upper in variables:
                return variables[column_upper].get("codelist")

        return None

    def _detect_domain_type(self, table_name: str) -> str:
        """Detect if table is SDTM, ADaM, or custom."""
        name_upper = table_name.upper()

        if name_upper in SDTM_DOMAINS:
            return "SDTM"
        elif name_upper in ADAM_DOMAINS:
            return "ADaM"
        elif name_upper.startswith("AD"):
            return "ADaM"
        else:
            return "custom"

    def _is_key_column(self, column_name: str) -> bool:
        """Check if column is a key column."""
        name_upper = column_name.upper()

        if name_upper in KEY_COLUMNS:
            return True

        # Check for SEQ suffix (e.g., AESEQ, CMSEQ)
        if name_upper.endswith("SEQ"):
            return True

        return False

    def build_schema_map(self,
                         include_samples: bool = True,
                         progress_callback: Optional[callable] = None
                         ) -> SchemaMap:
        """
        Build complete schema map.

        Args:
            include_samples: Include sample values for each column
            progress_callback: Optional callback(table, current, total)

        Returns:
            SchemaMap object
        """
        import datetime

        tables = self.get_tables()
        schema_map = SchemaMap(
            generated_at=datetime.datetime.now().isoformat()
        )

        # Track columns across tables
        column_tables: Dict[str, Set[str]] = {}
        column_info: Dict[str, Dict[str, Any]] = {}

        for i, table_name in enumerate(tables):
            if progress_callback:
                progress_callback(table_name, i + 1, len(tables))

            table_upper = table_name.upper()
            schema = self.get_table_schema(table_name)
            row_count = self.get_row_count(table_name)

            # Build table info
            columns = [col["name"].upper() for col in schema]
            key_columns = [c for c in columns if self._is_key_column(c)]

            table_info = TableInfo(
                name=table_upper,
                columns=columns,
                row_count=row_count,
                description=self._get_table_description(table_upper),
                domain_type=self._detect_domain_type(table_name),
                key_columns=key_columns
            )
            schema_map.tables[table_upper] = table_info

            # Process columns
            for col in schema:
                col_name = col["name"].upper()
                col_type = col["type"]

                if col_name not in column_tables:
                    column_tables[col_name] = set()
                    column_info[col_name] = {
                        "type": col_type,
                        "unique_counts": [],
                        "samples": []
                    }

                column_tables[col_name].add(table_upper)

                # Get unique count and samples
                unique_count = self.get_unique_count(table_name, col["name"])
                column_info[col_name]["unique_counts"].append(unique_count)

                if include_samples and "VARCHAR" in col_type.upper():
                    samples = self.get_sample_values(table_name, col["name"])
                    column_info[col_name]["samples"].extend(samples)

        # Build column entries
        for col_name, tables_set in column_tables.items():
            info = column_info[col_name]

            # Get description from first table that has it
            description = ""
            codelist = None
            for table in tables_set:
                desc = self.get_variable_description(table, col_name)
                if desc:
                    description = desc
                    codelist = self.get_codelist(table, col_name)
                    break

            # Deduplicate samples
            unique_samples = list(dict.fromkeys(info["samples"]))[:5]

            col_info = ColumnInfo(
                name=col_name,
                tables=sorted(list(tables_set)),
                data_type=info["type"],
                is_key=self._is_key_column(col_name),
                description=description,
                codelist=codelist,
                unique_values_count=max(info["unique_counts"]) if info["unique_counts"] else 0,
                sample_values=unique_samples
            )
            schema_map.columns[col_name] = col_info

        logger.info(
            f"Built schema map with {len(schema_map.tables)} tables "
            f"and {len(schema_map.columns)} columns"
        )

        return schema_map

    def _get_table_description(self, table_name: str) -> str:
        """Get table description from metadata or use default."""
        metadata = self._load_metadata()

        if metadata:
            domains = metadata.get("domains", {})
            if table_name in domains:
                return domains[table_name].get("description", "")

        # Default descriptions for common domains
        descriptions = {
            "DM": "Demographics",
            "AE": "Adverse Events",
            "CM": "Concomitant Medications",
            "LB": "Laboratory Tests",
            "VS": "Vital Signs",
            "EX": "Exposure",
            "MH": "Medical History",
            "PE": "Physical Examination",
            "EG": "ECG",
            "DS": "Disposition",
            "ADSL": "Subject-Level Analysis Dataset",
            "ADAE": "Adverse Events Analysis Dataset",
            "ADLB": "Laboratory Analysis Dataset",
            "ADVS": "Vital Signs Analysis Dataset",
            "ADCM": "Concomitant Medications Analysis Dataset",
        }

        return descriptions.get(table_name.upper(), "")

    def save_schema_map(self,
                        schema_map: SchemaMap,
                        output_path: str) -> None:
        """Save schema map to JSON file."""
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        with open(output, 'w') as f:
            json.dump(schema_map.to_dict(), f, indent=2)

        logger.info(f"Saved schema map to {output_path}")

    @classmethod
    def load_schema_map(cls, path: str) -> SchemaMap:
        """Load schema map from JSON file."""
        with open(path, 'r') as f:
            data = json.load(f)

        schema_map = SchemaMap(
            generated_at=data.get("generated_at", ""),
            version=data.get("version", "1.0")
        )

        # Reconstruct columns
        for col_name, col_data in data.get("columns", {}).items():
            schema_map.columns[col_name] = ColumnInfo(
                name=col_data["name"],
                tables=col_data["tables"],
                data_type=col_data["type"],
                is_key=col_data.get("is_key", False),
                description=col_data.get("description", ""),
                codelist=col_data.get("codelist"),
                unique_values_count=col_data.get("unique_values_count", 0),
                sample_values=col_data.get("sample_values", [])
            )

        # Reconstruct tables
        for table_name, table_data in data.get("tables", {}).items():
            schema_map.tables[table_name] = TableInfo(
                name=table_data["name"],
                columns=table_data["columns"],
                row_count=table_data["row_count"],
                description=table_data.get("description", ""),
                domain_type=table_data.get("domain_type", ""),
                key_columns=table_data.get("key_columns", [])
            )

        return schema_map

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def build_schema_map(db_path: str,
                     metadata_path: Optional[str] = None,
                     output_path: str = "knowledge/schema_map.json") -> SchemaMap:
    """
    Convenience function to build and save schema map.

    Args:
        db_path: Path to DuckDB database
        metadata_path: Optional path to golden metadata
        output_path: Output file path

    Returns:
        SchemaMap object
    """
    with SchemaMapper(db_path, metadata_path) as mapper:
        schema_map = mapper.build_schema_map()
        mapper.save_schema_map(schema_map, output_path)
        return schema_map
