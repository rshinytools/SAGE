# SAGE MedDRA Loader
# ===================
"""
Loads MedDRA dictionary from SAS7BDAT files into DuckDB for fast lookups.

MedDRA Hierarchy:
- SOC (System Organ Class) - highest level
- HLGT (High Level Group Term)
- HLT (High Level Term)
- PT (Preferred Term) - primary reporting level
- LLT (Lowest Level Term) - most specific

Expected SAS7BDAT structure (mdhier format):
- SOC_CODE, SOC_NAME
- HLGT_CODE, HLGT_NAME
- HLT_CODE, HLT_NAME
- PT_CODE, PT_NAME
- LLT_CODE, LLT_NAME
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, asdict

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# Column mappings for different MedDRA file formats
COLUMN_MAPPINGS = {
    # Standard mdhier format
    "soc_code": ["soc_code", "soc_cd", "soccd", "primary_soc_cd"],
    "soc_name": ["soc_name", "soc_nm", "socname", "primary_soc_name"],
    "hlgt_code": ["hlgt_code", "hlgt_cd", "hlgtcd"],
    "hlgt_name": ["hlgt_name", "hlgt_nm", "hlgtname"],
    "hlt_code": ["hlt_code", "hlt_cd", "hltcd"],
    "hlt_name": ["hlt_name", "hlt_nm", "hltname"],
    "pt_code": ["pt_code", "pt_cd", "ptcd", "meddra_pt_cd"],
    "pt_name": ["pt_name", "pt_nm", "ptname", "meddra_pt_name"],
    "llt_code": ["llt_code", "llt_cd", "lltcd"],
    "llt_name": ["llt_name", "llt_nm", "lltname"],
}


@dataclass
class MedDRAVersion:
    """MedDRA version information."""
    version: str
    language: str
    loaded_at: str
    total_terms: int
    soc_count: int
    hlgt_count: int
    hlt_count: int
    pt_count: int
    llt_count: int
    file_path: str


@dataclass
class MedDRATerm:
    """A single MedDRA term."""
    code: str
    name: str
    level: str  # SOC, HLGT, HLT, PT, LLT
    parent_code: Optional[str] = None
    parent_name: Optional[str] = None


@dataclass
class MedDRAHierarchy:
    """Full hierarchy for a term."""
    soc: MedDRATerm
    hlgt: MedDRATerm
    hlt: MedDRATerm
    pt: MedDRATerm
    llt: Optional[MedDRATerm] = None


class MedDRALoader:
    """Loads and manages MedDRA dictionary."""

    def __init__(self, db_path: str, knowledge_dir: str):
        """
        Initialize MedDRA loader.

        Args:
            db_path: Path to DuckDB database
            knowledge_dir: Directory for MedDRA status file
        """
        self.db_path = Path(db_path)
        self.knowledge_dir = Path(knowledge_dir)
        self.status_path = self.knowledge_dir / "meddra_status.json"

        # Ensure directories exist
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.knowledge_dir.mkdir(parents=True, exist_ok=True)

    def load_from_sas(self, sas_path: str) -> MedDRAVersion:
        """
        Load MedDRA dictionary from SAS7BDAT file.

        Args:
            sas_path: Path to SAS7BDAT file

        Returns:
            MedDRAVersion with load statistics
        """
        sas_path = Path(sas_path)
        if not sas_path.exists():
            raise FileNotFoundError(f"SAS file not found: {sas_path}")

        logger.info(f"Loading MedDRA from {sas_path}")

        # Read SAS file
        try:
            df = pd.read_sas(sas_path, encoding='latin1')
        except Exception as e:
            # Try alternative encoding
            df = pd.read_sas(sas_path, encoding='utf-8')

        # Normalize column names
        df.columns = [c.lower().strip() for c in df.columns]

        # Map columns to standard names
        df = self._normalize_columns(df)

        # Create DuckDB tables
        self._create_tables()

        # Load data
        version = self._load_data(df, str(sas_path))

        # Save status
        self._save_status(version)

        logger.info(f"MedDRA loaded: {version.total_terms} terms")
        return version

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names to standard format."""
        rename_map = {}

        for standard_name, variants in COLUMN_MAPPINGS.items():
            for variant in variants:
                if variant in df.columns:
                    rename_map[variant] = standard_name
                    break

        if rename_map:
            df = df.rename(columns=rename_map)

        return df

    def _create_tables(self):
        """Create MedDRA tables in DuckDB."""
        conn = duckdb.connect(str(self.db_path))

        try:
            # Drop existing tables
            conn.execute("DROP TABLE IF EXISTS meddra_hierarchy")
            conn.execute("DROP TABLE IF EXISTS meddra_soc")
            conn.execute("DROP TABLE IF EXISTS meddra_hlgt")
            conn.execute("DROP TABLE IF EXISTS meddra_hlt")
            conn.execute("DROP TABLE IF EXISTS meddra_pt")
            conn.execute("DROP TABLE IF EXISTS meddra_llt")

            # Create hierarchy table (main lookup table)
            conn.execute("""
                CREATE TABLE meddra_hierarchy (
                    soc_code VARCHAR,
                    soc_name VARCHAR,
                    hlgt_code VARCHAR,
                    hlgt_name VARCHAR,
                    hlt_code VARCHAR,
                    hlt_name VARCHAR,
                    pt_code VARCHAR,
                    pt_name VARCHAR,
                    llt_code VARCHAR,
                    llt_name VARCHAR
                )
            """)

            # Create individual level tables for faster lookups
            # Note: No PRIMARY KEY constraints as MedDRA has many-to-many relationships
            conn.execute("""
                CREATE TABLE meddra_soc (
                    code VARCHAR,
                    name VARCHAR,
                    pt_count INTEGER DEFAULT 0
                )
            """)

            conn.execute("""
                CREATE TABLE meddra_hlgt (
                    code VARCHAR,
                    name VARCHAR,
                    soc_code VARCHAR
                )
            """)

            conn.execute("""
                CREATE TABLE meddra_hlt (
                    code VARCHAR,
                    name VARCHAR,
                    hlgt_code VARCHAR
                )
            """)

            conn.execute("""
                CREATE TABLE meddra_pt (
                    code VARCHAR,
                    name VARCHAR,
                    hlt_code VARCHAR,
                    name_upper VARCHAR
                )
            """)

            conn.execute("""
                CREATE TABLE meddra_llt (
                    code VARCHAR,
                    name VARCHAR,
                    pt_code VARCHAR,
                    name_upper VARCHAR
                )
            """)

        finally:
            conn.close()

    def _load_data(self, df: pd.DataFrame, file_path: str) -> MedDRAVersion:
        """Load data into DuckDB tables."""
        conn = duckdb.connect(str(self.db_path))

        try:
            # Ensure all required columns exist
            required_cols = ['soc_code', 'soc_name', 'hlgt_code', 'hlgt_name',
                           'hlt_code', 'hlt_name', 'pt_code', 'pt_name']

            for col in required_cols:
                if col not in df.columns:
                    df[col] = None

            # Add LLT columns if missing
            if 'llt_code' not in df.columns:
                df['llt_code'] = None
            if 'llt_name' not in df.columns:
                df['llt_name'] = None

            # Convert specific MedDRA code columns to strings and remove .0 suffix
            # Only convert the actual code columns, not columns like PATH_CODE
            code_columns = ['soc_code', 'hlgt_code', 'hlt_code', 'pt_code', 'llt_code']
            for col in code_columns:
                if col in df.columns:
                    # Convert to string, remove .0 suffix, handle NaN
                    df[col] = df[col].apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x) not in ['nan', 'None', ''] else None)

            # Load hierarchy table
            hierarchy_df = df[['soc_code', 'soc_name', 'hlgt_code', 'hlgt_name',
                              'hlt_code', 'hlt_name', 'pt_code', 'pt_name',
                              'llt_code', 'llt_name']].drop_duplicates()

            conn.execute("INSERT INTO meddra_hierarchy SELECT * FROM hierarchy_df")

            # Load SOC table (deduplicate by code)
            soc_df = df[['soc_code', 'soc_name']].drop_duplicates(subset=['soc_code'])
            soc_df = soc_df.rename(columns={'soc_code': 'code', 'soc_name': 'name'})
            soc_df = soc_df[soc_df['code'].notna()]
            conn.execute("INSERT INTO meddra_soc (code, name) SELECT code, name FROM soc_df")

            # Update SOC PT counts
            conn.execute("""
                UPDATE meddra_soc SET pt_count = (
                    SELECT COUNT(DISTINCT pt_code)
                    FROM meddra_hierarchy
                    WHERE meddra_hierarchy.soc_code = meddra_soc.code
                )
            """)

            # Load HLGT table (deduplicate by code - HLGT can belong to multiple SOCs)
            hlgt_df = df[['hlgt_code', 'hlgt_name', 'soc_code']].drop_duplicates(subset=['hlgt_code'])
            hlgt_df = hlgt_df.rename(columns={'hlgt_code': 'code', 'hlgt_name': 'name'})
            hlgt_df = hlgt_df[hlgt_df['code'].notna()]
            conn.execute("INSERT INTO meddra_hlgt SELECT code, name, soc_code FROM hlgt_df")

            # Load HLT table (deduplicate by code)
            hlt_df = df[['hlt_code', 'hlt_name', 'hlgt_code']].drop_duplicates(subset=['hlt_code'])
            hlt_df = hlt_df.rename(columns={'hlt_code': 'code', 'hlt_name': 'name'})
            hlt_df = hlt_df[hlt_df['code'].notna()]
            conn.execute("INSERT INTO meddra_hlt SELECT code, name, hlgt_code FROM hlt_df")

            # Load PT table (deduplicate by code)
            pt_df = df[['pt_code', 'pt_name', 'hlt_code']].drop_duplicates(subset=['pt_code'])
            pt_df = pt_df.rename(columns={'pt_code': 'code', 'pt_name': 'name'})
            pt_df = pt_df[pt_df['code'].notna()]
            pt_df['name_upper'] = pt_df['name'].str.upper()
            conn.execute("INSERT INTO meddra_pt SELECT code, name, hlt_code, name_upper FROM pt_df")

            # Load LLT table (deduplicate by code)
            llt_df = df[['llt_code', 'llt_name', 'pt_code']].drop_duplicates(subset=['llt_code'])
            llt_df = llt_df.rename(columns={'llt_code': 'code', 'llt_name': 'name'})
            llt_df = llt_df[llt_df['code'].notna()]
            llt_df['name_upper'] = llt_df['name'].str.upper()
            conn.execute("INSERT INTO meddra_llt SELECT code, name, pt_code, name_upper FROM llt_df")

            # Create indexes for fast lookups
            conn.execute("CREATE INDEX idx_pt_name ON meddra_pt(name_upper)")
            conn.execute("CREATE INDEX idx_pt_code ON meddra_pt(code)")
            conn.execute("CREATE INDEX idx_llt_name ON meddra_llt(name_upper)")
            conn.execute("CREATE INDEX idx_llt_code ON meddra_llt(code)")
            conn.execute("CREATE INDEX idx_soc_code ON meddra_soc(code)")
            conn.execute("CREATE INDEX idx_hlgt_code ON meddra_hlgt(code)")
            conn.execute("CREATE INDEX idx_hlgt_soc ON meddra_hlgt(soc_code)")
            conn.execute("CREATE INDEX idx_hlt_code ON meddra_hlt(code)")
            conn.execute("CREATE INDEX idx_hlt_hlgt ON meddra_hlt(hlgt_code)")
            conn.execute("CREATE INDEX idx_hierarchy_pt ON meddra_hierarchy(pt_code)")
            conn.execute("CREATE INDEX idx_hierarchy_llt ON meddra_hierarchy(llt_code)")
            conn.execute("CREATE INDEX idx_hierarchy_soc ON meddra_hierarchy(soc_code)")

            # Get counts
            soc_count = conn.execute("SELECT COUNT(*) FROM meddra_soc").fetchone()[0]
            hlgt_count = conn.execute("SELECT COUNT(*) FROM meddra_hlgt").fetchone()[0]
            hlt_count = conn.execute("SELECT COUNT(*) FROM meddra_hlt").fetchone()[0]
            pt_count = conn.execute("SELECT COUNT(*) FROM meddra_pt").fetchone()[0]
            llt_count = conn.execute("SELECT COUNT(*) FROM meddra_llt").fetchone()[0]

            total_terms = soc_count + hlgt_count + hlt_count + pt_count + llt_count

            # Try to detect version from filename or data
            version_str = self._detect_version(file_path, df)

            return MedDRAVersion(
                version=version_str,
                language="English",
                loaded_at=datetime.now().isoformat(),
                total_terms=total_terms,
                soc_count=soc_count,
                hlgt_count=hlgt_count,
                hlt_count=hlt_count,
                pt_count=pt_count,
                llt_count=llt_count,
                file_path=file_path
            )

        finally:
            conn.close()

    def _detect_version(self, file_path: str, df: pd.DataFrame) -> str:
        """Try to detect MedDRA version from filename or data."""
        file_name = Path(file_path).name.lower()

        # Common version patterns in filenames
        import re
        version_patterns = [
            r'meddra[_\s]*v?(\d+\.?\d*)',
            r'v(\d+\.?\d*)',
            r'(\d+\.\d+)',
        ]

        for pattern in version_patterns:
            match = re.search(pattern, file_name)
            if match:
                return match.group(1)

        # Check if version column exists in data
        version_cols = ['meddra_version', 'version', 'mdhier_version']
        for col in version_cols:
            if col in df.columns:
                versions = df[col].dropna().unique()
                if len(versions) > 0:
                    return str(versions[0])

        return "Unknown"

    def _save_status(self, version: MedDRAVersion):
        """Save MedDRA status to JSON file."""
        with open(self.status_path, 'w') as f:
            json.dump(asdict(version), f, indent=2)

    def get_status(self) -> Optional[MedDRAVersion]:
        """Get current MedDRA status."""
        if not self.status_path.exists():
            return None

        try:
            with open(self.status_path, 'r') as f:
                data = json.load(f)
                return MedDRAVersion(**data)
        except Exception as e:
            logger.warning(f"Failed to load MedDRA status: {e}")
            return None

    def delete_version(self):
        """Delete current MedDRA version."""
        conn = duckdb.connect(str(self.db_path))

        try:
            conn.execute("DROP TABLE IF EXISTS meddra_hierarchy")
            conn.execute("DROP TABLE IF EXISTS meddra_soc")
            conn.execute("DROP TABLE IF EXISTS meddra_hlgt")
            conn.execute("DROP TABLE IF EXISTS meddra_hlt")
            conn.execute("DROP TABLE IF EXISTS meddra_pt")
            conn.execute("DROP TABLE IF EXISTS meddra_llt")
        finally:
            conn.close()

        if self.status_path.exists():
            self.status_path.unlink()

        logger.info("MedDRA version deleted")

    def is_available(self) -> bool:
        """Check if MedDRA is loaded and available."""
        if not self.db_path.exists():
            return False

        # Don't use read_only=True as it conflicts with write connections during loading
        conn = duckdb.connect(str(self.db_path))
        try:
            result = conn.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'meddra_pt'
            """).fetchone()
            return result[0] > 0
        except Exception:
            return False
        finally:
            conn.close()
