"""
CDISC Standards Library

Manages the CDISC standards database for SDTM IG and ADaM IG.
Used for auto-approval of standard variables during metadata import.
"""

import sqlite3
import json
import logging
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class CDISCDomain:
    """CDISC standard domain/dataset definition."""
    standard: str  # 'SDTM' or 'ADaM'
    version: str  # '3.4', '1.3'
    name: str  # 'DM', 'ADSL'
    label: str  # 'Demographics'
    domain_class: str  # 'General Observation', 'Subject Level'
    structure: Optional[str] = None  # 'One record per subject'

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CDISCVariable:
    """CDISC standard variable definition."""
    standard: str
    version: str
    domain: str
    name: str  # 'STUDYID', 'USUBJID'
    label: str  # 'Study Identifier'
    data_type: Optional[str] = None  # 'Char', 'Num'
    core: Optional[str] = None  # 'Req', 'Exp', 'Perm'
    role: Optional[str] = None  # 'Identifier', 'Topic', 'Timing'
    codelist: Optional[str] = None  # Codelist name
    codelist_code: Optional[str] = None  # NCI code
    description: Optional[str] = None  # CDISC notes
    order: Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MatchResult:
    """Result of matching a variable against CDISC standards."""
    matched: bool
    confidence: int  # 0-100
    match_type: str  # 'exact', 'label_fuzzy', 'pattern', 'none'
    standard: Optional[str] = None
    version: Optional[str] = None
    standard_variable: Optional[CDISCVariable] = None
    reason: str = ""

    def to_dict(self) -> dict:
        result = {
            'matched': self.matched,
            'confidence': self.confidence,
            'match_type': self.match_type,
            'standard': self.standard,
            'version': self.version,
            'reason': self.reason,
        }
        if self.standard_variable:
            result['standard_variable'] = self.standard_variable.to_dict()
        return result


class CDISCLibrary:
    """
    CDISC Standards Library for auto-approval of metadata.

    Stores SDTM IG and ADaM IG standards in SQLite database.
    Provides matching functions for auto-approval engine.
    """

    # Standard variable patterns that indicate CDISC compliance
    STANDARD_SUFFIXES = {
        'SEQ': ('Sequence Number', 'Num'),
        'DTC': ('Date/Time', 'Char'),
        'DY': ('Study Day', 'Num'),
        'STRF': ('Start Relative to Reference', 'Char'),
        'ENRF': ('End Relative to Reference', 'Char'),
        'CAT': ('Category', 'Char'),
        'SCAT': ('Subcategory', 'Char'),
        'STAT': ('Completion Status', 'Char'),
        'REASND': ('Reason Not Done', 'Char'),
        'LOC': ('Location', 'Char'),
        'LAT': ('Laterality', 'Char'),
        'DIR': ('Directionality', 'Char'),
        'PORTOT': ('Portion or Totality', 'Char'),
    }

    # Universal identifier variables present in all domains
    UNIVERSAL_IDENTIFIERS = {
        'STUDYID', 'DOMAIN', 'USUBJID', 'SUBJID',
    }

    def __init__(self, db_path: str):
        """Initialize with path to SQLite database."""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()

    def _init_database(self):
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Domains/Datasets table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS domains (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                standard TEXT NOT NULL,
                version TEXT NOT NULL,
                name TEXT NOT NULL,
                label TEXT,
                domain_class TEXT,
                structure TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(standard, version, name)
            )
        ''')

        # Variables table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS variables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                standard TEXT NOT NULL,
                version TEXT NOT NULL,
                domain TEXT NOT NULL,
                name TEXT NOT NULL,
                label TEXT,
                data_type TEXT,
                core TEXT,
                role TEXT,
                codelist TEXT,
                codelist_code TEXT,
                description TEXT,
                var_order INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(standard, version, domain, name)
            )
        ''')

        # Codelists table (for future CDISC CT integration)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS codelists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                nci_code TEXT,
                label TEXT,
                extensible TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(name)
            )
        ''')

        # Codelist terms table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS codelist_terms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codelist_name TEXT NOT NULL,
                code TEXT NOT NULL,
                decode TEXT,
                nci_code TEXT,
                UNIQUE(codelist_name, code)
            )
        ''')

        # Import history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS import_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                standard TEXT NOT NULL,
                version TEXT NOT NULL,
                source_file TEXT,
                domains_imported INTEGER,
                variables_imported INTEGER,
                imported_at TEXT DEFAULT CURRENT_TIMESTAMP,
                imported_by TEXT
            )
        ''')

        # Create indexes for fast lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_var_name ON variables(name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_var_domain ON variables(domain)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_var_standard ON variables(standard)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_var_label ON variables(label)')

        conn.commit()
        conn.close()

    def import_sdtm_ig(self, excel_path: str, user: str = "system") -> Dict[str, int]:
        """
        Import SDTM Implementation Guide from Excel.

        Args:
            excel_path: Path to SDTMIG Excel file
            user: Username for audit trail

        Returns:
            Dict with import statistics
        """
        logger.info(f"Importing SDTM IG from {excel_path}")

        xl = pd.ExcelFile(excel_path)

        # Import datasets
        datasets_df = pd.read_excel(xl, sheet_name='Datasets')
        domains_imported = 0

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for _, row in datasets_df.iterrows():
            version = row.get('Version', 'SDTMIG v3.4')
            version_short = version.replace('SDTMIG ', '') if 'SDTMIG' in str(version) else version

            cursor.execute('''
                INSERT OR REPLACE INTO domains
                (standard, version, name, label, domain_class, structure)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                'SDTM',
                version_short,
                row.get('Dataset Name', ''),
                row.get('Dataset Label', ''),
                row.get('Class', ''),
                row.get('Structure', '')
            ))
            domains_imported += 1

        # Import variables
        variables_df = pd.read_excel(xl, sheet_name='Variables')
        variables_imported = 0

        for _, row in variables_df.iterrows():
            version = row.get('Version', 'SDTMIG v3.4')
            version_short = version.replace('SDTMIG ', '') if 'SDTMIG' in str(version) else version

            cursor.execute('''
                INSERT OR REPLACE INTO variables
                (standard, version, domain, name, label, data_type, core, role,
                 codelist, codelist_code, description, var_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                'SDTM',
                version_short,
                row.get('Dataset Name', ''),
                row.get('Variable Name', ''),
                row.get('Variable Label', ''),
                row.get('Type', ''),
                row.get('Core', ''),
                row.get('Role', ''),
                row.get('Codelist Submission Value(s)', ''),
                row.get('CDISC CT Codelist Code(s)', ''),
                row.get('CDISC Notes', ''),
                row.get('Variable Order', 0)
            ))
            variables_imported += 1

        # Record import history
        cursor.execute('''
            INSERT INTO import_history
            (standard, version, source_file, domains_imported, variables_imported, imported_by)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ('SDTM', version_short, str(excel_path), domains_imported, variables_imported, user))

        conn.commit()
        conn.close()

        logger.info(f"SDTM IG import complete: {domains_imported} domains, {variables_imported} variables")
        return {
            'standard': 'SDTM',
            'version': version_short,
            'domains_imported': domains_imported,
            'variables_imported': variables_imported
        }

    def import_adam_ig(self, excel_path: str, user: str = "system") -> Dict[str, int]:
        """
        Import ADaM Implementation Guide from Excel.

        Args:
            excel_path: Path to ADaMIG Excel file
            user: Username for audit trail

        Returns:
            Dict with import statistics
        """
        logger.info(f"Importing ADaM IG from {excel_path}")

        xl = pd.ExcelFile(excel_path)
        variables_df = pd.read_excel(xl, sheet_name='Variables')

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Extract unique data structures as domains
        domains = variables_df['Data Structure Name'].unique()
        version = 'v1.3'

        # ADaM data structure classes
        adam_classes = {
            'adsl': 'Subject Level Analysis',
            'adlb': 'Basic Data Structure (BDS)',
            'adae': 'Occurrence Data Structure (OCCDS)',
        }

        domains_imported = 0
        for domain in domains:
            domain_upper = domain.upper()
            cursor.execute('''
                INSERT OR REPLACE INTO domains
                (standard, version, name, label, domain_class, structure)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                'ADaM',
                version,
                domain_upper,
                f'{domain_upper} Analysis Dataset',
                adam_classes.get(domain.lower(), 'Analysis Dataset'),
                'One record per subject' if domain.lower() == 'adsl' else 'Multiple records per subject'
            ))
            domains_imported += 1

        # Import variables
        variables_imported = 0
        for _, row in variables_df.iterrows():
            domain = str(row.get('Data Structure Name', '')).upper()
            var_name = row.get('Variable Name', '')

            cursor.execute('''
                INSERT OR REPLACE INTO variables
                (standard, version, domain, name, label, data_type, core, role,
                 codelist, codelist_code, description, var_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                'ADaM',
                version,
                domain,
                var_name,
                row.get('Variable Label', ''),
                None,  # ADaM IG doesn't specify type in this export
                None,  # Core not specified
                None,  # Role not specified
                None,  # Codelist not specified
                None,  # Codelist code not specified
                None,  # Description not specified
                variables_imported + 1
            ))
            variables_imported += 1

        # Record import history
        cursor.execute('''
            INSERT INTO import_history
            (standard, version, source_file, domains_imported, variables_imported, imported_by)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ('ADaM', version, str(excel_path), domains_imported, variables_imported, user))

        conn.commit()
        conn.close()

        logger.info(f"ADaM IG import complete: {domains_imported} domains, {variables_imported} variables")
        return {
            'standard': 'ADaM',
            'version': version,
            'domains_imported': domains_imported,
            'variables_imported': variables_imported
        }

    def match_variable(
        self,
        domain: str,
        name: str,
        label: str = "",
        data_type: str = ""
    ) -> MatchResult:
        """
        Match a variable against CDISC standards.

        Returns MatchResult with confidence score and match details.

        Matching tiers:
        1. Exact name + domain match (confidence: 95-100)
        2. Exact name match in any domain (confidence: 85-94)
        3. Label fuzzy match (confidence: 70-84)
        4. Pattern match (suffixes) (confidence: 60-79)
        5. No match (confidence: 0)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Normalize inputs
        domain_upper = domain.upper()
        name_upper = name.upper()

        # Tier 1: Exact domain + variable name match
        cursor.execute('''
            SELECT * FROM variables
            WHERE UPPER(domain) = ? AND UPPER(name) = ?
            ORDER BY standard DESC
        ''', (domain_upper, name_upper))

        row = cursor.fetchone()
        if row:
            var = self._row_to_variable(row)

            # Check label similarity for confidence adjustment
            label_match = self._label_similarity(label, var.label) if label and var.label else 0.5
            confidence = int(95 + (5 * label_match))

            conn.close()
            return MatchResult(
                matched=True,
                confidence=confidence,
                match_type='exact_domain',
                standard=var.standard,
                version=var.version,
                standard_variable=var,
                reason=f"Exact match in {var.standard} {var.version} - {var.domain}.{var.name}"
            )

        # Tier 2: Exact variable name in any domain
        cursor.execute('''
            SELECT * FROM variables
            WHERE UPPER(name) = ?
            ORDER BY standard DESC, domain
        ''', (name_upper,))

        row = cursor.fetchone()
        if row:
            var = self._row_to_variable(row)
            confidence = 88 if var.standard == 'ADaM' else 85

            conn.close()
            return MatchResult(
                matched=True,
                confidence=confidence,
                match_type='exact_name',
                standard=var.standard,
                version=var.version,
                standard_variable=var,
                reason=f"Name match in {var.standard} {var.version} (defined in {var.domain})"
            )

        # Tier 3: Check for standard suffix patterns
        for suffix, (suffix_label, suffix_type) in self.STANDARD_SUFFIXES.items():
            if name_upper.endswith(suffix):
                # Check if base variable exists
                base_name = name_upper[:-len(suffix)]
                cursor.execute('''
                    SELECT * FROM variables
                    WHERE UPPER(domain) = ? AND UPPER(name) LIKE ?
                    LIMIT 1
                ''', (domain_upper, f'{base_name}%'))

                row = cursor.fetchone()
                if row:
                    var = self._row_to_variable(row)
                    conn.close()
                    return MatchResult(
                        matched=True,
                        confidence=75,
                        match_type='pattern_suffix',
                        standard=var.standard,
                        version=var.version,
                        reason=f"Standard CDISC suffix pattern: --{suffix} ({suffix_label})"
                    )

        # Tier 4: Universal identifiers
        if name_upper in self.UNIVERSAL_IDENTIFIERS:
            cursor.execute('''
                SELECT * FROM variables
                WHERE UPPER(name) = ?
                LIMIT 1
            ''', (name_upper,))

            row = cursor.fetchone()
            if row:
                var = self._row_to_variable(row)
                conn.close()
                return MatchResult(
                    matched=True,
                    confidence=98,
                    match_type='universal_identifier',
                    standard=var.standard,
                    version=var.version,
                    standard_variable=var,
                    reason=f"Universal CDISC identifier variable"
                )

        # Tier 5: Label similarity search
        if label:
            cursor.execute('''
                SELECT * FROM variables
                WHERE UPPER(domain) = ?
            ''', (domain_upper,))

            rows = cursor.fetchall()
            best_match = None
            best_similarity = 0

            for row in rows:
                var = self._row_to_variable(row)
                if var.label:
                    similarity = self._label_similarity(label, var.label)
                    if similarity > best_similarity and similarity > 0.8:
                        best_similarity = similarity
                        best_match = var

            if best_match:
                confidence = int(70 + (14 * best_similarity))
                conn.close()
                return MatchResult(
                    matched=True,
                    confidence=confidence,
                    match_type='label_fuzzy',
                    standard=best_match.standard,
                    version=best_match.version,
                    standard_variable=best_match,
                    reason=f"Label similarity match ({int(best_similarity*100)}%): '{label}' ≈ '{best_match.label}'"
                )

        conn.close()
        return MatchResult(
            matched=False,
            confidence=0,
            match_type='none',
            reason="No CDISC standard match found"
        )

    def _row_to_variable(self, row: tuple) -> CDISCVariable:
        """Convert database row to CDISCVariable."""
        return CDISCVariable(
            standard=row[1],
            version=row[2],
            domain=row[3],
            name=row[4],
            label=row[5],
            data_type=row[6],
            core=row[7],
            role=row[8],
            codelist=row[9],
            codelist_code=row[10],
            description=row[11],
            order=row[12]
        )

    def _label_similarity(self, label1: str, label2: str) -> float:
        """Calculate similarity between two labels (0-1)."""
        if not label1 or not label2:
            return 0.0

        # Normalize
        l1 = label1.lower().strip()
        l2 = label2.lower().strip()

        if l1 == l2:
            return 1.0

        # Simple word overlap similarity
        words1 = set(l1.split())
        words2 = set(l2.split())

        if not words1 or not words2:
            return 0.0

        intersection = words1 & words2
        union = words1 | words2

        return len(intersection) / len(union)

    def get_all_domains(self, standard: str = None) -> List[CDISCDomain]:
        """Get all domains/datasets from the library."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if standard:
            cursor.execute('''
                SELECT standard, version, name, label, domain_class, structure
                FROM domains
                WHERE UPPER(standard) = UPPER(?)
                ORDER BY standard, name
            ''', (standard,))
        else:
            cursor.execute('''
                SELECT standard, version, name, label, domain_class, structure
                FROM domains
                ORDER BY standard, name
            ''')

        domains = []
        for row in cursor.fetchall():
            domains.append(CDISCDomain(
                standard=row[0],
                version=row[1],
                name=row[2],
                label=row[3],
                domain_class=row[4],
                structure=row[5]
            ))

        conn.close()
        return domains

    def get_domain_variables(self, domain: str, standard: str = None) -> List[CDISCVariable]:
        """Get all variables for a specific domain."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if standard:
            cursor.execute('''
                SELECT * FROM variables
                WHERE UPPER(domain) = UPPER(?) AND UPPER(standard) = UPPER(?)
                ORDER BY var_order, name
            ''', (domain, standard))
        else:
            cursor.execute('''
                SELECT * FROM variables
                WHERE UPPER(domain) = UPPER(?)
                ORDER BY standard, var_order, name
            ''', (domain,))

        variables = []
        for row in cursor.fetchall():
            variables.append(self._row_to_variable(row))

        conn.close()
        return variables

    def search_variables(self, query: str, limit: int = 50) -> List[CDISCVariable]:
        """Search variables by name or label."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM variables
            WHERE UPPER(name) LIKE UPPER(?) OR UPPER(label) LIKE UPPER(?)
            ORDER BY standard, domain, name
            LIMIT ?
        ''', (f'%{query}%', f'%{query}%', limit))

        variables = []
        for row in cursor.fetchall():
            variables.append(self._row_to_variable(row))

        conn.close()
        return variables

    def get_statistics(self) -> Dict[str, Any]:
        """Get library statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        stats = {}

        # Total counts
        cursor.execute('SELECT COUNT(*) FROM domains')
        stats['total_domains'] = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM variables')
        stats['total_variables'] = cursor.fetchone()[0]

        # By standard
        cursor.execute('''
            SELECT standard, COUNT(*) FROM domains GROUP BY standard
        ''')
        stats['domains_by_standard'] = dict(cursor.fetchall())

        cursor.execute('''
            SELECT standard, COUNT(*) FROM variables GROUP BY standard
        ''')
        stats['variables_by_standard'] = dict(cursor.fetchall())

        # Import history
        cursor.execute('''
            SELECT standard, version, domains_imported, variables_imported, imported_at
            FROM import_history
            ORDER BY imported_at DESC
        ''')
        stats['import_history'] = [
            {
                'standard': row[0],
                'version': row[1],
                'domains': row[2],
                'variables': row[3],
                'imported_at': row[4]
            }
            for row in cursor.fetchall()
        ]

        conn.close()
        return stats

    def clear_standard(self, standard: str):
        """Clear all data for a specific standard (for reimport)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('DELETE FROM domains WHERE UPPER(standard) = UPPER(?)', (standard,))
        cursor.execute('DELETE FROM variables WHERE UPPER(standard) = UPPER(?)', (standard,))

        conn.commit()
        conn.close()
        logger.info(f"Cleared {standard} data from library")


def initialize_cdisc_library(
    db_path: str,
    sdtm_path: str = None,
    adam_path: str = None,
    user: str = "system"
) -> CDISCLibrary:
    """
    Initialize CDISC library and optionally import standards.

    Args:
        db_path: Path to SQLite database
        sdtm_path: Optional path to SDTM IG Excel
        adam_path: Optional path to ADaM IG Excel
        user: Username for audit trail

    Returns:
        Initialized CDISCLibrary instance
    """
    library = CDISCLibrary(db_path)

    if sdtm_path and Path(sdtm_path).exists():
        library.import_sdtm_ig(sdtm_path, user)

    if adam_path and Path(adam_path).exists():
        library.import_adam_ig(adam_path, user)

    return library
