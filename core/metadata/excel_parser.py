# SAGE - Excel Specification Parser
# ==================================
# Parses SDTM/ADaM specification Excel files
"""
Excel specification parser for clinical trial metadata.

Supports common specification formats:
- CDISC SDTM specification files
- ADaM specification files
- Define-XML style specifications
- Custom sponsor specification formats
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import json
import re

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class VariableSpec:
    """Specification for a single variable."""
    name: str
    label: str
    data_type: str
    length: Optional[int] = None
    format: Optional[str] = None
    codelist: Optional[str] = None
    origin: Optional[str] = None
    role: Optional[str] = None
    core: Optional[str] = None  # Req/Exp/Perm
    description: Optional[str] = None
    derivation: Optional[str] = None
    comment: Optional[str] = None
    order: int = 0
    # AK112 format additional fields
    source: Optional[str] = None  # ADaM predecessor/source reference
    predecessor: Optional[str] = None  # Explicit predecessor variable
    assigned_value: Optional[str] = None  # Default/assigned value
    method: Optional[str] = None  # ADaM derivation method

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'label': self.label,
            'data_type': self.data_type,
            'length': self.length,
            'format': self.format,
            'codelist': self.codelist,
            'origin': self.origin,
            'role': self.role,
            'core': self.core,
            'description': self.description,
            'derivation': self.derivation,
            'comment': self.comment,
            'order': self.order,
            'source': self.source,
            'predecessor': self.predecessor,
            'assigned_value': self.assigned_value,
            'method': self.method
        }


@dataclass
class DomainSpec:
    """Specification for a domain/dataset."""
    name: str
    label: str
    structure: str = ""  # One record per subject, etc.
    purpose: str = ""
    keys: List[str] = field(default_factory=list)
    variables: List[VariableSpec] = field(default_factory=list)
    comment: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'label': self.label,
            'structure': self.structure,
            'purpose': self.purpose,
            'keys': self.keys,
            'variables': [v.to_dict() for v in self.variables],
            'comment': self.comment
        }

    def get_variable(self, name: str) -> Optional[VariableSpec]:
        """Get a variable by name."""
        name = name.upper()
        for var in self.variables:
            if var.name.upper() == name:
                return var
        return None


@dataclass
class CodelistSpec:
    """Specification for a codelist."""
    name: str
    label: str
    data_type: str = "text"
    values: List[Dict[str, str]] = field(default_factory=list)  # [{code, decode}]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'label': self.label,
            'data_type': self.data_type,
            'values': self.values
        }


@dataclass
class ParseResult:
    """Result of parsing a specification file."""
    success: bool
    filename: str
    domains: List[DomainSpec] = field(default_factory=list)
    codelists: List[CodelistSpec] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'filename': self.filename,
            'domains': [d.to_dict() for d in self.domains],
            'codelists': [c.to_dict() for c in self.codelists],
            'errors': self.errors,
            'warnings': self.warnings,
            'metadata': self.metadata
        }


class ExcelParser:
    """
    Parser for clinical trial specification Excel files.

    Automatically detects and parses:
    - Domain/dataset sheets
    - Variable specifications
    - Codelists/controlled terminology
    - Value level metadata

    Example:
        parser = ExcelParser()
        result = parser.parse_file('specs/SDTM_Spec.xlsx')

        for domain in result.domains:
            print(f"{domain.name}: {len(domain.variables)} variables")
    """

    # Common sheet name patterns - Updated for AK112 format
    DOMAIN_SHEET_PATTERNS = [
        # SDTM core domains
        r'^(DM|AE|CM|LB|VS|EX|MH|DS|SV|SC|FA|QS|IE|TI|TV|TA|TE|SE)$',
        # Additional SDTM domains from AK112
        r'^(BS|CV|DD|DV|EC|EG|GF|IS|MB|PC|PE|PP|PR|RP|RS|SS|SU|TR|TS|TU|XA)$',
        # ADaM datasets - expanded for AK112
        r'^(ADSL|ADAE|ADAESUM|ADCM|ADEXSUM|ADLB|ADLBHY|ADVS|ADEG|ADPR|ADTUMOR|ADEFF|ADTTE|ADQS|ADQSTTE|ADIS)$',
        # Generic patterns
        r'^Variables?$',
        r'^Dataset\s*Variables?$',
    ]

    # Sheets to explicitly skip
    SKIP_SHEET_PATTERNS = [
        r'^Instructions?$',
        r'^Study$',
        r'^Datasets?$',
        r'^Revision\s*History$',
        r'^Define$',
        r'^Algorithm$',
        r'^Variable\s*Mapping$',
        r'^IETEST$',
        r'^RELREC$',
        # Skip SUPP domains for now (per user decision)
        r'^SUPP[A-Z]{2}$',
    ]

    CODELIST_SHEET_PATTERNS = [
        r'^Codelist[s]?$',
        r'^Controlled\s*Term',
        r'^CT$',
        r'^Value\s*Level',
        r'^Dictionaries$',  # AK112 ADaM format
    ]

    # Common column name mappings - Updated for AK112 format
    VARIABLE_COLUMN_MAP = {
        # Variable name mappings
        'variable': 'name',
        'variable name': 'name',  # SDTM format
        'var name': 'name',
        'varname': 'name',
        'name': 'name',

        # Label mappings
        'label': 'label',
        'variable label': 'label',  # SDTM format

        # Data type mappings
        'type': 'data_type',
        'data type': 'data_type',
        'data_type': 'data_type',  # AK112 ADaM format (with underscore)
        'datatype': 'data_type',

        # Length and format
        'length': 'length',
        'format': 'format',
        'display format': 'format',

        # Codelist mappings
        'codelist': 'codelist',
        'controlled terms': 'codelist',
        'controlled terms or format': 'codelist',  # SDTM format
        'ct': 'codelist',

        # Origin mappings
        'origin': 'origin',

        # Source/predecessor mappings (AK112 ADaM - these are different from origin)
        'source': 'source',  # ADaM predecessor/source reference
        'predecessor': 'predecessor',  # Explicit predecessor variable

        # Role and core
        'role': 'role',
        'core': 'core',
        'requirement': 'core',
        'mandatory': 'core',  # AK112 ADaM format

        # Derivation mappings
        'derivation': 'derivation',
        'algorithm': 'derivation',
        'computation method': 'derivation',
        'conversion definition': 'derivation',  # SDTM format
        'method': 'method',  # AK112 ADaM - separate method field

        # Description
        'description': 'description',

        # Comment mappings
        'comment': 'comment',
        'comments': 'comment',  # SDTM format (plural)
        'notes': 'comment',
        'developer_notes': 'developer_notes',

        # Order/sequence
        'order': 'order',
        'seq': 'order',
        'sequence': 'order',
        'sequence order': 'order',  # SDTM format

        # AK112 specific fields
        'assigned_value': 'assigned_value',
        'assigned value': 'assigned_value',
    }

    def __init__(self):
        """Initialize the Excel parser."""
        self._sheet_cache: Dict[str, pd.DataFrame] = {}

    def parse_file(self, filepath: str) -> ParseResult:
        """
        Parse an Excel specification file.

        Args:
            filepath: Path to the Excel file

        Returns:
            ParseResult with domains, codelists, and any errors
        """
        filepath = Path(filepath)

        if not filepath.exists():
            return ParseResult(
                success=False,
                filename=filepath.name,
                errors=[f"File not found: {filepath}"]
            )

        if filepath.suffix.lower() not in ['.xlsx', '.xls', '.xlsm']:
            return ParseResult(
                success=False,
                filename=filepath.name,
                errors=[f"Invalid file type: {filepath.suffix}"]
            )

        errors = []
        warnings = []
        domains = []
        codelists = []

        try:
            # Read all sheets
            excel_file = pd.ExcelFile(filepath)
            sheet_names = excel_file.sheet_names

            logger.info(f"Parsing {filepath.name}: {len(sheet_names)} sheets")

            # Classify sheets
            domain_sheets = []
            codelist_sheets = []
            other_sheets = []

            for sheet in sheet_names:
                if self._is_domain_sheet(sheet):
                    domain_sheets.append(sheet)
                elif self._is_codelist_sheet(sheet):
                    codelist_sheets.append(sheet)
                else:
                    other_sheets.append(sheet)

            # Parse domain sheets
            for sheet_name in domain_sheets:
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    domain = self._parse_domain_sheet(sheet_name, df)
                    if domain:
                        domains.append(domain)
                        logger.info(f"  Parsed domain {domain.name}: {len(domain.variables)} variables")
                except Exception as e:
                    warnings.append(f"Error parsing sheet '{sheet_name}': {e}")

            # Parse codelist sheets
            for sheet_name in codelist_sheets:
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    parsed_codelists = self._parse_codelist_sheet(sheet_name, df)
                    codelists.extend(parsed_codelists)
                    logger.info(f"  Parsed {len(parsed_codelists)} codelists from {sheet_name}")
                except Exception as e:
                    warnings.append(f"Error parsing codelist sheet '{sheet_name}': {e}")

            # If no domain sheets found, try to parse as single variable list
            if not domains and other_sheets:
                for sheet_name in other_sheets:
                    try:
                        df = pd.read_excel(excel_file, sheet_name=sheet_name)
                        domain = self._parse_variable_list_sheet(sheet_name, df)
                        if domain and domain.variables:
                            domains.append(domain)
                    except Exception:
                        pass

            # Build metadata
            metadata = {
                'source_file': filepath.name,
                'parsed_at': datetime.now().isoformat(),
                'total_sheets': len(sheet_names),
                'domain_sheets': domain_sheets,
                'codelist_sheets': codelist_sheets,
                'total_domains': len(domains),
                'total_variables': sum(len(d.variables) for d in domains),
                'total_codelists': len(codelists)
            }

            success = len(domains) > 0 or len(codelists) > 0

            if not success:
                errors.append("No domains or codelists found in file")

            return ParseResult(
                success=success,
                filename=filepath.name,
                domains=domains,
                codelists=codelists,
                errors=errors,
                warnings=warnings,
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Failed to parse {filepath}: {e}")
            return ParseResult(
                success=False,
                filename=filepath.name,
                errors=[f"Parse error: {e}"]
            )

    def _should_skip_sheet(self, sheet_name: str) -> bool:
        """Check if a sheet should be skipped."""
        for pattern in self.SKIP_SHEET_PATTERNS:
            if re.match(pattern, sheet_name, re.IGNORECASE):
                return True
        return False

    def _is_domain_sheet(self, sheet_name: str) -> bool:
        """Check if a sheet name matches domain patterns."""
        # First check if it should be skipped
        if self._should_skip_sheet(sheet_name):
            return False

        for pattern in self.DOMAIN_SHEET_PATTERNS:
            if re.match(pattern, sheet_name, re.IGNORECASE):
                return True
        return False

    def _is_codelist_sheet(self, sheet_name: str) -> bool:
        """Check if a sheet name matches codelist patterns."""
        for pattern in self.CODELIST_SHEET_PATTERNS:
            if re.match(pattern, sheet_name, re.IGNORECASE):
                return True
        return False

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names to standard names."""
        rename_map = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if col_lower in self.VARIABLE_COLUMN_MAP:
                rename_map[col] = self.VARIABLE_COLUMN_MAP[col_lower]
        return df.rename(columns=rename_map)

    def _get_str_field(self, row: pd.Series, field: str) -> Optional[str]:
        """Safely extract a string field from a row."""
        val = row.get(field)
        if pd.isna(val):
            return None
        val_str = str(val).strip()
        return val_str if val_str and val_str.lower() != 'nan' else None

    def _parse_domain_sheet(self, sheet_name: str, df: pd.DataFrame) -> Optional[DomainSpec]:
        """Parse a domain/dataset sheet."""
        if df.empty:
            return None

        # Normalize column names
        df = self._normalize_columns(df)

        # Check if this looks like a variable specification
        if 'name' not in df.columns:
            return None

        variables = []
        for idx, row in df.iterrows():
            var_name = self._get_str_field(row, 'name')
            if not var_name:
                continue

            # Build derivation from multiple sources
            # Priority: method > derivation > source (for ADaM predecessor logic)
            derivation = self._get_str_field(row, 'method')
            if not derivation:
                derivation = self._get_str_field(row, 'derivation')

            # For ADaM, source often contains predecessor derivation info
            source = self._get_str_field(row, 'source')
            if source and not derivation and 'derived' in str(row.get('origin', '')).lower():
                # If origin is "Derived" and source has info, use source as derivation hint
                derivation = source

            var = VariableSpec(
                name=var_name.upper(),
                label=self._get_str_field(row, 'label') or '',
                data_type=self._normalize_datatype(row.get('data_type', 'Char')),
                length=self._parse_length(row.get('length')),
                format=self._get_str_field(row, 'format'),
                codelist=self._get_str_field(row, 'codelist'),
                origin=self._get_str_field(row, 'origin'),
                role=self._get_str_field(row, 'role'),
                core=self._normalize_core(row.get('core')),
                description=self._get_str_field(row, 'description'),
                derivation=derivation,
                comment=self._get_str_field(row, 'comment'),
                order=idx + 1,
                # AK112 additional fields
                source=source,
                predecessor=self._get_str_field(row, 'predecessor'),
                assigned_value=self._get_str_field(row, 'assigned_value'),
                method=self._get_str_field(row, 'method'),
            )
            variables.append(var)

        if not variables:
            return None

        # Determine domain name
        domain_name = sheet_name.upper()
        if domain_name in ['VARIABLES', 'DATASET VARIABLES']:
            # Try to infer from data
            if variables and variables[0].name.startswith(('DM', 'AE', 'CM', 'LB', 'VS')):
                domain_name = variables[0].name[:2]

        return DomainSpec(
            name=domain_name,
            label=f"{domain_name} Domain",
            variables=variables
        )

    def _parse_variable_list_sheet(self, sheet_name: str, df: pd.DataFrame) -> Optional[DomainSpec]:
        """Parse a generic variable list sheet."""
        return self._parse_domain_sheet(sheet_name, df)

    def _parse_codelist_sheet(self, sheet_name: str, df: pd.DataFrame) -> List[CodelistSpec]:
        """Parse a codelist/controlled terminology sheet."""
        codelists = []

        if df.empty:
            return codelists

        # Normalize column names
        df.columns = [str(c).lower().strip().replace('_', ' ') for c in df.columns]

        # Find the relevant columns
        # AK112 format: ID, Name, Term, Decoded_Value
        # Standard format: codelist, code/value, decode/description
        codelist_col = None
        codelist_name_col = None
        code_col = None
        decode_col = None
        data_type_col = None

        for col in df.columns:
            col_clean = col.replace(' ', '')
            if col == 'id' or col_clean == 'id':
                codelist_col = col
            elif col == 'name' and not codelist_name_col:
                codelist_name_col = col
            elif 'codelist' in col:
                if not codelist_col:
                    codelist_col = col
            elif col in ['term', 'code', 'coded value', 'value']:
                code_col = col
            elif col in ['decoded value', 'decode', 'description', 'meaning']:
                decode_col = col
            elif col in ['data type', 'datatype']:
                data_type_col = col

        # Fallback: if no ID column but has Name column, use Name as codelist identifier
        if not codelist_col and codelist_name_col:
            codelist_col = codelist_name_col

        if not codelist_col or not code_col:
            logger.warning(f"Could not find codelist columns in sheet {sheet_name}. "
                          f"Found columns: {list(df.columns)}")
            return codelists

        # Group by codelist - handle AK112 format where ID might be sparse
        # (only first row of each codelist has the ID)
        codelists_dict: Dict[str, Dict] = {}
        current_codelist = None

        for _, row in df.iterrows():
            # Get codelist identifier
            cl_id = str(row.get(codelist_col, '')).strip()
            if cl_id and cl_id.lower() != 'nan':
                current_codelist = cl_id

            if not current_codelist:
                continue

            # Get codelist name (label) - might be in a separate column
            cl_name = current_codelist
            if codelist_name_col and codelist_name_col != codelist_col:
                name_val = str(row.get(codelist_name_col, '')).strip()
                if name_val and name_val.lower() != 'nan':
                    cl_name = name_val

            # Get code and decode values
            code = str(row.get(code_col, '')).strip()
            if not code or code.lower() == 'nan':
                continue

            decode = code  # Default decode to code
            if decode_col:
                decode_val = str(row.get(decode_col, '')).strip()
                if decode_val and decode_val.lower() != 'nan':
                    decode = decode_val

            # Get data type if available
            data_type = 'text'
            if data_type_col:
                dt_val = str(row.get(data_type_col, '')).strip().lower()
                if dt_val in ['integer', 'int', 'numeric', 'num']:
                    data_type = 'integer'
                elif dt_val and dt_val != 'nan':
                    data_type = dt_val

            # Add to codelist dict
            if current_codelist not in codelists_dict:
                codelists_dict[current_codelist] = {
                    'name': current_codelist,
                    'label': cl_name,
                    'data_type': data_type,
                    'values': []
                }

            codelists_dict[current_codelist]['values'].append({
                'code': code,
                'decode': decode
            })

        # Convert dict to list of CodelistSpec
        for cl_id, cl_data in codelists_dict.items():
            codelists.append(CodelistSpec(
                name=cl_data['name'],
                label=cl_data['label'],
                data_type=cl_data['data_type'],
                values=cl_data['values']
            ))

        return codelists

    def _normalize_datatype(self, value: Any) -> str:
        """Normalize data type to standard values."""
        if pd.isna(value):
            return 'Char'

        val = str(value).lower().strip()

        if val in ['num', 'numeric', 'number', 'float', 'integer', 'int']:
            return 'Num'
        elif val in ['char', 'character', 'text', 'string', 'varchar']:
            return 'Char'
        elif val in ['date', 'datetime', 'time']:
            return 'Char'  # Dates stored as ISO strings in CDISC

        return 'Char'

    def _normalize_core(self, value: Any) -> Optional[str]:
        """Normalize core/requirement to standard values."""
        if pd.isna(value):
            return None

        val = str(value).lower().strip()

        if val in ['req', 'required', 'r']:
            return 'Req'
        elif val in ['exp', 'expected', 'e']:
            return 'Exp'
        elif val in ['perm', 'permissible', 'p']:
            return 'Perm'

        return str(value).strip()

    def _parse_length(self, value: Any) -> Optional[int]:
        """Parse length value."""
        if pd.isna(value):
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def get_sheet_names(self, filepath: str) -> List[str]:
        """Get list of sheet names in an Excel file."""
        try:
            return pd.ExcelFile(filepath).sheet_names
        except Exception:
            return []

    def preview_sheet(self, filepath: str, sheet_name: str, rows: int = 10) -> pd.DataFrame:
        """Preview a specific sheet."""
        try:
            return pd.read_excel(filepath, sheet_name=sheet_name, nrows=rows)
        except Exception:
            return pd.DataFrame()
