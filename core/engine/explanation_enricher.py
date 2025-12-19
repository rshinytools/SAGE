# SAGE - Explanation Enricher
# ============================
"""
Metadata-Based Column Explanation
================================
Enriches query results with detailed explanations based on Golden Metadata.

This module provides:
1. Column explanations from Golden Metadata (label, description, derivation)
2. Population context (what population was queried)
3. Assumptions made during query processing
4. Term resolution details

Output appears in the Details tab with "Medium" detail level:
- Column definitions from metadata
- Population explanation
- Assumptions made

Example:
    enricher = ExplanationEnricher(metadata_path)
    explanation = enricher.explain(
        columns_used=['USUBJID', 'AEDECOD', 'AETOXGR'],
        table_used='ADAE',
        population='Safety',
        population_filter="SAFFL='Y'",
        entities_resolved=[{'original': 'nausea', 'resolved': 'Nausea'}],
        assumptions=['Counted unique subjects', 'Used Safety population']
    )
"""

import json
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ColumnExplanation:
    """Explanation for a single column."""
    name: str
    label: str
    domain: str
    description: Optional[str] = None
    derivation: Optional[str] = None
    data_type: Optional[str] = None
    codelist: Optional[str] = None
    core: Optional[str] = None  # Req, Exp, Model Permissible


@dataclass
class QueryExplanation:
    """Complete explanation for a query result."""
    # Column explanations
    columns: List[ColumnExplanation]

    # Population context
    population_name: str
    population_filter: Optional[str]
    population_description: str

    # Term resolutions
    term_resolutions: List[Dict[str, str]]

    # Assumptions
    assumptions: List[str]

    # Table info
    table_used: str
    table_label: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            'columns': [
                {
                    'name': c.name,
                    'label': c.label,
                    'domain': c.domain,
                    'description': c.description,
                    'derivation': c.derivation,
                    'data_type': c.data_type,
                    'codelist': c.codelist,
                    'core': c.core
                }
                for c in self.columns
            ],
            'population': {
                'name': self.population_name,
                'filter': self.population_filter,
                'description': self.population_description
            },
            'term_resolutions': self.term_resolutions,
            'assumptions': self.assumptions,
            'table': {
                'name': self.table_used,
                'label': self.table_label
            }
        }

    def to_markdown(self) -> str:
        """Generate markdown explanation for Details tab."""
        lines = []

        # Table header
        lines.append(f"### Data Source: {self.table_used}")
        if self.table_label:
            lines.append(f"*{self.table_label}*")
        lines.append("")

        # Population
        lines.append("### Population")
        lines.append(f"**{self.population_name}**")
        if self.population_description:
            lines.append(f"- {self.population_description}")
        if self.population_filter:
            lines.append(f"- Filter applied: `{self.population_filter}`")
        lines.append("")

        # Columns used
        if self.columns:
            lines.append("### Columns Used")
            lines.append("")
            for col in self.columns:
                lines.append(f"**{col.name}** ({col.domain})")
                lines.append(f"- Label: {col.label}")
                if col.description and col.description != "_null_":
                    lines.append(f"- Description: {col.description}")
                if col.derivation and col.derivation != "_null_":
                    lines.append(f"- Derivation: {col.derivation}")
                if col.codelist:
                    lines.append(f"- Codelist: {col.codelist}")
                lines.append("")

        # Term resolutions
        if self.term_resolutions:
            lines.append("### Term Resolution")
            for res in self.term_resolutions:
                original = res.get('original', '')
                resolved = res.get('resolved', '')
                match_type = res.get('match_type', '')
                confidence = res.get('confidence', '')
                lines.append(f"- \"{original}\" → **{resolved}** ({match_type}, {confidence}% confidence)")
            lines.append("")

        # Assumptions
        if self.assumptions:
            lines.append("### Assumptions Made")
            for assumption in self.assumptions:
                lines.append(f"- {assumption}")
            lines.append("")

        return "\n".join(lines)


class ExplanationEnricher:
    """
    Enriches query results with metadata-based explanations.

    Uses Golden Metadata to provide detailed column explanations,
    population context, and assumption documentation.
    """

    # Standard population descriptions
    POPULATION_DESCRIPTIONS = {
        'safety': 'All subjects who received at least one dose of study treatment',
        'itt': 'All randomized subjects (Intent-to-Treat)',
        'intent-to-treat': 'All randomized subjects (Intent-to-Treat)',
        'fas': 'Full Analysis Set - all randomized subjects with baseline and at least one post-baseline assessment',
        'per-protocol': 'All subjects who completed the study without major protocol deviations',
        'pp': 'All subjects who completed the study without major protocol deviations',
        'all': 'All subjects in the dataset',
        'enrolled': 'All subjects enrolled in the study'
    }

    # Standard population filters
    POPULATION_FILTERS = {
        'safety': "SAFFL='Y'",
        'itt': "ITTFL='Y'",
        'intent-to-treat': "ITTFL='Y'",
        'fas': "FASFL='Y'",
        'per-protocol': "PPROTFL='Y'",
        'pp': "PPROTFL='Y'",
        'all': None,
        'enrolled': "ENRLFL='Y'"
    }

    # Table labels
    TABLE_LABELS = {
        'ADAE': 'Analysis Dataset - Adverse Events',
        'ADSL': 'Analysis Dataset - Subject Level',
        'ADLB': 'Analysis Dataset - Laboratory',
        'ADVS': 'Analysis Dataset - Vital Signs',
        'ADCM': 'Analysis Dataset - Concomitant Medications',
        'ADEX': 'Analysis Dataset - Exposure',
        'AE': 'Adverse Events Domain',
        'DM': 'Demographics Domain',
        'LB': 'Laboratory Domain',
        'VS': 'Vital Signs Domain',
        'CM': 'Concomitant Medications Domain',
        'EX': 'Exposure Domain'
    }

    def __init__(self, metadata_path: str = None):
        """
        Initialize explanation enricher.

        Args:
            metadata_path: Path to golden_metadata.json
        """
        self.metadata_path = metadata_path
        self._metadata: Optional[Dict] = None
        self._variable_index: Dict[str, Dict] = {}  # {DOMAIN.VARNAME: variable_info}

        if metadata_path:
            self._load_metadata()

    def _load_metadata(self):
        """Load and index golden metadata."""
        if not self.metadata_path:
            return

        try:
            path = Path(self.metadata_path)
            if path.exists():
                with open(path, 'r', encoding='utf-8') as f:
                    self._metadata = json.load(f)
                self._index_variables()
                logger.info(f"Loaded metadata with {len(self._variable_index)} variables")
            else:
                logger.warning(f"Metadata file not found: {self.metadata_path}")
        except Exception as e:
            logger.error(f"Failed to load metadata: {e}")

    def _index_variables(self):
        """Build index of variables for quick lookup."""
        if not self._metadata:
            return

        for domain in self._metadata.get('domains', []):
            domain_name = domain.get('name', '')
            for var in domain.get('variables', []):
                var_name = var.get('name', '')
                key = f"{domain_name}.{var_name}"
                self._variable_index[key] = var
                # Also index by variable name alone for convenience
                if var_name not in self._variable_index:
                    self._variable_index[var_name] = var

    def explain(self,
                columns_used: List[str],
                table_used: str,
                population: str = 'Safety',
                population_filter: str = None,
                entities_resolved: List[Dict] = None,
                assumptions: List[str] = None) -> QueryExplanation:
        """
        Generate explanation for a query result.

        Args:
            columns_used: List of column names used in query
            table_used: Table name (e.g., 'ADAE')
            population: Population name (e.g., 'Safety', 'ITT')
            population_filter: SQL filter applied (e.g., "SAFFL='Y'")
            entities_resolved: List of term resolution dicts
            assumptions: List of assumptions made

        Returns:
            QueryExplanation with full details
        """
        # Get column explanations
        column_explanations = []
        for col in columns_used:
            col_upper = col.upper()
            explanation = self._get_column_explanation(col_upper, table_used)
            if explanation:
                column_explanations.append(explanation)

        # Get population details
        pop_lower = population.lower() if population else 'safety'
        pop_description = self.POPULATION_DESCRIPTIONS.get(
            pop_lower,
            f'{population} population'
        )

        # Use provided filter or look up default
        if not population_filter:
            population_filter = self.POPULATION_FILTERS.get(pop_lower)

        # Get table label
        table_label = self.TABLE_LABELS.get(
            table_used.upper(),
            f'{table_used} Dataset'
        )

        # Format term resolutions
        term_resolutions = []
        if entities_resolved:
            for entity in entities_resolved:
                term_resolutions.append({
                    'original': entity.get('original', ''),
                    'resolved': entity.get('resolved', ''),
                    'match_type': entity.get('match_type', 'exact'),
                    'confidence': entity.get('confidence', 100)
                })

        return QueryExplanation(
            columns=column_explanations,
            population_name=population,
            population_filter=population_filter,
            population_description=pop_description,
            term_resolutions=term_resolutions,
            assumptions=assumptions or [],
            table_used=table_used,
            table_label=table_label
        )

    def _get_column_explanation(self, column: str, table: str) -> Optional[ColumnExplanation]:
        """Get explanation for a single column."""
        # Try domain.column lookup first
        key = f"{table}.{column}"
        var_info = self._variable_index.get(key)

        # Fall back to column name only
        if not var_info:
            var_info = self._variable_index.get(column)

        if not var_info:
            # Return basic info even without metadata
            return ColumnExplanation(
                name=column,
                label=self._generate_label(column),
                domain=table or 'Unknown',
                description=None,
                derivation=None
            )

        return ColumnExplanation(
            name=column,
            label=var_info.get('label', column),
            domain=var_info.get('domain', table or 'Unknown'),
            description=var_info.get('description'),
            derivation=var_info.get('derivation'),
            data_type=var_info.get('data_type'),
            codelist=var_info.get('codelist'),
            core=var_info.get('core')
        )

    def _generate_label(self, column: str) -> str:
        """Generate human-readable label from column name."""
        # Common CDISC column name patterns
        labels = {
            'USUBJID': 'Unique Subject Identifier',
            'SUBJID': 'Subject Identifier',
            'STUDYID': 'Study Identifier',
            'AEDECOD': 'Adverse Event Dictionary-Derived Term',
            'AEBODSYS': 'Body System or Organ Class',
            'AETOXGR': 'Adverse Event Toxicity Grade',
            'AESEV': 'Adverse Event Severity',
            'AESER': 'Serious Adverse Event',
            'AEREL': 'Adverse Event Relationship to Treatment',
            'AESTDTC': 'Adverse Event Start Date/Time',
            'AEENDTC': 'Adverse Event End Date/Time',
            'SAFFL': 'Safety Population Flag',
            'ITTFL': 'Intent-to-Treat Population Flag',
            'TRTA': 'Actual Treatment',
            'TRTP': 'Planned Treatment',
            'AGE': 'Age',
            'SEX': 'Sex',
            'RACE': 'Race',
            'ETHNIC': 'Ethnicity',
            'LBTEST': 'Lab Test Name',
            'LBSTRESN': 'Lab Result Numeric',
            'LBSTRESU': 'Lab Result Unit',
            'VSTEST': 'Vital Sign Test Name',
            'VSSTRESN': 'Vital Sign Result Numeric'
        }

        if column in labels:
            return labels[column]

        # Generate from column name
        # Convert AETOXGR to "AE Toxicity Grade" pattern
        return column

    def get_variable_info(self, column: str, domain: str = None) -> Optional[Dict]:
        """
        Get full variable info from metadata.

        Args:
            column: Column name
            domain: Optional domain name for qualified lookup

        Returns:
            Variable info dict or None
        """
        if domain:
            return self._variable_index.get(f"{domain}.{column}")
        return self._variable_index.get(column)

    def list_domain_variables(self, domain: str) -> List[str]:
        """List all variables in a domain."""
        variables = []
        prefix = f"{domain}."
        for key in self._variable_index:
            if key.startswith(prefix):
                variables.append(key.split('.')[1])
        return sorted(set(variables))


def create_explanation_enricher(metadata_path: str = None) -> ExplanationEnricher:
    """
    Factory function to create ExplanationEnricher.

    Args:
        metadata_path: Path to golden_metadata.json

    Returns:
        Configured ExplanationEnricher
    """
    return ExplanationEnricher(metadata_path=metadata_path)
