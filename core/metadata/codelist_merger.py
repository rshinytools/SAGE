# SAGE - Codelist Merger Module
# ==============================
# Merges codelists with variable definitions
"""
Codelist merger for enriching variable metadata with controlled terminology.

Features:
- Link variables to codelists by name
- Expand codelist values into variable metadata
- Detect missing codelist references
- Support for extensible codelists
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime

from .excel_parser import DomainSpec, VariableSpec, CodelistSpec

logger = logging.getLogger(__name__)


@dataclass
class EnrichedVariable:
    """Variable specification enriched with codelist information."""
    variable: VariableSpec
    codelist_values: List[Dict[str, str]] = field(default_factory=list)
    codelist_name: Optional[str] = None
    codelist_label: Optional[str] = None
    has_codelist: bool = False
    codelist_missing: bool = False

    def to_dict(self) -> Dict[str, Any]:
        base = self.variable.to_dict()
        base.update({
            'codelist_values': self.codelist_values,
            'codelist_name': self.codelist_name,
            'codelist_label': self.codelist_label,
            'has_codelist': self.has_codelist,
            'codelist_missing': self.codelist_missing
        })
        return base


@dataclass
class EnrichedDomain:
    """Domain specification enriched with codelist information."""
    domain: DomainSpec
    variables: List[EnrichedVariable] = field(default_factory=list)
    codelist_coverage: float = 0.0  # Percentage of variables with codelists
    missing_codelists: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.domain.name,
            'label': self.domain.label,
            'structure': self.domain.structure,
            'purpose': self.domain.purpose,
            'keys': self.domain.keys,
            'variables': [v.to_dict() for v in self.variables],
            'codelist_coverage': self.codelist_coverage,
            'missing_codelists': self.missing_codelists
        }


@dataclass
class MergeResult:
    """Result of merging codelists with domains."""
    success: bool
    domains: List[EnrichedDomain] = field(default_factory=list)
    unused_codelists: List[str] = field(default_factory=list)
    missing_codelists: List[str] = field(default_factory=list)
    statistics: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'domains': [d.to_dict() for d in self.domains],
            'unused_codelists': self.unused_codelists,
            'missing_codelists': self.missing_codelists,
            'statistics': self.statistics,
            'warnings': self.warnings
        }


class CodelistMerger:
    """
    Merges codelist definitions with variable specifications.

    Example:
        merger = CodelistMerger()

        # Load codelists
        merger.add_codelists(parsed_result.codelists)

        # Merge with domains
        result = merger.merge_domains(parsed_result.domains)

        for domain in result.domains:
            for var in domain.variables:
                if var.has_codelist:
                    print(f"{var.variable.name}: {len(var.codelist_values)} values")
    """

    def __init__(self):
        """Initialize the codelist merger."""
        self._codelists: Dict[str, CodelistSpec] = {}
        self._codelist_usage: Dict[str, int] = {}

    def add_codelist(self, codelist: CodelistSpec):
        """Add a single codelist."""
        key = codelist.name.upper()
        self._codelists[key] = codelist
        self._codelist_usage[key] = 0

    def add_codelists(self, codelists: List[CodelistSpec]):
        """Add multiple codelists."""
        for cl in codelists:
            self.add_codelist(cl)

    def get_codelist(self, name: str) -> Optional[CodelistSpec]:
        """Get a codelist by name."""
        return self._codelists.get(name.upper())

    def list_codelists(self) -> List[str]:
        """List all codelist names."""
        return list(self._codelists.keys())

    def clear_codelists(self):
        """Clear all codelists."""
        self._codelists.clear()
        self._codelist_usage.clear()

    def merge_variable(self, variable: VariableSpec) -> EnrichedVariable:
        """
        Merge a single variable with its codelist.

        Args:
            variable: Variable specification

        Returns:
            EnrichedVariable with codelist information
        """
        enriched = EnrichedVariable(variable=variable)

        if not variable.codelist:
            return enriched

        # Try to find codelist
        cl_name = variable.codelist.upper().strip()

        # Try exact match first
        codelist = self._codelists.get(cl_name)

        # Try with common prefixes/suffixes removed
        if not codelist:
            for suffix in ['_CL', '_CT', '_CODELIST']:
                if cl_name.endswith(suffix):
                    codelist = self._codelists.get(cl_name[:-len(suffix)])
                    break
            for prefix in ['CL_', 'CT_']:
                if cl_name.startswith(prefix):
                    codelist = self._codelists.get(cl_name[len(prefix):])
                    break

        # Try partial match
        if not codelist:
            for key in self._codelists:
                if cl_name in key or key in cl_name:
                    codelist = self._codelists[key]
                    break

        if codelist:
            enriched.has_codelist = True
            enriched.codelist_name = codelist.name
            enriched.codelist_label = codelist.label
            enriched.codelist_values = codelist.values.copy()
            self._codelist_usage[codelist.name.upper()] = \
                self._codelist_usage.get(codelist.name.upper(), 0) + 1
        else:
            enriched.codelist_missing = True
            enriched.codelist_name = variable.codelist  # Keep original reference

        return enriched

    def merge_domain(self, domain: DomainSpec) -> EnrichedDomain:
        """
        Merge all variables in a domain with codelists.

        Args:
            domain: Domain specification

        Returns:
            EnrichedDomain with all variables enriched
        """
        enriched_vars = []
        missing_cls = set()
        vars_with_cl = 0
        vars_expecting_cl = 0

        for var in domain.variables:
            enriched = self.merge_variable(var)
            enriched_vars.append(enriched)

            if var.codelist:
                vars_expecting_cl += 1
                if enriched.has_codelist:
                    vars_with_cl += 1
                else:
                    missing_cls.add(var.codelist)

        coverage = (vars_with_cl / vars_expecting_cl * 100) if vars_expecting_cl > 0 else 100.0

        return EnrichedDomain(
            domain=domain,
            variables=enriched_vars,
            codelist_coverage=round(coverage, 1),
            missing_codelists=list(missing_cls)
        )

    def merge_domains(self, domains: List[DomainSpec]) -> MergeResult:
        """
        Merge multiple domains with codelists.

        Args:
            domains: List of domain specifications

        Returns:
            MergeResult with all enriched domains
        """
        # Reset usage tracking
        self._codelist_usage = {k: 0 for k in self._codelists}

        enriched_domains = []
        all_missing = set()
        warnings = []

        for domain in domains:
            enriched = self.merge_domain(domain)
            enriched_domains.append(enriched)
            all_missing.update(enriched.missing_codelists)

        # Find unused codelists
        unused = [k for k, v in self._codelist_usage.items() if v == 0]

        # Generate warnings
        if all_missing:
            warnings.append(f"Missing codelists: {', '.join(sorted(all_missing))}")
        if unused:
            warnings.append(f"Unused codelists: {', '.join(sorted(unused))}")

        # Statistics
        total_vars = sum(len(d.variables) for d in enriched_domains)
        vars_with_cl = sum(
            sum(1 for v in d.variables if v.has_codelist)
            for d in enriched_domains
        )

        statistics = {
            'total_domains': len(enriched_domains),
            'total_variables': total_vars,
            'variables_with_codelist': vars_with_cl,
            'total_codelists': len(self._codelists),
            'codelists_used': len(self._codelists) - len(unused),
            'codelists_unused': len(unused),
            'missing_codelists': len(all_missing),
            'merged_at': datetime.now().isoformat()
        }

        return MergeResult(
            success=True,
            domains=enriched_domains,
            unused_codelists=unused,
            missing_codelists=list(all_missing),
            statistics=statistics,
            warnings=warnings
        )

    def generate_codelist_report(self) -> Dict[str, Any]:
        """Generate a report on codelist usage."""
        return {
            'total_codelists': len(self._codelists),
            'usage': {k: v for k, v in sorted(self._codelist_usage.items())},
            'unused': [k for k, v in self._codelist_usage.items() if v == 0],
            'most_used': sorted(
                self._codelist_usage.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
        }

    def find_codelist_by_value(self, value: str) -> List[Tuple[str, str]]:
        """
        Find codelists that contain a specific value.

        Args:
            value: Value to search for

        Returns:
            List of (codelist_name, decode) tuples
        """
        results = []
        value_upper = value.upper()

        for cl_name, cl in self._codelists.items():
            for item in cl.values:
                if item['code'].upper() == value_upper:
                    results.append((cl_name, item.get('decode', '')))

        return results

    def validate_value(self, codelist_name: str, value: str) -> Tuple[bool, Optional[str]]:
        """
        Validate a value against a codelist.

        Args:
            codelist_name: Name of the codelist
            value: Value to validate

        Returns:
            Tuple of (is_valid, decode_value)
        """
        codelist = self.get_codelist(codelist_name)
        if not codelist:
            return False, None

        value_upper = value.upper()
        for item in codelist.values:
            if item['code'].upper() == value_upper:
                return True, item.get('decode', '')

        return False, None
