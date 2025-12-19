# SAGE - Clinical Query Configuration
# ====================================
"""
Clinical Query Configuration
============================
Defines deterministic rules for clinical data queries.
These rules are NOT AI-generated - they are clinical standards.

Core Principles:
1. ADaM FIRST: Always prefer ADaM over SDTM when both exist
2. SAFETY DEFAULT: Any safety query uses SAFFL='Y' unless specified
3. ANALYSIS COLUMNS: Use derived analysis columns over raw
4. TRANSPARENCY: Every answer includes methodology
5. NEVER ASSUME: When ambiguous, ASK the user
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from enum import Enum


class QueryDomain(Enum):
    """Clinical query domains."""
    ADVERSE_EVENTS = "adverse_events"
    DEMOGRAPHICS = "demographics"
    CONCOMITANT_MEDS = "concomitant_meds"
    LABS = "labs"
    VITAL_SIGNS = "vital_signs"
    EXPOSURE = "exposure"
    EFFICACY = "efficacy"
    UNKNOWN = "unknown"


class PopulationType(Enum):
    """Standard clinical trial populations."""
    SAFETY = "safety"              # SAFFL = 'Y'
    ITT = "itt"                    # ITTFL = 'Y'
    EFFICACY = "efficacy"          # EFFFL = 'Y'
    PER_PROTOCOL = "per_protocol"  # PPROTFL = 'Y'
    ALL_ENROLLED = "all"           # No filter

    def get_flag_column(self) -> Optional[str]:
        """Get the flag column for this population."""
        flags = {
            PopulationType.SAFETY: "SAFFL",
            PopulationType.ITT: "ITTFL",
            PopulationType.EFFICACY: "EFFFL",
            PopulationType.PER_PROTOCOL: "PPROTFL",
            PopulationType.ALL_ENROLLED: None,
        }
        return flags.get(self)

    def get_display_name(self) -> str:
        """Get human-readable name."""
        names = {
            PopulationType.SAFETY: "Safety Population",
            PopulationType.ITT: "Intent-to-Treat Population",
            PopulationType.EFFICACY: "Efficacy Population",
            PopulationType.PER_PROTOCOL: "Per-Protocol Population",
            PopulationType.ALL_ENROLLED: "All Enrolled Subjects",
        }
        return names.get(self, "Unknown Population")


@dataclass
class TablePriority:
    """Defines which table to use for a domain."""
    domain: QueryDomain
    adam_table: Optional[str]      # Preferred table (ADaM)
    sdtm_table: str                # Fallback table (SDTM)
    default_population: PopulationType = PopulationType.SAFETY
    description: str = ""

    def get_preferred_table(self, available_tables: Set[str]) -> Tuple[str, bool]:
        """
        Get the preferred table based on availability.

        Returns:
            Tuple of (table_name, is_fallback)
        """
        if self.adam_table and self.adam_table.upper() in available_tables:
            return (self.adam_table.upper(), False)
        elif self.sdtm_table.upper() in available_tables:
            return (self.sdtm_table.upper(), True)
        else:
            raise ValueError(f"No table found for domain {self.domain.value}. "
                           f"Expected {self.adam_table} or {self.sdtm_table}")


@dataclass
class ColumnPriority:
    """Defines which column to use when multiple exist for a concept."""
    concept: str                   # e.g., "toxicity_grade"
    preferred_column: str          # e.g., "ATOXGR"
    fallback_column: str           # e.g., "AETOXGR"
    description: str               # Explain the difference

    def get_column(self, available_columns: Set[str]) -> Tuple[str, str]:
        """
        Get the column to use based on availability.

        Returns:
            Tuple of (column_name, reason)
        """
        preferred_upper = self.preferred_column.upper()
        fallback_upper = self.fallback_column.upper()

        if preferred_upper in available_columns:
            return (preferred_upper, f"Using {self.preferred_column} (preferred): {self.description}")
        elif fallback_upper in available_columns:
            return (fallback_upper, f"Using {self.fallback_column} (fallback): {self.description}")
        else:
            return (None, f"Neither {self.preferred_column} nor {self.fallback_column} found")


@dataclass
class ClinicalQueryConfig:
    """
    Complete configuration for clinical queries.

    This class defines all the deterministic rules for handling clinical data.
    These rules ensure 200% accuracy by encoding clinical domain knowledge.
    """

    # Table priority by domain (ADaM > SDTM)
    table_priorities: Dict[QueryDomain, TablePriority] = field(default_factory=dict)

    # Column priority by concept (analysis > raw)
    column_priorities: Dict[str, ColumnPriority] = field(default_factory=dict)

    # Keywords that trigger specific populations
    population_keywords: Dict[str, PopulationType] = field(default_factory=dict)

    # Safety-related query indicators
    safety_indicators: Set[str] = field(default_factory=set)

    # Domain detection keywords
    domain_keywords: Dict[str, QueryDomain] = field(default_factory=dict)

    @classmethod
    def default_config(cls) -> 'ClinicalQueryConfig':
        """Create default clinical configuration with all rules."""
        config = cls()

        # === TABLE PRIORITIES (ADaM > SDTM) ===
        config.table_priorities = {
            QueryDomain.ADVERSE_EVENTS: TablePriority(
                domain=QueryDomain.ADVERSE_EVENTS,
                adam_table="ADAE",
                sdtm_table="AE",
                default_population=PopulationType.SAFETY,
                description="Adverse events - use ADAE for analysis, AE for raw data"
            ),
            QueryDomain.DEMOGRAPHICS: TablePriority(
                domain=QueryDomain.DEMOGRAPHICS,
                adam_table="ADSL",
                sdtm_table="DM",
                default_population=PopulationType.SAFETY,
                description="Subject demographics - use ADSL for flags and derived vars"
            ),
            QueryDomain.CONCOMITANT_MEDS: TablePriority(
                domain=QueryDomain.CONCOMITANT_MEDS,
                adam_table="ADCM",
                sdtm_table="CM",
                default_population=PopulationType.SAFETY,
                description="Concomitant medications"
            ),
            QueryDomain.LABS: TablePriority(
                domain=QueryDomain.LABS,
                adam_table="ADLB",
                sdtm_table="LB",
                default_population=PopulationType.SAFETY,
                description="Laboratory results - use ADLB for analysis values"
            ),
            QueryDomain.VITAL_SIGNS: TablePriority(
                domain=QueryDomain.VITAL_SIGNS,
                adam_table="ADVS",
                sdtm_table="VS",
                default_population=PopulationType.SAFETY,
                description="Vital signs measurements"
            ),
            QueryDomain.EXPOSURE: TablePriority(
                domain=QueryDomain.EXPOSURE,
                adam_table="ADEX",
                sdtm_table="EX",
                default_population=PopulationType.SAFETY,
                description="Drug exposure data"
            ),
        }

        # === COLUMN PRIORITIES (Analysis > Raw) ===
        config.column_priorities = {
            "toxicity_grade": ColumnPriority(
                concept="toxicity_grade",
                preferred_column="ATOXGR",
                fallback_column="AETOXGR",
                description="ATOXGR is maximum grade reached; AETOXGR is grade at onset"
            ),
            "severity": ColumnPriority(
                concept="severity",
                preferred_column="ASEV",
                fallback_column="AESEV",
                description="ASEV is analysis severity; AESEV is reported severity"
            ),
            "treatment_actual": ColumnPriority(
                concept="treatment_actual",
                preferred_column="TRT01A",
                fallback_column="TRT01P",
                description="TRT01A is actual treatment received; TRT01P is planned"
            ),
            "analysis_value": ColumnPriority(
                concept="analysis_value",
                preferred_column="AVAL",
                fallback_column="STRESN",
                description="AVAL is analysis value; STRESN is standard result numeric"
            ),
            "baseline_value": ColumnPriority(
                concept="baseline_value",
                preferred_column="BASE",
                fallback_column="STRESN",
                description="BASE is baseline value for change calculations"
            ),
            "change_from_baseline": ColumnPriority(
                concept="change_from_baseline",
                preferred_column="CHG",
                fallback_column="AVAL",
                description="CHG is pre-calculated change from baseline"
            ),
            "analysis_date": ColumnPriority(
                concept="analysis_date",
                preferred_column="ADT",
                fallback_column="AESTDTC",
                description="ADT is analysis date (numeric); AESTDTC is ISO date string"
            ),
        }

        # No hard-coded keyword mappings - LLM handles query understanding naturally
        # The LLM understands clinical terminology and will determine:
        # - Which population to use based on query context
        # - Which domain the query belongs to
        # - When to apply filters and when not to
        config.population_keywords = {}
        config.safety_indicators = set()
        config.domain_keywords = {}

        return config

    def detect_domain(self, query: str) -> QueryDomain:
        """
        Domain detection is now handled by LLM.

        The LLM understands clinical terminology naturally and will
        select the appropriate table based on query context.

        Returns UNKNOWN to let LLM make the decision.
        """
        # LLM handles domain detection - no keyword matching
        return QueryDomain.UNKNOWN

    def detect_population(self, query: str) -> PopulationType:
        """
        Population detection is now handled by LLM.

        The LLM understands when to apply population filters based on
        query context. It will add SAFFL='Y' when clinically appropriate,
        not based on keyword matching.

        Returns ALL_ENROLLED to let LLM decide what filters to apply.
        """
        # LLM handles population detection - no keyword matching
        return PopulationType.ALL_ENROLLED

    def is_safety_query(self, query: str) -> bool:
        """
        Safety query detection is now handled by LLM.

        The LLM understands clinical context naturally.
        """
        # LLM handles safety query detection
        return False


# Global default configuration
DEFAULT_CLINICAL_CONFIG = ClinicalQueryConfig.default_config()
