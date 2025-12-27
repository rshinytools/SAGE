"""
Clinical Protocol Guard - Detect and resolve ambiguous clinical terms.

Ensures queries use protocol-defined thresholds and definitions.
In clinical trials, terms like "high," "recent," or "severe" have
specific protocol-defined meanings that must be enforced.
"""

import re
import json
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from pathlib import Path
from enum import Enum


class ResolutionType(Enum):
    """How a term was resolved."""
    AUTO_RESOLVED = "AUTO_RESOLVED"          # Mapped automatically via CDISC standard
    PROTOCOL_RESOLVED = "PROTOCOL_RESOLVED"  # Found in study protocol
    USER_CLARIFICATION = "USER_CLARIFICATION"  # Need to ask user
    CONTEXT_RESOLVED = "CONTEXT_RESOLVED"    # Resolved from query context


# Ambiguous terms registry with clinical context
AMBIGUOUS_TERMS: Dict[str, Dict[str, Any]] = {
    # Temporal terms
    "recent": {
        "type": "temporal",
        "ask": "Please define 'recent': Last 7 days, 30 days, or since last visit?",
        "protocol_key": "RECENT_DEFINITION"
    },
    "baseline": {
        "type": "temporal",
        "ask": "Clarify 'baseline': Screening visit, Day 1, or first dose?",
        "protocol_key": "BASELINE_DEFINITION",
        "common_mappings": {
            "screening": "VISIT = 'SCREENING'",
            "day1": "VISITNUM = 1",
            "day 1": "VISITNUM = 1",
            "first_dose": "VISITNUM = 1"
        }
    },
    "endpoint": {
        "type": "temporal",
        "ask": "Clarify 'endpoint': Final visit, Week 24, or early termination?",
        "protocol_key": "ENDPOINT_DEFINITION"
    },
    "early": {
        "type": "temporal",
        "ask": "Define 'early': First week, first month, or before specific visit?",
        "protocol_key": "EARLY_DEFINITION"
    },
    "late": {
        "type": "temporal",
        "ask": "Define 'late': Last month, after specific visit, or end of study?",
        "protocol_key": "LATE_DEFINITION"
    },

    # Severity/Threshold terms
    "high": {
        "type": "threshold",
        "ask": "Define 'high': What threshold value should I use?",
        "protocol_key": "HIGH_THRESHOLD",
        "context_mappings": {
            "blood pressure": {"sql": "SYSBP > 140 OR DIABP > 90", "description": "BP > 140/90"},
            "bp": {"sql": "SYSBP > 140 OR DIABP > 90", "description": "BP > 140/90"},
            "heart rate": {"sql": "HR > 100", "description": "HR > 100 bpm"},
            "hr": {"sql": "HR > 100", "description": "HR > 100 bpm"},
            "temperature": {"sql": "TEMP > 38.0", "description": "Temp > 38.0C"},
            "fever": {"sql": "TEMP > 38.0", "description": "Temp > 38.0C"},
            "glucose": {"sql": "GLUC > 126", "description": "Glucose > 126 mg/dL"},
            "cholesterol": {"sql": "CHOL > 240", "description": "Cholesterol > 240 mg/dL"}
        }
    },
    "low": {
        "type": "threshold",
        "ask": "Define 'low': What threshold value should I use?",
        "protocol_key": "LOW_THRESHOLD",
        "context_mappings": {
            "blood pressure": {"sql": "SYSBP < 90 OR DIABP < 60", "description": "BP < 90/60"},
            "heart rate": {"sql": "HR < 60", "description": "HR < 60 bpm"},
            "hemoglobin": {"sql": "HGB < 12", "description": "HGB < 12 g/dL"},
            "platelet": {"sql": "PLAT < 150", "description": "Platelets < 150K"}
        }
    },
    "elevated": {
        "type": "threshold",
        "ask": "Define 'elevated': What threshold above normal?",
        "protocol_key": "ELEVATED_THRESHOLD"
    },
    "abnormal": {
        "type": "threshold",
        "ask": "Define 'abnormal': Outside reference range, or specific criteria?",
        "protocol_key": "ABNORMAL_DEFINITION",
        "common_value": "LBNRIND IN ('HIGH', 'LOW', 'ABNORMAL')"
    },

    # AE Severity terms (CDISC standard - can auto-map)
    "severe": {
        "type": "severity",
        "auto_map": "AESEV = 'SEVERE'",
        "note": "CDISC standard severity grade"
    },
    "serious": {
        "type": "severity",
        "auto_map": "AESER = 'Y'",
        "note": "SAE indicator - different from severity"
    },
    "mild": {
        "type": "severity",
        "auto_map": "AESEV = 'MILD'",
        "note": "CDISC standard severity grade"
    },
    "moderate": {
        "type": "severity",
        "auto_map": "AESEV = 'MODERATE'",
        "note": "CDISC standard severity grade"
    },
    "life-threatening": {
        "type": "severity",
        "auto_map": "AESLIFE = 'Y'",
        "note": "Life-threatening SAE criterion"
    },
    "fatal": {
        "type": "severity",
        "auto_map": "AESDTH = 'Y'",
        "note": "Death-related SAE"
    },

    # Population/Age terms
    "elderly": {
        "type": "threshold",
        "ask": "Define 'elderly': Age >= 65 or Age >= 75?",
        "protocol_key": "ELDERLY_DEFINITION",
        "common_value": "AGE >= 65"
    },
    "pediatric": {
        "type": "threshold",
        "ask": "Define 'pediatric': Age < 18, Age < 12, or other?",
        "protocol_key": "PEDIATRIC_DEFINITION"
    },
    "adult": {
        "type": "threshold",
        "auto_map": "AGE >= 18",
        "note": "Standard adult definition"
    },
    "young": {
        "type": "threshold",
        "ask": "Define 'young': What age range?",
        "protocol_key": "YOUNG_DEFINITION"
    },
    "old": {
        "type": "threshold",
        "ask": "Define 'old': What age threshold?",
        "protocol_key": "OLD_DEFINITION"
    },

    # Clinical outcome terms
    "responder": {
        "type": "clinical",
        "ask": "Define 'responder': What response criteria?",
        "protocol_key": "RESPONDER_DEFINITION"
    },
    "non-responder": {
        "type": "clinical",
        "ask": "Define 'non-responder': What criteria for non-response?",
        "protocol_key": "NONRESPONDER_DEFINITION"
    },
    "completer": {
        "type": "clinical",
        "auto_map": "COMPLFL = 'Y'",
        "note": "Study completer flag"
    },
    "discontinuer": {
        "type": "clinical",
        "auto_map": "DCSREAS IS NOT NULL",
        "note": "Discontinued subject"
    },
    "dropout": {
        "type": "clinical",
        "auto_map": "DCSREAS IS NOT NULL",
        "note": "Same as discontinuer"
    },

    # Treatment-related
    "treated": {
        "type": "clinical",
        "auto_map": "SAFFL = 'Y'",
        "note": "Received at least one dose"
    },
    "untreated": {
        "type": "clinical",
        "auto_map": "SAFFL = 'N'",
        "note": "Did not receive treatment"
    },

    # Frequency terms
    "frequent": {
        "type": "threshold",
        "ask": "Define 'frequent': What count or percentage threshold?",
        "protocol_key": "FREQUENT_DEFINITION"
    },
    "rare": {
        "type": "threshold",
        "ask": "Define 'rare': What count or percentage threshold?",
        "protocol_key": "RARE_DEFINITION"
    },
    "common": {
        "type": "threshold",
        "ask": "Define 'common': What percentage threshold (e.g., >5%)?",
        "protocol_key": "COMMON_DEFINITION"
    },

    # Significance terms
    "significant": {
        "type": "statistical",
        "ask": "Define 'significant': Statistical significance (p<0.05) or clinical significance?",
        "protocol_key": "SIGNIFICANCE_DEFINITION"
    },
    "clinically significant": {
        "type": "clinical",
        "ask": "Define 'clinically significant': What clinical criteria?",
        "protocol_key": "CLINICAL_SIGNIFICANCE_DEFINITION"
    }
}


@dataclass
class AmbiguousTerm:
    """An ambiguous term found in a query."""
    term: str
    term_type: str
    position: int
    context: str  # Surrounding text for context


@dataclass
class TermResolution:
    """Resolution of an ambiguous term."""
    term: str
    resolution_type: ResolutionType
    resolved_value: Optional[str]
    sql_fragment: Optional[str]
    clarification_needed: bool
    clarification_question: Optional[str]
    confidence: float
    note: Optional[str] = None


@dataclass
class ProtocolGuardResult:
    """Result of protocol guard check."""
    query: str
    enhanced_query: str  # Query with resolved terms annotated
    terms_found: List[AmbiguousTerm]
    resolutions: List[TermResolution]
    all_resolved: bool
    clarifications_needed: List[str]
    sql_enhancements: Dict[str, str]  # term -> SQL fragment


class ProtocolGuard:
    """
    Clinical Protocol Guard - Resolves ambiguous terms using protocol definitions.

    This component ensures clinical queries use precise, protocol-defined
    meanings rather than ambiguous natural language terms.
    """

    def __init__(
        self,
        protocol_path: str = "knowledge/study_protocol.json",
        ambiguous_terms: Optional[Dict] = None
    ):
        """
        Initialize Protocol Guard.

        Args:
            protocol_path: Path to study protocol definitions JSON
            ambiguous_terms: Optional custom ambiguous terms registry
        """
        self.protocol_path = Path(protocol_path)
        self.ambiguous_terms = ambiguous_terms or AMBIGUOUS_TERMS
        self.protocol = self._load_protocol()

    def _load_protocol(self) -> Dict:
        """Load study protocol definitions."""
        if self.protocol_path.exists():
            try:
                with open(self.protocol_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load protocol file: {e}")
                return {}
        return {}

    def check_query(self, query: str) -> ProtocolGuardResult:
        """
        Check query for ambiguous terms and attempt resolution.

        Args:
            query: The user's natural language query

        Returns:
            ProtocolGuardResult with resolution status and any needed clarifications
        """
        query_lower = query.lower()
        terms_found: List[AmbiguousTerm] = []
        resolutions: List[TermResolution] = []
        sql_enhancements: Dict[str, str] = {}
        clarifications_needed: List[str] = []
        enhanced_query = query

        # Find all ambiguous terms
        for term, config in self.ambiguous_terms.items():
            # Use word boundary matching to avoid partial matches
            pattern = rf'\b{re.escape(term)}\b'
            matches = list(re.finditer(pattern, query_lower))

            for match in matches:
                # Extract context (surrounding words)
                context = self._extract_context(query, match.start(), match.end())

                terms_found.append(AmbiguousTerm(
                    term=term,
                    term_type=config.get("type", "unknown"),
                    position=match.start(),
                    context=context
                ))

                # Attempt resolution
                resolution = self._resolve_term(term, config, context, query)
                resolutions.append(resolution)

                if resolution.sql_fragment:
                    sql_enhancements[term] = resolution.sql_fragment

                if resolution.clarification_needed and resolution.clarification_question:
                    clarifications_needed.append(resolution.clarification_question)

                # Enhance query with resolution annotation
                if resolution.resolved_value and not resolution.clarification_needed:
                    enhanced_query = self._enhance_query(
                        enhanced_query, term, resolution.resolved_value
                    )

        return ProtocolGuardResult(
            query=query,
            enhanced_query=enhanced_query,
            terms_found=terms_found,
            resolutions=resolutions,
            all_resolved=len(clarifications_needed) == 0,
            clarifications_needed=clarifications_needed,
            sql_enhancements=sql_enhancements
        )

    def _extract_context(self, query: str, start: int, end: int, window: int = 40) -> str:
        """Extract context around a term for better resolution."""
        context_start = max(0, start - window)
        context_end = min(len(query), end + window)
        return query[context_start:context_end].strip()

    def _resolve_term(
        self,
        term: str,
        config: Dict,
        context: str,
        full_query: str
    ) -> TermResolution:
        """
        Attempt to resolve an ambiguous term.

        Resolution priority:
        1. Auto-map (CDISC standard terms)
        2. Protocol definition
        3. Context-based resolution
        4. Common value (with confirmation)
        5. User clarification required
        """
        # Priority 1: Auto-map if available (CDISC standard)
        if "auto_map" in config:
            return TermResolution(
                term=term,
                resolution_type=ResolutionType.AUTO_RESOLVED,
                resolved_value=config["auto_map"],
                sql_fragment=config["auto_map"],
                clarification_needed=False,
                clarification_question=None,
                confidence=1.0,
                note=config.get("note")
            )

        # Priority 2: Check protocol definitions
        protocol_key = config.get("protocol_key")
        if protocol_key and protocol_key in self.protocol:
            protocol_value = self.protocol[protocol_key]
            sql_fragment = self._protocol_to_sql(term, protocol_value)
            return TermResolution(
                term=term,
                resolution_type=ResolutionType.PROTOCOL_RESOLVED,
                resolved_value=str(protocol_value.get("description", protocol_value)),
                sql_fragment=sql_fragment,
                clarification_needed=False,
                clarification_question=None,
                confidence=0.95,
                note=f"From study protocol: {protocol_key}"
            )

        # Priority 3: Context-based resolution
        if "context_mappings" in config:
            context_resolution = self._resolve_from_context(
                term, config["context_mappings"], context, full_query
            )
            if context_resolution:
                return context_resolution

        # Priority 4: Common value (with lower confidence, may ask for confirmation)
        if "common_value" in config:
            return TermResolution(
                term=term,
                resolution_type=ResolutionType.CONTEXT_RESOLVED,
                resolved_value=config["common_value"],
                sql_fragment=config["common_value"],
                clarification_needed=False,  # Use common value but note it
                clarification_question=None,
                confidence=0.75,
                note=f"Using common definition: {config['common_value']}"
            )

        # Priority 5: Need user clarification
        return TermResolution(
            term=term,
            resolution_type=ResolutionType.USER_CLARIFICATION,
            resolved_value=None,
            sql_fragment=None,
            clarification_needed=True,
            clarification_question=config.get("ask", f"Please define '{term}' for this query."),
            confidence=0.0,
            note=None
        )

    def _resolve_from_context(
        self,
        term: str,
        context_mappings: Dict,
        context: str,
        full_query: str
    ) -> Optional[TermResolution]:
        """Resolve term based on query context."""
        context_lower = context.lower()
        query_lower = full_query.lower()

        for context_key, mapping in context_mappings.items():
            # Check if context key appears in query
            context_patterns = [
                context_key,
                context_key.replace("_", " "),
                context_key.replace("-", " ")
            ]

            for pattern in context_patterns:
                if pattern in query_lower or pattern in context_lower:
                    if isinstance(mapping, dict):
                        sql_fragment = mapping.get("sql", str(mapping))
                        description = mapping.get("description", sql_fragment)
                    else:
                        sql_fragment = str(mapping)
                        description = sql_fragment

                    return TermResolution(
                        term=term,
                        resolution_type=ResolutionType.CONTEXT_RESOLVED,
                        resolved_value=description,
                        sql_fragment=sql_fragment,
                        clarification_needed=False,
                        clarification_question=None,
                        confidence=0.85,
                        note=f"Resolved from context: '{context_key}'"
                    )

        return None

    def _protocol_to_sql(self, term: str, protocol_value: Any) -> str:
        """Convert protocol definition to SQL fragment."""
        if isinstance(protocol_value, dict):
            if "sql" in protocol_value:
                return protocol_value["sql"]
            if "column" in protocol_value and "operator" in protocol_value:
                value = protocol_value.get("value", "")
                if isinstance(value, str):
                    return f"{protocol_value['column']} {protocol_value['operator']} '{value}'"
                return f"{protocol_value['column']} {protocol_value['operator']} {value}"
        return str(protocol_value)

    def _enhance_query(self, query: str, term: str, resolution: str) -> str:
        """Enhance query with resolved term definition in parentheses."""
        # Find the term and add resolution annotation
        pattern = rf'\b({re.escape(term)})\b'

        def replacement(match):
            return f"{match.group(1)} [{resolution}]"

        # Only replace first occurrence to avoid duplication
        return re.sub(pattern, replacement, query, count=1, flags=re.IGNORECASE)

    def apply_user_clarification(
        self,
        term: str,
        user_value: str,
        sql_fragment: Optional[str] = None
    ) -> TermResolution:
        """
        Apply user-provided clarification for a term.

        Args:
            term: The ambiguous term
            user_value: User's clarification
            sql_fragment: Optional SQL fragment (if not provided, uses user_value)

        Returns:
            TermResolution with user-provided value
        """
        return TermResolution(
            term=term,
            resolution_type=ResolutionType.USER_CLARIFICATION,
            resolved_value=user_value,
            sql_fragment=sql_fragment or user_value,
            clarification_needed=False,
            clarification_question=None,
            confidence=1.0,  # User-provided is authoritative
            note="User-provided clarification"
        )

    def get_ambiguous_terms(self) -> Dict[str, Dict]:
        """Get the registry of ambiguous terms."""
        return self.ambiguous_terms.copy()

    def add_protocol_definition(self, key: str, definition: Dict):
        """
        Add or update a protocol definition.

        Args:
            key: Protocol key (e.g., "BASELINE_DEFINITION")
            definition: Definition dict with 'sql' and/or 'description'
        """
        self.protocol[key] = definition

    def save_protocol(self):
        """Save current protocol definitions to file."""
        self.protocol_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.protocol_path, 'w') as f:
            json.dump(self.protocol, f, indent=2)
