"""
Golden View Router - Maps queries to appropriate pre-joined views.

Reduces SQL complexity by routing complex multi-table queries to
pre-validated, pre-joined database views.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class ViewMapping:
    """Mapping of query intent to golden view."""
    view_name: str
    description: str
    primary_tables: List[str]
    columns_available: List[str]
    use_cases: List[str]


@dataclass
class ViewRoutingResult:
    """Result of view routing."""
    should_use_view: bool
    view_name: Optional[str] = None
    view_mapping: Optional[ViewMapping] = None
    reason: Optional[str] = None
    prompt_addition: Optional[str] = None


# Golden View Registry
GOLDEN_VIEWS: Dict[str, ViewMapping] = {
    "vw_ae_with_demographics": ViewMapping(
        view_name="vw_ae_with_demographics",
        description="Adverse events joined with demographics and treatment",
        primary_tables=["ae", "dm", "adsl"],
        columns_available=[
            "USUBJID", "AGE", "SEX", "RACE", "TREATMENT",
            "AETERM", "AEDECOD", "AEBODSYS", "AESEV", "AESER",
            "SAFFL", "IS_SAE", "IS_RELATED"
        ],
        use_cases=[
            "adverse events by age",
            "ae by treatment",
            "serious adverse events",
            "safety population ae",
            "ae demographics"
        ]
    ),

    "vw_subject_summary": ViewMapping(
        view_name="vw_subject_summary",
        description="Subject demographics with disposition",
        primary_tables=["adsl"],
        columns_available=[
            "USUBJID", "AGE", "SEX", "RACE", "TRT01P",
            "SAFFL", "ITTFL", "COMPLFL", "DCSREAS",
            "IN_SAFETY_POP", "COMPLETED_STUDY", "DISCONTINUED"
        ],
        use_cases=[
            "patient count",
            "subject count",
            "demographics",
            "safety population",
            "completers",
            "discontinuations",
            "disposition"
        ]
    ),

    "vw_lab_with_ranges": ViewMapping(
        view_name="vw_lab_with_ranges",
        description="Lab values with reference ranges and abnormal flags",
        primary_tables=["lb", "adsl"],
        columns_available=[
            "USUBJID", "LBTESTCD", "LBTEST", "LBSTRESN",
            "LBORNRLO", "LBORNRHI", "VISIT", "TREATMENT",
            "RANGE_STATUS", "IS_ABNORMAL"
        ],
        use_cases=[
            "lab values",
            "laboratory",
            "abnormal labs",
            "lab by visit",
            "lab trends",
            "blood test"
        ]
    ),

    "vw_conmeds": ViewMapping(
        view_name="vw_conmeds",
        description="Concomitant medications with treatment",
        primary_tables=["cm", "adsl"],
        columns_available=[
            "USUBJID", "CMTRT", "CMDECOD", "CMCAT",
            "TREATMENT", "IS_PRIOR", "IS_ONGOING"
        ],
        use_cases=[
            "concomitant medications",
            "prior medications",
            "medications by treatment",
            "conmeds",
            "drug use"
        ]
    ),

    "vw_vitals": ViewMapping(
        view_name="vw_vitals",
        description="Vital signs with treatment",
        primary_tables=["vs", "adsl"],
        columns_available=[
            "USUBJID", "VSTESTCD", "VSTEST", "VSSTRESN",
            "VISIT", "TREATMENT"
        ],
        use_cases=[
            "vital signs",
            "blood pressure",
            "heart rate",
            "temperature",
            "weight",
            "height",
            "bmi"
        ]
    )
}


class GoldenViewRouter:
    """Routes queries to appropriate golden views."""

    def __init__(self, views: Optional[Dict[str, ViewMapping]] = None):
        """
        Initialize router with view registry.

        Args:
            views: Optional custom view registry (defaults to GOLDEN_VIEWS)
        """
        self.views = views or GOLDEN_VIEWS

    def route_query(
        self,
        question: str,
        detected_tables: Optional[List[str]] = None
    ) -> Optional[ViewMapping]:
        """
        Determine if query should use a golden view.

        Args:
            question: The user's natural language query
            detected_tables: Tables detected in the query

        Returns:
            ViewMapping if a suitable view exists, None otherwise
        """
        question_lower = question.lower()
        detected_tables = [t.lower() for t in (detected_tables or [])]

        best_match: Optional[ViewMapping] = None
        best_score = 0

        for view_name, mapping in self.views.items():
            score = 0

            # Check use case matches (highest weight)
            for use_case in mapping.use_cases:
                if use_case in question_lower:
                    score += 10
                # Partial match
                elif any(word in question_lower for word in use_case.split()):
                    score += 3

            # Check table matches
            for table in mapping.primary_tables:
                if table.lower() in detected_tables:
                    score += 5

            # Check column mentions
            for col in mapping.columns_available:
                if col.lower() in question_lower:
                    score += 2

            if score > best_score:
                best_score = score
                best_match = mapping

        # Only return if we have a meaningful match
        return best_match if best_score >= 5 else None

    def get_view_prompt(self, view: ViewMapping) -> str:
        """
        Generate prompt addition for using a golden view.

        Args:
            view: The ViewMapping to use

        Returns:
            Prompt text to inject for LLM
        """
        columns_str = ', '.join(view.columns_available)
        return f"""
Use the pre-joined view `{view.view_name}` instead of joining tables directly.

Available columns in this view:
{columns_str}

This view already includes:
{view.description}

Example: SELECT * FROM {view.view_name} WHERE ...
"""

    def should_use_view(
        self,
        question: str,
        detected_tables: List[str]
    ) -> bool:
        """
        Check if query involves multiple tables that have a golden view.

        Args:
            question: User's query
            detected_tables: Tables detected in query

        Returns:
            True if a golden view should be used
        """
        if len(detected_tables) < 2:
            return False

        return self.route_query(question, detected_tables) is not None

    def get_all_views(self) -> Dict[str, ViewMapping]:
        """Get all registered golden views."""
        return self.views.copy()

    def get_view_by_name(self, name: str) -> Optional[ViewMapping]:
        """Get a specific view by name."""
        return self.views.get(name)

    def get_views_for_tables(self, tables: List[str]) -> List[ViewMapping]:
        """
        Get all views that cover the specified tables.

        Args:
            tables: List of table names

        Returns:
            List of ViewMappings that include all specified tables
        """
        tables_lower = {t.lower() for t in tables}
        matching_views = []

        for view in self.views.values():
            view_tables = {t.lower() for t in view.primary_tables}
            if tables_lower.issubset(view_tables):
                matching_views.append(view)

        return matching_views

    def route(
        self,
        question: str,
        required_tables: List[str]
    ) -> ViewRoutingResult:
        """
        Route a query to an appropriate golden view.

        Args:
            question: User's query
            required_tables: Tables required by the query

        Returns:
            ViewRoutingResult with routing decision
        """
        if len(required_tables) < 2:
            return ViewRoutingResult(
                should_use_view=False,
                reason="Single table query - no view needed"
            )

        view = self.route_query(question, required_tables)

        if view:
            return ViewRoutingResult(
                should_use_view=True,
                view_name=view.view_name,
                view_mapping=view,
                reason=f"Query matches view use case: {view.description}",
                prompt_addition=self.get_view_prompt(view)
            )

        return ViewRoutingResult(
            should_use_view=False,
            reason="No suitable view found for tables"
        )
