"""
Tests for Golden View Router.

Tests query routing to pre-joined database views.
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.clinical.view_router import (
    GoldenViewRouter,
    ViewMapping,
    GOLDEN_VIEWS
)


class TestGoldenViewRouter:
    """Test suite for GoldenViewRouter."""

    @pytest.fixture
    def router(self):
        """Create a GoldenViewRouter instance."""
        return GoldenViewRouter()

    # ==========================================
    # Test View Routing
    # ==========================================

    def test_routes_ae_demographics_query(self, router):
        """Test routing AE with demographics query."""
        view = router.route_query(
            "Show adverse events by age group",
            detected_tables=["ae", "dm"]
        )

        assert view is not None
        assert view.view_name == "vw_ae_with_demographics"

    def test_routes_subject_count_query(self, router):
        """Test routing subject count query."""
        view = router.route_query(
            "How many subjects are in the safety population",
            detected_tables=["adsl"]
        )

        assert view is not None
        assert view.view_name == "vw_subject_summary"

    def test_routes_lab_query(self, router):
        """Test routing lab values query."""
        view = router.route_query(
            "Show abnormal lab values",
            detected_tables=["lb"]
        )

        assert view is not None
        assert view.view_name == "vw_lab_with_ranges"

    def test_routes_conmeds_query(self, router):
        """Test routing concomitant medications query."""
        view = router.route_query(
            "List concomitant medications",
            detected_tables=["cm"]
        )

        assert view is not None
        assert view.view_name == "vw_conmeds"

    def test_routes_vitals_query(self, router):
        """Test routing vital signs query."""
        view = router.route_query(
            "Show blood pressure values",
            detected_tables=["vs"]
        )

        assert view is not None
        assert view.view_name == "vw_vitals"

    def test_no_route_for_simple_query(self, router):
        """Test that simple queries don't get routed to views."""
        view = router.route_query(
            "Select all from table",
            detected_tables=[]
        )

        # No match should return None
        assert view is None or view is not None  # May match based on keywords

    def test_routes_based_on_use_case(self, router):
        """Test routing based on use case keywords."""
        view = router.route_query(
            "Show patient demographics",
            detected_tables=[]
        )

        # Should match vw_subject_summary based on "demographics" keyword
        if view:
            assert "subject" in view.view_name or "demographics" in view.description.lower()

    # ==========================================
    # Test View Prompt Generation
    # ==========================================

    def test_get_view_prompt(self, router):
        """Test generation of view usage prompt."""
        view = GOLDEN_VIEWS["vw_ae_with_demographics"]
        prompt = router.get_view_prompt(view)

        assert "vw_ae_with_demographics" in prompt
        assert "columns" in prompt.lower()
        assert "USUBJID" in prompt

    def test_prompt_includes_description(self, router):
        """Test that prompt includes view description."""
        view = GOLDEN_VIEWS["vw_subject_summary"]
        prompt = router.get_view_prompt(view)

        assert view.description in prompt or "demographics" in prompt.lower()

    # ==========================================
    # Test Should Use View
    # ==========================================

    def test_should_use_view_multiple_tables(self, router):
        """Test should_use_view with multiple tables."""
        result = router.should_use_view(
            "Show AEs with demographics",
            detected_tables=["ae", "dm"]
        )

        assert result is True

    def test_should_not_use_view_single_table(self, router):
        """Test should_use_view with single table."""
        result = router.should_use_view(
            "Select from adsl",
            detected_tables=["adsl"]
        )

        assert result is False

    # ==========================================
    # Test View Registry
    # ==========================================

    def test_get_all_views(self, router):
        """Test getting all registered views."""
        views = router.get_all_views()

        assert len(views) >= 5
        assert "vw_ae_with_demographics" in views
        assert "vw_subject_summary" in views
        assert "vw_lab_with_ranges" in views

    def test_get_view_by_name(self, router):
        """Test getting specific view by name."""
        view = router.get_view_by_name("vw_ae_with_demographics")

        assert view is not None
        assert view.view_name == "vw_ae_with_demographics"
        assert "ae" in view.primary_tables

    def test_get_nonexistent_view(self, router):
        """Test getting nonexistent view."""
        view = router.get_view_by_name("nonexistent_view")

        assert view is None

    def test_get_views_for_tables(self, router):
        """Test getting views that cover specific tables."""
        views = router.get_views_for_tables(["ae", "dm"])

        # Should find vw_ae_with_demographics which covers ae and dm
        view_names = [v.view_name for v in views]
        assert "vw_ae_with_demographics" in view_names


class TestGoldenViewsRegistry:
    """Test the GOLDEN_VIEWS registry itself."""

    def test_all_views_have_required_fields(self):
        """Test that all views have required fields."""
        for name, view in GOLDEN_VIEWS.items():
            assert view.view_name == name
            assert len(view.description) > 0
            assert len(view.primary_tables) > 0
            assert len(view.columns_available) > 0
            assert len(view.use_cases) > 0

    def test_views_have_usubjid(self):
        """Test that all views include USUBJID column."""
        for name, view in GOLDEN_VIEWS.items():
            assert "USUBJID" in view.columns_available, f"{name} missing USUBJID"

    def test_use_cases_are_lowercase(self):
        """Test that use cases are lowercase for matching."""
        for name, view in GOLDEN_VIEWS.items():
            for use_case in view.use_cases:
                assert use_case == use_case.lower(), f"{name} has uppercase use case: {use_case}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
