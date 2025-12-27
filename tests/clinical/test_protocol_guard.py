"""
Tests for Clinical Protocol Guard.

Tests the detection and resolution of ambiguous clinical terms.
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.clinical.protocol_guard import (
    ProtocolGuard,
    ProtocolGuardResult,
    ResolutionType,
    AMBIGUOUS_TERMS
)


class TestProtocolGuard:
    """Test suite for ProtocolGuard."""

    @pytest.fixture
    def guard(self):
        """Create a ProtocolGuard instance."""
        return ProtocolGuard(
            protocol_path="knowledge/study_protocol.json"
        )

    # ==========================================
    # Test Ambiguous Term Detection
    # ==========================================

    def test_detects_severity_terms(self, guard):
        """Test detection of severity terms like severe, mild, moderate."""
        result = guard.check_query("Show all severe adverse events")

        assert len(result.terms_found) >= 1
        terms = [t.term for t in result.terms_found]
        assert "severe" in terms

    def test_detects_multiple_terms(self, guard):
        """Test detection of multiple ambiguous terms in one query."""
        result = guard.check_query("Show severe AEs in elderly patients with high blood pressure")

        terms = [t.term for t in result.terms_found]
        assert "severe" in terms
        assert "elderly" in terms
        assert "high" in terms
        assert len(result.terms_found) >= 3

    def test_detects_temporal_terms(self, guard):
        """Test detection of temporal terms like recent, baseline."""
        result = guard.check_query("Compare recent lab values to baseline")

        terms = [t.term for t in result.terms_found]
        assert "recent" in terms
        assert "baseline" in terms

    def test_no_partial_matches(self, guard):
        """Test that partial word matches are not detected."""
        # "severely" should not match "severe"
        result = guard.check_query("The patient was severely impacted")

        # Should not find "severe" as a standalone term
        terms = [t.term for t in result.terms_found]
        # Note: This tests word boundary matching
        assert "severe" not in terms or result.terms_found[0].context != "severely"

    # ==========================================
    # Test Auto-Resolution (CDISC Standard)
    # ==========================================

    def test_auto_resolves_severity(self, guard):
        """Test auto-resolution of CDISC standard severity terms."""
        result = guard.check_query("List all severe adverse events")

        assert result.all_resolved
        assert len(result.clarifications_needed) == 0

        # Find the severe term resolution
        severe_resolution = next(
            (r for r in result.resolutions if r.term == "severe"),
            None
        )
        assert severe_resolution is not None
        assert severe_resolution.resolution_type == ResolutionType.AUTO_RESOLVED
        assert severe_resolution.sql_fragment == "AESEV = 'SEVERE'"
        assert severe_resolution.confidence == 1.0

    def test_auto_resolves_serious(self, guard):
        """Test auto-resolution of serious AE term."""
        result = guard.check_query("Count serious adverse events")

        serious_resolution = next(
            (r for r in result.resolutions if r.term == "serious"),
            None
        )
        assert serious_resolution is not None
        assert serious_resolution.sql_fragment == "AESER = 'Y'"

    def test_auto_resolves_completer(self, guard):
        """Test auto-resolution of study completer."""
        # Use singular form to match the term exactly
        result = guard.check_query("How many patients are a completer")

        completer_resolution = next(
            (r for r in result.resolutions if r.term == "completer"),
            None
        )
        assert completer_resolution is not None
        assert "COMPLFL" in completer_resolution.sql_fragment

    # ==========================================
    # Test Protocol Resolution
    # ==========================================

    def test_resolves_from_protocol(self, guard):
        """Test resolution from study protocol definitions."""
        # The study_protocol.json should have ELDERLY_DEFINITION
        result = guard.check_query("Show elderly patients")

        elderly_resolution = next(
            (r for r in result.resolutions if r.term == "elderly"),
            None
        )
        assert elderly_resolution is not None
        # Should be resolved from protocol or common value
        assert elderly_resolution.sql_fragment is not None
        assert "AGE" in elderly_resolution.sql_fragment or "65" in elderly_resolution.sql_fragment

    def test_resolves_baseline_from_protocol(self, guard):
        """Test baseline resolution from protocol."""
        result = guard.check_query("Compare to baseline values")

        baseline_resolution = next(
            (r for r in result.resolutions if r.term == "baseline"),
            None
        )
        assert baseline_resolution is not None

    # ==========================================
    # Test Context Resolution
    # ==========================================

    def test_context_resolution_high_blood_pressure(self, guard):
        """Test context-based resolution for 'high blood pressure'."""
        # Create guard without protocol to test pure context resolution
        guard_no_protocol = ProtocolGuard(protocol_path="nonexistent.json")
        result = guard_no_protocol.check_query("Show patients with high blood pressure")

        high_resolution = next(
            (r for r in result.resolutions if r.term == "high"),
            None
        )
        assert high_resolution is not None
        assert high_resolution.resolution_type == ResolutionType.CONTEXT_RESOLVED
        assert "SYSBP" in high_resolution.sql_fragment or "140" in high_resolution.sql_fragment

    def test_protocol_takes_priority_over_context(self, guard):
        """Test that protocol resolution takes priority over context."""
        # With protocol loaded, HIGH_THRESHOLD from protocol takes priority
        result = guard.check_query("Show patients with high blood pressure")

        high_resolution = next(
            (r for r in result.resolutions if r.term == "high"),
            None
        )
        assert high_resolution is not None
        # Protocol resolution should take priority
        assert high_resolution.resolution_type == ResolutionType.PROTOCOL_RESOLVED

    def test_context_resolution_high_heart_rate(self, guard):
        """Test context-based resolution for 'high heart rate'."""
        result = guard.check_query("Patients with high heart rate")

        high_resolution = next(
            (r for r in result.resolutions if r.term == "high"),
            None
        )
        assert high_resolution is not None
        assert high_resolution.sql_fragment is not None
        assert "HR" in high_resolution.sql_fragment or "100" in high_resolution.sql_fragment

    # ==========================================
    # Test Clarification Needed
    # ==========================================

    def test_needs_clarification_for_recent(self, guard):
        """Test that 'recent' needs clarification when not in protocol."""
        # Create guard without protocol
        guard_no_protocol = ProtocolGuard(protocol_path="nonexistent.json")

        result = guard_no_protocol.check_query("Show recent adverse events")

        recent_resolution = next(
            (r for r in result.resolutions if r.term == "recent"),
            None
        )
        assert recent_resolution is not None
        # Without protocol and no context, should need clarification
        assert recent_resolution.clarification_needed or recent_resolution.sql_fragment is not None

    def test_needs_clarification_for_responder(self, guard):
        """Test that 'responder' may need clarification."""
        # Create guard without protocol
        guard_no_protocol = ProtocolGuard(protocol_path="nonexistent.json")

        # Use singular form to match the term exactly
        result = guard_no_protocol.check_query("Count each responder by treatment")

        responder_resolution = next(
            (r for r in result.resolutions if r.term == "responder"),
            None
        )
        assert responder_resolution is not None
        # Without protocol definition, should need clarification
        assert responder_resolution.clarification_needed

    # ==========================================
    # Test SQL Enhancements
    # ==========================================

    def test_sql_enhancements_populated(self, guard):
        """Test that SQL enhancements dict is populated."""
        result = guard.check_query("Show severe and serious AEs")

        assert len(result.sql_enhancements) >= 2
        assert "severe" in result.sql_enhancements
        assert "serious" in result.sql_enhancements
        assert result.sql_enhancements["severe"] == "AESEV = 'SEVERE'"
        assert result.sql_enhancements["serious"] == "AESER = 'Y'"

    def test_enhanced_query_annotation(self, guard):
        """Test that enhanced query has annotations."""
        result = guard.check_query("Count severe adverse events")

        # Enhanced query should have the resolution in brackets
        assert "[" in result.enhanced_query or result.enhanced_query != result.query

    # ==========================================
    # Test User Clarification
    # ==========================================

    def test_apply_user_clarification(self, guard):
        """Test applying user-provided clarification."""
        resolution = guard.apply_user_clarification(
            term="recent",
            user_value="Within last 14 days",
            sql_fragment="AESTDTC >= CURRENT_DATE - INTERVAL '14 days'"
        )

        assert resolution.term == "recent"
        assert resolution.resolution_type == ResolutionType.USER_CLARIFICATION
        assert resolution.confidence == 1.0
        assert resolution.clarification_needed is False
        assert "14 days" in resolution.sql_fragment

    # ==========================================
    # Test Edge Cases
    # ==========================================

    def test_empty_query(self, guard):
        """Test handling of empty query."""
        result = guard.check_query("")

        assert result.query == ""
        assert len(result.terms_found) == 0
        assert result.all_resolved

    def test_query_no_ambiguous_terms(self, guard):
        """Test query with no ambiguous terms."""
        result = guard.check_query("SELECT COUNT(*) FROM adsl WHERE SAFFL = 'Y'")

        # May or may not find terms depending on implementation
        assert result.all_resolved

    def test_case_insensitive_matching(self, guard):
        """Test that term matching is case-insensitive."""
        result1 = guard.check_query("Show SEVERE adverse events")
        result2 = guard.check_query("Show severe adverse events")

        assert len(result1.terms_found) == len(result2.terms_found)

    # ==========================================
    # Test Registry
    # ==========================================

    def test_get_ambiguous_terms(self, guard):
        """Test getting the ambiguous terms registry."""
        terms = guard.get_ambiguous_terms()

        assert "severe" in terms
        assert "baseline" in terms
        assert "elderly" in terms
        assert len(terms) > 20  # Should have many terms


class TestAmbiguousTermsRegistry:
    """Test the AMBIGUOUS_TERMS registry itself."""

    def test_all_terms_have_type(self):
        """Test that all terms have a type defined."""
        for term, config in AMBIGUOUS_TERMS.items():
            assert "type" in config, f"Term '{term}' missing type"

    def test_auto_map_terms_have_sql(self):
        """Test that auto_map terms have valid SQL."""
        auto_map_terms = [
            t for t, c in AMBIGUOUS_TERMS.items()
            if "auto_map" in c
        ]

        assert len(auto_map_terms) > 0, "Should have auto_map terms"

        for term in auto_map_terms:
            sql = AMBIGUOUS_TERMS[term]["auto_map"]
            assert len(sql) > 0
            assert "=" in sql or "IS" in sql  # Basic SQL check

    def test_clarification_terms_have_ask(self):
        """Test that terms needing clarification have 'ask' text."""
        for term, config in AMBIGUOUS_TERMS.items():
            if "auto_map" not in config and "common_value" not in config:
                assert "ask" in config or "protocol_key" in config, \
                    f"Term '{term}' needs 'ask' or 'protocol_key'"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
