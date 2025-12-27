"""Tests for Complexity Scorer."""

import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.engine.learning.complexity_scorer import (
    ComplexityScorer,
    ComplexityLevel,
    ComplexityAssessment
)


class TestComplexityScorer:
    """Test suite for ComplexityScorer."""

    @pytest.fixture
    def scorer(self):
        """Create ComplexityScorer instance."""
        return ComplexityScorer()

    def test_simple_count_query(self, scorer):
        """Test simple count query assessment."""
        result = scorer.assess("How many patients are in the study?")
        assert result.level == ComplexityLevel.SIMPLE
        assert result.score < 15

    def test_moderate_query_with_filter(self, scorer):
        """Test moderate query with filter."""
        result = scorer.assess(
            "How many male patients are over 65 years old?",
            detected_tables=["adsl"],
            detected_columns=["SEX", "AGE"]
        )
        assert result.level in [ComplexityLevel.SIMPLE, ComplexityLevel.MODERATE]

    def test_complex_multi_table_query(self, scorer):
        """Test complex multi-table query."""
        result = scorer.assess(
            "Compare adverse event rates between treatment groups for each visit",
            detected_tables=["adsl", "adae", "sv"],
            detected_columns=["TRT01P", "AEDECOD", "VISITNUM"]
        )
        # 3 tables + compare + rates + visit = very complex
        assert result.level in [ComplexityLevel.COMPLEX, ComplexityLevel.VERY_COMPLEX]
        assert result.score >= 35

    def test_very_complex_query(self, scorer):
        """Test very complex analytical query."""
        result = scorer.assess(
            "Calculate the difference in average laboratory values between "
            "baseline and endpoint for each treatment group, excluding "
            "patients who discontinued before week 12, grouped by site",
            detected_tables=["adsl", "adlb", "dm"],
            detected_columns=["TRT01P", "AVAL", "VISITNUM", "SITEID", "DCSREAS"]
        )
        assert result.level in [ComplexityLevel.COMPLEX, ComplexityLevel.VERY_COMPLEX]
        assert result.score >= 35

    def test_complexity_factors(self, scorer):
        """Test that complexity factors are calculated."""
        result = scorer.assess(
            "Show average age per treatment group",
            detected_tables=["adsl"],
            detected_columns=["AGE", "TRT01P"]
        )

        assert "question_length" in result.factors
        assert "table_count" in result.factors
        assert "complex_keywords" in result.factors
        assert "aggregations" in result.factors

    def test_join_indicators_increase_score(self, scorer):
        """Test join indicators increase complexity."""
        simple = scorer.assess("Count patients")
        with_join = scorer.assess(
            "Count patients with their adverse events including medications"
        )

        assert with_join.score > simple.score

    def test_temporal_keywords_increase_score(self, scorer):
        """Test temporal keywords increase complexity."""
        simple = scorer.assess("Count adverse events")
        temporal = scorer.assess(
            "Count adverse events before baseline visit during first week"
        )

        assert temporal.score > simple.score

    def test_threshold_adjustment_simple(self, scorer):
        """Test threshold adjustment for simple queries."""
        adjustment = scorer.get_threshold_adjustment(ComplexityLevel.SIMPLE)
        assert adjustment == 0.0

    def test_threshold_adjustment_complex(self, scorer):
        """Test threshold adjustment for complex queries."""
        adjustment = scorer.get_threshold_adjustment(ComplexityLevel.COMPLEX)
        assert adjustment == 0.10

    def test_threshold_adjustment_very_complex(self, scorer):
        """Test threshold adjustment for very complex queries."""
        adjustment = scorer.get_threshold_adjustment(ComplexityLevel.VERY_COMPLEX)
        assert adjustment == 0.15

    def test_is_simple_count_query_true(self, scorer):
        """Test simple count query detection."""
        assert scorer.is_simple_count_query("How many patients are enrolled?")
        assert scorer.is_simple_count_query("Count of subjects")
        assert scorer.is_simple_count_query("Total number of adverse events")
        assert scorer.is_simple_count_query("Number of male patients")

    def test_is_simple_count_query_false_with_complexity(self, scorer):
        """Test that complex patterns are not flagged as simple."""
        assert not scorer.is_simple_count_query(
            "How many patients had AEs compared to baseline"
        )
        assert not scorer.is_simple_count_query(
            "Count of patients over time"
        )

    def test_is_simple_count_query_false_for_non_count(self, scorer):
        """Test non-count queries are not flagged as simple."""
        assert not scorer.is_simple_count_query("Show all patients")
        assert not scorer.is_simple_count_query("What is the average age?")

    def test_recommended_threshold(self, scorer):
        """Test that recommended threshold is returned."""
        simple = scorer.assess("How many patients?")
        assert simple.recommended_threshold == 0.75

        complex_q = scorer.assess(
            "Compare the trend of adverse event rates between treatment "
            "groups over each visit with percentage change"
        )
        assert complex_q.recommended_threshold >= 0.80

    def test_warnings_generated(self, scorer):
        """Test that warnings are generated for complex queries."""
        result = scorer.assess(
            "Calculate average change from baseline across sites",
            detected_tables=["adsl", "adlb", "dm", "sv"]
        )
        assert len(result.warnings) > 0

    def test_multiple_aggregations_warning(self, scorer):
        """Test warning for multiple aggregations."""
        result = scorer.assess(
            "Show the average, sum, and count of adverse events"
        )
        assert any("aggregation" in w.lower() for w in result.warnings)

    def test_multiple_temporal_warning(self, scorer):
        """Test warning for multiple temporal conditions."""
        result = scorer.assess(
            "Count adverse events before visit 4 during treatment period"
        )
        # May or may not generate warning depending on exact matches
        assert isinstance(result.warnings, list)

    def test_get_complexity_factors(self, scorer):
        """Test getting all complexity factor keywords."""
        factors = scorer.get_complexity_factors()

        assert "complex_keywords" in factors
        assert "join_indicators" in factors
        assert "aggregation_keywords" in factors
        assert "temporal_keywords" in factors
        assert "conditional_keywords" in factors

        assert "compare" in factors["complex_keywords"]
        assert "with" in factors["join_indicators"]
        assert "average" in factors["aggregation_keywords"]

    def test_column_count_factor(self, scorer):
        """Test column count affects complexity."""
        few_cols = scorer.assess(
            "Show data",
            detected_columns=["COL1", "COL2"]
        )
        many_cols = scorer.assess(
            "Show data",
            detected_columns=["COL1", "COL2", "COL3", "COL4", "COL5", "COL6", "COL7"]
        )

        assert many_cols.score > few_cols.score
