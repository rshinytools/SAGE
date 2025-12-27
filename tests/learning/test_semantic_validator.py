"""Tests for Semantic Validator."""

import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.engine.learning.semantic_validator import (
    SemanticValidator,
    SemanticValidation,
    ValidationResult
)


class TestSemanticValidator:
    """Test suite for SemanticValidator."""

    @pytest.fixture
    def validator(self):
        """Create SemanticValidator instance."""
        return SemanticValidator()

    def test_valid_count_query(self, validator):
        """Test valid count query validation."""
        result = validator.validate(
            sql="SELECT COUNT(*) FROM adsl",
            intent="count patients"
        )
        assert result.result == ValidationResult.VALID
        assert result.score >= 0.9

    def test_valid_list_query(self, validator):
        """Test valid list query validation."""
        result = validator.validate(
            sql="SELECT USUBJID, AGE, SEX FROM adsl ORDER BY AGE LIMIT 10",
            intent="list patients"
        )
        assert result.result == ValidationResult.VALID

    def test_valid_average_query(self, validator):
        """Test valid average query validation."""
        result = validator.validate(
            sql="SELECT AVG(AGE) FROM adsl",
            intent="average age"
        )
        assert result.result == ValidationResult.VALID

    def test_valid_sum_query(self, validator):
        """Test valid sum query validation."""
        result = validator.validate(
            sql="SELECT SUM(AVAL) FROM adlb GROUP BY TRT01P",
            intent="sum values"
        )
        assert result.result == ValidationResult.VALID

    def test_valid_max_query(self, validator):
        """Test valid max query validation."""
        result = validator.validate(
            sql="SELECT MAX(AGE) FROM adsl",
            intent="maximum age"
        )
        assert result.result == ValidationResult.VALID

    def test_valid_min_query(self, validator):
        """Test valid min query validation."""
        result = validator.validate(
            sql="SELECT MIN(AGE) FROM adsl",
            intent="minimum age"
        )
        assert result.result == ValidationResult.VALID

    def test_missing_count_function(self, validator):
        """Test missing COUNT function for count intent."""
        result = validator.validate(
            sql="SELECT * FROM adsl",
            intent="count patients"
        )
        assert result.result in [ValidationResult.WARNING, ValidationResult.INVALID]
        assert len(result.issues) > 0

    def test_missing_where_for_filter(self, validator):
        """Test missing WHERE for filter intent."""
        result = validator.validate(
            sql="SELECT * FROM adsl",
            intent="filter by treatment"
        )
        assert result.result in [ValidationResult.WARNING, ValidationResult.INVALID]

    def test_required_tables_present(self, validator):
        """Test that required tables are validated."""
        result = validator.validate(
            sql="SELECT COUNT(*) FROM adsl WHERE SEX = 'M'",
            intent="count",
            expected_tables=["adsl"]
        )
        assert result.checks.get("required_tables", True)

    def test_required_tables_missing(self, validator):
        """Test that missing tables are detected."""
        result = validator.validate(
            sql="SELECT COUNT(*) FROM adsl",
            intent="count",
            expected_tables=["adsl", "adae"]
        )
        assert not result.checks.get("required_tables", True)
        assert any("missing" in issue.lower() for issue in result.issues)

    def test_required_columns_present(self, validator):
        """Test that required columns are validated."""
        result = validator.validate(
            sql="SELECT COUNT(*) FROM adsl WHERE AGE > 65 AND SEX = 'M'",
            intent="count",
            expected_columns=["AGE", "SEX"]
        )
        assert result.checks.get("required_columns", True)

    def test_required_columns_missing(self, validator):
        """Test that missing columns are detected."""
        result = validator.validate(
            sql="SELECT COUNT(*) FROM adsl WHERE AGE > 65",
            intent="count",
            expected_columns=["AGE", "SEX", "RACE"]
        )
        assert not result.checks.get("required_columns", True)

    def test_filter_values_present(self, validator):
        """Test that filter values are validated."""
        result = validator.validate(
            sql="SELECT * FROM adsl WHERE TRT01P = 'Placebo'",
            intent="filter",
            filter_values={"treatment": "Placebo"}
        )
        assert result.checks.get("filter_values", True)

    def test_filter_values_missing(self, validator):
        """Test that missing filter values are detected."""
        result = validator.validate(
            sql="SELECT * FROM adsl WHERE TRT01P = 'Active'",
            intent="filter",
            filter_values={"treatment": "Placebo"}
        )
        assert not result.checks.get("filter_values", True)

    def test_grouping_check(self, validator):
        """Test grouping check for aggregation."""
        result = validator.validate(
            sql="SELECT TRT01P, COUNT(*) FROM adsl GROUP BY TRT01P",
            intent="count by treatment"
        )
        assert result.checks.get("grouping", True)

    def test_missing_grouping(self, validator):
        """Test missing GROUP BY for aggregation."""
        result = validator.validate(
            sql="SELECT COUNT(*) FROM adsl",
            intent="count by treatment"
        )
        # Should detect missing GROUP BY for "count by" intent
        assert "grouping" in result.checks

    def test_dangerous_delete(self, validator):
        """Test dangerous DELETE detection."""
        result = validator.validate(
            sql="DELETE FROM adsl WHERE AGE < 18",
            intent="remove patients"
        )
        assert not result.checks.get("no_dangerous", True)
        assert any("DELETE" in str(issue) for issue in result.issues)

    def test_dangerous_drop(self, validator):
        """Test dangerous DROP detection."""
        result = validator.validate(
            sql="DROP TABLE adsl",
            intent="delete table"
        )
        assert not result.checks.get("no_dangerous", True)

    def test_dangerous_update(self, validator):
        """Test dangerous UPDATE detection."""
        result = validator.validate(
            sql="UPDATE adsl SET AGE = 0",
            intent="update age"
        )
        assert not result.checks.get("no_dangerous", True)

    def test_valid_syntax(self, validator):
        """Test basic syntax validation."""
        result = validator.validate(
            sql="SELECT COUNT(*) FROM adsl",
            intent="count"
        )
        assert result.checks.get("valid_syntax", True)

    def test_invalid_syntax_no_from(self, validator):
        """Test syntax validation catches missing FROM."""
        result = validator.validate(
            sql="SELECT COUNT(*)",
            intent="count"
        )
        assert not result.checks.get("valid_syntax", True)

    def test_invalid_syntax_no_select(self, validator):
        """Test syntax validation catches missing SELECT."""
        result = validator.validate(
            sql="FROM adsl",
            intent="list"
        )
        assert not result.checks.get("valid_syntax", True)

    def test_unbalanced_parentheses(self, validator):
        """Test unbalanced parentheses detection."""
        result = validator.validate(
            sql="SELECT COUNT(* FROM adsl",
            intent="count"
        )
        assert not result.checks.get("valid_syntax", True)

    def test_unbalanced_quotes(self, validator):
        """Test unbalanced quotes detection."""
        result = validator.validate(
            sql="SELECT * FROM adsl WHERE TRT01P = 'Placebo",
            intent="filter"
        )
        assert not result.checks.get("valid_syntax", True)

    def test_quick_validate_valid(self, validator):
        """Test quick validation for valid SQL."""
        assert validator.quick_validate("SELECT COUNT(*) FROM adsl")
        assert validator.quick_validate("SELECT * FROM adsl WHERE AGE > 65")

    def test_quick_validate_invalid(self, validator):
        """Test quick validation for invalid SQL."""
        assert not validator.quick_validate("FROM adsl SELECT *")
        assert not validator.quick_validate("DELETE FROM adsl")
        assert not validator.quick_validate("DROP TABLE adsl")
        assert not validator.quick_validate("SELECT *")

    def test_score_calculation(self, validator):
        """Test score calculation."""
        valid = validator.validate(
            sql="SELECT COUNT(*) FROM adsl",
            intent="count"
        )
        assert valid.score >= 0.8

        invalid = validator.validate(
            sql="DROP TABLE adsl; SELECT *",
            intent="count"
        )
        assert invalid.score < valid.score

    def test_suggestions_provided(self, validator):
        """Test that suggestions are provided for issues."""
        result = validator.validate(
            sql="SELECT * FROM adsl",
            intent="count patients"
        )
        if result.result != ValidationResult.VALID:
            assert len(result.suggestions) > 0
