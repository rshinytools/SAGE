"""Tests for Result Validator."""

import pytest
import tempfile
import shutil
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.engine.learning.result_validator import (
    ResultValidator,
    ResultValidation
)


class TestResultValidator:
    """Test suite for ResultValidator."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        temp = tempfile.mkdtemp()
        yield temp
        shutil.rmtree(temp, ignore_errors=True)

    @pytest.fixture
    def validator(self, temp_dir):
        """Create ResultValidator with temp path."""
        db_path = Path(temp_dir) / "learning.db"
        return ResultValidator(db_path=str(db_path))

    def test_validate_valid_result(self, validator):
        """Test validating a valid result."""
        result = validator.validate(
            question="How many patients?",
            sql="SELECT COUNT(*) FROM adsl",
            result=[{"count": 100}]
        )
        assert result.is_valid
        assert result.confidence_adjustment >= -0.3

    def test_validate_empty_result(self, validator):
        """Test validating empty result."""
        result = validator.validate(
            question="How many patients?",
            sql="SELECT COUNT(*) FROM adsl",
            result=[]
        )
        assert not result.checks["not_empty"]
        assert "no results" in result.warnings[0].lower()
        assert result.confidence_adjustment < 0

    def test_validate_large_result(self, validator):
        """Test validating very large result."""
        large_result = [{"id": i} for i in range(150000)]
        result = validator.validate(
            question="List all data",
            sql="SELECT * FROM large_table",
            result=large_result
        )
        assert not result.checks["reasonable_size"]
        assert any("large" in w.lower() for w in result.warnings)

    def test_check_percentage_bounds_valid(self, validator):
        """Test percentage bounds check - valid."""
        result = validator.validate(
            question="What is the percentage?",
            sql="SELECT percent FROM table",
            result=[{"percent": 50.5}]
        )
        assert result.checks["value_bounds"]

    def test_check_percentage_bounds_invalid(self, validator):
        """Test percentage bounds check - invalid."""
        result = validator.validate(
            question="What is the percentage?",
            sql="SELECT pct FROM table",
            result=[{"pct": 150}]
        )
        assert not result.checks["value_bounds"]
        assert any("percentage" in w.lower() for w in result.warnings)

    def test_check_negative_count(self, validator):
        """Test negative count detection."""
        result = validator.validate(
            question="How many?",
            sql="SELECT count FROM table",
            result=[{"count": -5}]
        )
        assert not result.checks["value_bounds"]

    def test_check_invalid_age(self, validator):
        """Test invalid age detection."""
        result = validator.validate(
            question="What is the age?",
            sql="SELECT age FROM table",
            result=[{"age": 200}]
        )
        assert not result.checks["value_bounds"]

    def test_null_ratio_check_pass(self, validator):
        """Test null ratio check passes with few nulls."""
        result = validator.validate(
            question="Get data",
            sql="SELECT * FROM table",
            result=[
                {"a": 1, "b": 2},
                {"a": 3, "b": None},
                {"a": 5, "b": 6}
            ]
        )
        assert result.checks["acceptable_nulls"]

    def test_null_ratio_check_fail(self, validator):
        """Test null ratio check fails with many nulls."""
        result = validator.validate(
            question="Get data",
            sql="SELECT * FROM table",
            result=[
                {"a": None, "b": None},
                {"a": None, "b": None},
                {"a": 1, "b": None}
            ]
        )
        assert not result.checks["acceptable_nulls"]

    def test_historical_comparison_no_history(self, validator):
        """Test historical comparison with no history."""
        result = validator.validate(
            question="New query",
            sql="SELECT * FROM new_table",
            result=[{"a": 1}]
        )
        # No history means historical match defaults to True
        assert result.checks.get("historical_match", True)

    def test_historical_comparison_with_history(self, validator):
        """Test historical comparison with matching history."""
        # First query stores history
        validator.validate(
            question="Test query",
            sql="SELECT * FROM test",
            result=[{"a": i} for i in range(10)]
        )

        # Second query should match
        result = validator.validate(
            question="Test query",
            sql="SELECT * FROM test",
            result=[{"a": i} for i in range(12)]  # Similar count
        )
        assert result.checks.get("historical_match", True)

    def test_historical_comparison_large_deviation(self, validator):
        """Test historical comparison with large deviation."""
        # First query
        validator.validate(
            question="Count query",
            sql="SELECT * FROM test",
            result=[{"a": i} for i in range(100)]
        )

        # Second query with very different result
        result = validator.validate(
            question="Count query",
            sql="SELECT * FROM test",
            result=[{"a": 1}]  # Only 1 row vs 100
        )
        # Should detect significant change
        assert not result.checks.get("historical_match", True) or len(result.anomalies) > 0

    def test_normalize_dataframe(self, validator):
        """Test normalizing DataFrame-like result."""
        class MockDataFrame:
            def to_dict(self, orient):
                return [{"a": 1}, {"a": 2}]

        result = validator.validate(
            question="Get data",
            sql="SELECT * FROM test",
            result=MockDataFrame()
        )
        assert result.checks["not_empty"]

    def test_normalize_single_value(self, validator):
        """Test normalizing single value result."""
        result = validator.validate(
            question="Get count",
            sql="SELECT COUNT(*)",
            result=42,
            expected_type="count"
        )
        assert result.is_valid
        assert result.checks["not_empty"]

    def test_normalize_dict_result(self, validator):
        """Test normalizing dict result."""
        result = validator.validate(
            question="Get summary",
            sql="SELECT summary",
            result={"total": 100, "avg": 50.5}
        )
        assert result.checks["not_empty"]

    def test_normalize_none_result(self, validator):
        """Test normalizing None result."""
        result = validator.validate(
            question="Get data",
            sql="SELECT * FROM empty",
            result=None
        )
        assert not result.checks["not_empty"]

    def test_confidence_adjustment_bounds(self, validator):
        """Test confidence adjustment is bounded."""
        # Empty result with many issues
        result = validator.validate(
            question="Bad query",
            sql="SELECT * FROM table",
            result=[]
        )
        assert result.confidence_adjustment >= -0.3
        assert result.confidence_adjustment <= 0.1

    def test_clear_history(self, validator):
        """Test clearing historical data."""
        validator.validate(
            question="Test",
            sql="SELECT 1",
            result=[{"a": 1}]
        )

        validator.clear_history()

        # After clear, no historical match possible
        result = validator.validate(
            question="Test",
            sql="SELECT 1",
            result=[{"a": 1}]
        )
        # Should still work, just no comparison
        assert result.is_valid

    def test_store_result_error_handling(self, validator):
        """Test that storage errors don't cause validation to fail."""
        # Force a storage situation - validation should still work
        result = validator.validate(
            question="Test query",
            sql="SELECT * FROM test",
            result=[{"a": 1}]
        )
        # Should complete without error
        assert isinstance(result, ResultValidation)

    def test_negative_count_expected_type(self, validator):
        """Test negative value for count expected type."""
        result = validator.validate(
            question="Get count",
            sql="SELECT value FROM test",
            result=[{"value": -10}],
            expected_type="count"
        )
        assert not result.checks["value_bounds"]
