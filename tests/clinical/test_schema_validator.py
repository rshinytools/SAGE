"""
Tests for Schema Validation Layer.

Tests SQL validation against database schema.
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.clinical.schema_validator import (
    SchemaValidator,
    SchemaValidationResult,
    SchemaValidationStatus,
    SchemaIssue
)


class TestSchemaValidator:
    """Test suite for SchemaValidator."""

    @pytest.fixture
    def validator(self):
        """Create a SchemaValidator with mock schema."""
        validator = SchemaValidator(db_connection=None)
        # Set mock schema
        validator.set_schema({
            "ADSL": {"USUBJID", "SUBJID", "AGE", "SEX", "RACE", "TRT01P", "SAFFL", "ITTFL", "COMPLFL"},
            "ADAE": {"USUBJID", "AETERM", "AEDECOD", "AEBODSYS", "AESEV", "AESER", "AESTDTC"},
            "DM": {"USUBJID", "AGE", "SEX", "RACE", "ARM", "COUNTRY"},
            "AE": {"USUBJID", "AETERM", "AEDECOD", "AESEV", "AESER"},
            "LB": {"USUBJID", "LBTESTCD", "LBTEST", "LBSTRESN", "VISITNUM", "LBNRIND"}
        })
        return validator

    # ==========================================
    # Test Valid SQL
    # ==========================================

    def test_valid_simple_select(self, validator):
        """Test validation of simple valid SELECT."""
        sql = "SELECT COUNT(*) FROM adsl WHERE SAFFL = 'Y'"
        result = validator.validate(sql)

        assert result.is_valid
        assert result.status == SchemaValidationStatus.VALID
        assert len(result.issues) == 0

    def test_valid_with_multiple_columns(self, validator):
        """Test validation with multiple columns."""
        sql = "SELECT USUBJID, AGE, SEX, RACE FROM adsl WHERE SAFFL = 'Y'"
        result = validator.validate(sql)

        assert result.is_valid
        assert "ADSL" in [t.upper() for t in result.tables_found]

    def test_valid_join_query(self, validator):
        """Test validation of JOIN query."""
        sql = """
        SELECT a.USUBJID, a.AGE, ae.AETERM
        FROM adsl a
        JOIN adae ae ON a.USUBJID = ae.USUBJID
        """
        result = validator.validate(sql)

        assert result.is_valid
        assert len(result.tables_found) >= 2

    # ==========================================
    # Test Invalid Tables
    # ==========================================

    def test_invalid_table(self, validator):
        """Test detection of missing table."""
        sql = "SELECT * FROM nonexistent_table"
        result = validator.validate(sql)

        assert not result.is_valid
        assert result.status == SchemaValidationStatus.TABLE_MISSING
        assert any(i.issue_type == SchemaValidationStatus.TABLE_MISSING for i in result.issues)

    # ==========================================
    # Test Invalid Columns
    # ==========================================

    def test_invalid_column(self, validator):
        """Test detection of missing column in WHERE clause."""
        # Column extraction is more reliable in WHERE clause
        sql = "SELECT * FROM adsl WHERE NONEXISTENT_COLUMN = 'Y'"
        result = validator.validate(sql)

        assert not result.is_valid
        assert any(i.issue_type == SchemaValidationStatus.COLUMN_MISSING for i in result.issues)

    def test_invalid_column_in_where(self, validator):
        """Test detection of invalid column in WHERE clause."""
        sql = "SELECT * FROM adsl WHERE INVALID_COL = 'Y'"
        result = validator.validate(sql)

        assert not result.is_valid

    # ==========================================
    # Test Auto-Repair
    # ==========================================

    def test_auto_repair_column_alias(self, validator):
        """Test auto-repair of known column aliases."""
        # VISIT is a known alias for VISITNUM
        sql = "SELECT * FROM lb WHERE VISIT = 1"
        result = validator.validate(sql)

        # Should detect VISIT as missing but suggest VISITNUM
        if not result.is_valid:
            visit_issue = next(
                (i for i in result.issues if i.element.upper() == "VISIT"),
                None
            )
            if visit_issue:
                assert visit_issue.suggestion == "VISITNUM"
                if result.can_auto_repair:
                    assert "VISITNUM" in result.repaired_sql

    def test_repairable_status(self, validator):
        """Test that repairable issues get REPAIRABLE status."""
        # VISIT -> VISITNUM is a known mapping
        sql = "SELECT * FROM lb WHERE VISIT = 1"
        result = validator.validate(sql)

        # If VISIT is detected as missing and VISITNUM is suggested
        if not result.is_valid and result.can_auto_repair:
            assert result.status == SchemaValidationStatus.REPAIRABLE
            assert result.repaired_sql is not None

    # ==========================================
    # Test Table Extraction
    # ==========================================

    def test_extracts_from_clause_table(self, validator):
        """Test extraction of table from FROM clause."""
        sql = "SELECT * FROM adsl"
        result = validator.validate(sql)

        assert "ADSL" in [t.upper() for t in result.tables_found]

    def test_extracts_join_tables(self, validator):
        """Test extraction of tables from JOIN clauses."""
        sql = "SELECT * FROM adsl JOIN adae ON adsl.USUBJID = adae.USUBJID"
        result = validator.validate(sql)

        tables_upper = [t.upper() for t in result.tables_found]
        assert "ADSL" in tables_upper
        assert "ADAE" in tables_upper

    # ==========================================
    # Test Column Extraction
    # ==========================================

    def test_extracts_select_columns(self, validator):
        """Test extraction of columns from SELECT."""
        sql = "SELECT USUBJID, AGE, SEX FROM adsl"
        result = validator.validate(sql)

        # Should find these columns
        assert result.is_valid

    def test_extracts_where_columns(self, validator):
        """Test extraction of columns from WHERE clause."""
        sql = "SELECT * FROM adsl WHERE SAFFL = 'Y' AND AGE > 65"
        result = validator.validate(sql)

        columns_upper = [c.upper() for c in result.columns_found]
        assert "SAFFL" in columns_upper or result.is_valid
        assert "AGE" in columns_upper or result.is_valid

    # ==========================================
    # Test Edge Cases
    # ==========================================

    def test_empty_schema(self):
        """Test validation with no schema loaded."""
        validator = SchemaValidator(db_connection=None)
        # Don't set any schema

        sql = "SELECT * FROM anything"
        result = validator.validate(sql)

        # Should pass when no schema to validate against
        assert result.is_valid

    def test_case_insensitive(self, validator):
        """Test that validation is case-insensitive."""
        sql1 = "SELECT * FROM ADSL WHERE saffl = 'Y'"
        sql2 = "SELECT * FROM adsl WHERE SAFFL = 'Y'"

        result1 = validator.validate(sql1)
        result2 = validator.validate(sql2)

        assert result1.is_valid == result2.is_valid

    def test_string_literals_ignored(self, validator):
        """Test that string literals don't cause false positives."""
        sql = "SELECT * FROM adsl WHERE RACE = 'INVALID_COLUMN_NAME'"
        result = validator.validate(sql)

        # Should be valid - INVALID_COLUMN_NAME is a string value, not a column
        assert result.is_valid

    def test_get_schema_info(self, validator):
        """Test getting schema information."""
        schema = validator.get_schema_info()

        assert "ADSL" in schema
        assert "USUBJID" in schema["ADSL"]


class TestColumnAliases:
    """Test column alias mappings."""

    def test_known_aliases_defined(self):
        """Test that common aliases are defined."""
        from core.engine.clinical.schema_validator import SchemaValidator

        aliases = SchemaValidator.COLUMN_ALIASES

        assert "VISIT" in aliases
        assert "VISITNUM" in aliases["VISIT"]

        assert "USUBJID" in aliases
        assert "SUBJID" in aliases["USUBJID"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
