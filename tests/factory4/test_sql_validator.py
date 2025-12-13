# Tests for SQL Validator
"""
Test Suite for SQL Validator
============================
Tests SQL validation including:
- Dangerous operation blocking
- SQL injection pattern detection
- Table/column verification
- Query complexity checks
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.engine.sql_validator import SQLValidator, ValidatorConfig


class TestBasicValidation:
    """Test basic SQL validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.available_tables = {
            'ADAE': ['USUBJID', 'AEDECOD', 'ATOXGR', 'SAFFL'],
            'ADSL': ['USUBJID', 'AGE', 'SEX', 'SAFFL']
        }
        self.validator = SQLValidator(
            available_tables=self.available_tables,
            config=ValidatorConfig()
        )

    def test_valid_select_query(self):
        """Test valid SELECT query passes."""
        sql = "SELECT COUNT(DISTINCT USUBJID) FROM ADAE WHERE SAFFL = 'Y'"
        result = self.validator.validate(sql)
        assert result.is_valid is True

    def test_empty_query(self):
        """Test empty query fails."""
        result = self.validator.validate("")
        assert result.is_valid is False
        assert "Empty SQL query" in result.errors[0]

    def test_whitespace_only_query(self):
        """Test whitespace-only query fails."""
        result = self.validator.validate("   ")
        assert result.is_valid is False

    def test_non_select_query_blocked(self):
        """Test non-SELECT queries are blocked."""
        result = self.validator.validate("SHOW TABLES")
        assert result.is_valid is False
        assert "Only SELECT queries are allowed" in result.errors[0]


class TestDangerousOperations:
    """Test dangerous operation blocking."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = SQLValidator(config=ValidatorConfig())

    def test_delete_blocked(self):
        """Test DELETE is blocked."""
        result = self.validator.validate("DELETE FROM ADAE WHERE 1=1")
        assert result.is_valid is False
        assert "DELETE" in result.errors[0].upper()

    def test_update_blocked(self):
        """Test UPDATE is blocked."""
        result = self.validator.validate("UPDATE ADAE SET AEDECOD='TEST'")
        assert result.is_valid is False
        assert "UPDATE" in result.errors[0].upper()

    def test_drop_blocked(self):
        """Test DROP is blocked."""
        result = self.validator.validate("DROP TABLE ADAE")
        assert result.is_valid is False
        assert "DROP" in result.errors[0].upper()

    def test_insert_blocked(self):
        """Test INSERT is blocked."""
        result = self.validator.validate("INSERT INTO ADAE VALUES (1,2,3)")
        assert result.is_valid is False
        assert "INSERT" in result.errors[0].upper()

    def test_truncate_blocked(self):
        """Test TRUNCATE is blocked."""
        result = self.validator.validate("TRUNCATE TABLE ADAE")
        assert result.is_valid is False
        assert "TRUNCATE" in result.errors[0].upper()

    def test_alter_blocked(self):
        """Test ALTER is blocked."""
        result = self.validator.validate("ALTER TABLE ADAE ADD COLUMN test")
        assert result.is_valid is False
        assert "ALTER" in result.errors[0].upper()

    def test_create_blocked(self):
        """Test CREATE is blocked."""
        result = self.validator.validate("CREATE TABLE test (id INT)")
        assert result.is_valid is False
        assert "CREATE" in result.errors[0].upper()

    def test_exec_blocked(self):
        """Test EXEC is blocked."""
        result = self.validator.validate("EXEC sp_executesql")
        assert result.is_valid is False
        assert "EXEC" in result.errors[0].upper()

    def test_info_schema_blocked(self):
        """Test INFORMATION_SCHEMA is blocked."""
        result = self.validator.validate("SELECT * FROM INFORMATION_SCHEMA.TABLES")
        assert result.is_valid is False
        assert "INFO_SCHEMA" in result.errors[0].upper()


class TestSQLInjectionPatterns:
    """Test SQL injection pattern detection."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = SQLValidator(config=ValidatorConfig())

    def test_comment_injection(self):
        """Test comment injection detection."""
        result = self.validator.validate("SELECT * FROM ADAE -- DROP TABLE")
        assert result.is_valid is False
        # Could be blocked for comment injection or DROP detection
        assert len(result.errors) > 0

    def test_union_attack(self):
        """Test UNION attack detection."""
        result = self.validator.validate("SELECT * FROM ADAE WHERE AEDECOD='' UNION SELECT * FROM users")
        assert result.is_valid is False

    def test_semicolon_injection(self):
        """Test semicolon injection detection."""
        result = self.validator.validate("SELECT * FROM ADAE; DROP TABLE users")
        assert result.is_valid is False

    def test_hex_encoding(self):
        """Test hex encoding detection."""
        result = self.validator.validate("SELECT * FROM ADAE WHERE AEDECOD=0x48454144414348")
        assert result.is_valid is False

    def test_char_encoding(self):
        """Test CHAR encoding detection."""
        result = self.validator.validate("SELECT * FROM ADAE WHERE AEDECOD=CHAR(72)")
        assert result.is_valid is False


class TestTableVerification:
    """Test table existence verification."""

    def setup_method(self):
        """Set up test fixtures."""
        self.available_tables = {
            'ADAE': ['USUBJID', 'AEDECOD', 'ATOXGR', 'SAFFL'],
            'ADSL': ['USUBJID', 'AGE', 'SEX', 'SAFFL']
        }
        self.validator = SQLValidator(
            available_tables=self.available_tables,
            config=ValidatorConfig()
        )

    def test_valid_table(self):
        """Test valid table passes."""
        result = self.validator.validate("SELECT * FROM ADAE")
        assert result.is_valid is True
        assert "ADAE" in result.tables_verified

    def test_invalid_table(self):
        """Test invalid table fails."""
        result = self.validator.validate("SELECT * FROM NONEXISTENT")
        assert result.is_valid is False
        assert "Table not found" in result.errors[0]

    def test_table_case_insensitive(self):
        """Test table name is case insensitive."""
        result = self.validator.validate("SELECT * FROM adae")
        assert result.is_valid is True

    def test_join_tables_verified(self):
        """Test JOIN tables are verified."""
        sql = "SELECT * FROM ADAE JOIN ADSL ON ADAE.USUBJID = ADSL.USUBJID"
        result = self.validator.validate(sql)
        assert result.is_valid is True
        assert "ADAE" in result.tables_verified
        assert "ADSL" in result.tables_verified


class TestQueryComplexity:
    """Test query complexity checks."""

    def setup_method(self):
        """Set up test fixtures."""
        self.available_tables = {
            'ADAE': ['USUBJID', 'AEDECOD'],
            'ADSL': ['USUBJID', 'AGE'],
            'ADLB': ['USUBJID', 'AVAL'],
            'ADCM': ['USUBJID', 'CMTRT'],
            'ADVS': ['USUBJID', 'AVAL'],
            'ADEX': ['USUBJID', 'EXDOSE']
        }
        self.validator = SQLValidator(
            available_tables=self.available_tables,
            config=ValidatorConfig(max_joins=3)
        )

    def test_acceptable_joins(self):
        """Test acceptable number of joins passes."""
        sql = """SELECT * FROM ADAE
                 JOIN ADSL ON ADAE.USUBJID = ADSL.USUBJID
                 JOIN ADLB ON ADAE.USUBJID = ADLB.USUBJID"""
        result = self.validator.validate(sql)
        assert result.is_valid is True

    def test_too_many_joins_warning(self):
        """Test too many joins generates warning."""
        sql = """SELECT * FROM ADAE
                 JOIN ADSL ON ADAE.USUBJID = ADSL.USUBJID
                 JOIN ADLB ON ADAE.USUBJID = ADLB.USUBJID
                 JOIN ADCM ON ADAE.USUBJID = ADCM.USUBJID
                 JOIN ADVS ON ADAE.USUBJID = ADVS.USUBJID"""
        result = self.validator.validate(sql)
        # Should still be valid but with warning
        assert result.is_valid is True
        assert any("joins" in w.lower() for w in result.warnings)


class TestLimitHandling:
    """Test LIMIT handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.available_tables = {
            'ADAE': ['USUBJID', 'AEDECOD', 'SAFFL']
        }
        self.validator = SQLValidator(
            available_tables=self.available_tables,
            config=ValidatorConfig(max_limit=1000)
        )

    def test_limit_added_when_missing(self):
        """Test LIMIT is added when missing."""
        sql = "SELECT * FROM ADAE"
        result = self.validator.validate(sql)
        assert result.is_valid is True
        assert "LIMIT" in result.validated_sql

    def test_existing_limit_preserved(self):
        """Test existing LIMIT is preserved."""
        sql = "SELECT * FROM ADAE LIMIT 100"
        result = self.validator.validate(sql)
        assert result.is_valid is True
        assert "LIMIT 100" in result.validated_sql

    def test_limit_warning_added(self):
        """Test warning is added when LIMIT is added."""
        sql = "SELECT * FROM ADAE"
        result = self.validator.validate(sql)
        assert any("LIMIT" in w for w in result.warnings)


class TestConfigurationOptions:
    """Test configuration options."""

    def test_allow_delete(self):
        """Test allowing DELETE when configured."""
        config = ValidatorConfig(block_delete=False)
        validator = SQLValidator(config=config)
        result = validator.validate("DELETE FROM ADAE")
        # DELETE should not be blocked, but query still fails (not SELECT)
        assert "DELETE" not in str(result.errors).upper() or "Only SELECT" in str(result.errors)

    def test_allow_info_schema(self):
        """Test allowing INFORMATION_SCHEMA when configured."""
        config = ValidatorConfig(block_info_schema=False)
        validator = SQLValidator(config=config)
        result = validator.validate("SELECT * FROM INFORMATION_SCHEMA.TABLES")
        # INFO_SCHEMA should not be blocked
        assert "INFO_SCHEMA" not in str(result.errors).upper()


class TestQuickValidate:
    """Test quick validation method."""

    def setup_method(self):
        """Set up test fixtures."""
        # Provide tables for validation
        self.available_tables = {'ADAE': ['USUBJID', 'AEDECOD']}
        self.validator = SQLValidator(
            available_tables=self.available_tables,
            config=ValidatorConfig()
        )

    def test_quick_validate_valid(self):
        """Test quick_validate returns True for valid query."""
        assert self.validator.quick_validate("SELECT * FROM ADAE") is True

    def test_quick_validate_invalid(self):
        """Test quick_validate returns False for invalid query."""
        assert self.validator.quick_validate("DROP TABLE ADAE") is False
