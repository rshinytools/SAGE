# Tests for SchemaMapper
# =======================

import pytest
import tempfile
import os
import json
from pathlib import Path

import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import duckdb
from core.dictionary.schema_mapper import (
    SchemaMapper,
    SchemaMap,
    ColumnInfo,
    TableInfo,
    build_schema_map
)


class TestSchemaMapperBasic:
    """Basic SchemaMapper tests."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary DuckDB database with test data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            conn = duckdb.connect(db_path)

            # Create test tables
            conn.execute("""
                CREATE TABLE DM (
                    STUDYID VARCHAR,
                    USUBJID VARCHAR,
                    SEX VARCHAR,
                    AGE INTEGER,
                    RACE VARCHAR
                )
            """)
            conn.execute("""
                INSERT INTO DM VALUES
                ('STUDY001', 'SUBJ001', 'M', 45, 'WHITE'),
                ('STUDY001', 'SUBJ002', 'F', 32, 'BLACK')
            """)

            conn.execute("""
                CREATE TABLE AE (
                    STUDYID VARCHAR,
                    USUBJID VARCHAR,
                    AESEQ INTEGER,
                    AETERM VARCHAR,
                    AESEV VARCHAR
                )
            """)
            conn.execute("""
                INSERT INTO AE VALUES
                ('STUDY001', 'SUBJ001', 1, 'HEADACHE', 'MILD'),
                ('STUDY001', 'SUBJ001', 2, 'NAUSEA', 'MODERATE')
            """)

            conn.close()
            yield db_path

    def test_mapper_initialization(self, temp_db):
        """Test mapper initialization."""
        mapper = SchemaMapper(temp_db)
        assert mapper.db_path.exists()
        mapper.close()

    def test_mapper_initialization_missing_db(self):
        """Test initialization with missing database."""
        with pytest.raises(FileNotFoundError):
            SchemaMapper("/nonexistent/path.duckdb")

    def test_get_tables(self, temp_db):
        """Test getting table list."""
        with SchemaMapper(temp_db) as mapper:
            tables = mapper.get_tables()
            assert "DM" in tables
            assert "AE" in tables

    def test_get_table_schema(self, temp_db):
        """Test getting table schema."""
        with SchemaMapper(temp_db) as mapper:
            schema = mapper.get_table_schema("DM")
            col_names = [c["name"] for c in schema]
            assert "USUBJID" in col_names
            assert "SEX" in col_names
            assert "AGE" in col_names

    def test_get_row_count(self, temp_db):
        """Test getting row count."""
        with SchemaMapper(temp_db) as mapper:
            count = mapper.get_row_count("DM")
            assert count == 2

    def test_get_unique_count(self, temp_db):
        """Test getting unique value count."""
        with SchemaMapper(temp_db) as mapper:
            count = mapper.get_unique_count("DM", "SEX")
            assert count == 2  # M and F

    def test_get_sample_values(self, temp_db):
        """Test getting sample values."""
        with SchemaMapper(temp_db) as mapper:
            samples = mapper.get_sample_values("DM", "SEX", limit=5)
            assert len(samples) <= 5
            assert "M" in samples or "F" in samples


class TestSchemaMapBuilding:
    """Test schema map building."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary DuckDB database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            conn = duckdb.connect(db_path)

            conn.execute("""
                CREATE TABLE DM (
                    STUDYID VARCHAR,
                    USUBJID VARCHAR,
                    SEX VARCHAR
                )
            """)
            conn.execute("INSERT INTO DM VALUES ('S1', 'U1', 'M'), ('S1', 'U2', 'F')")

            conn.execute("""
                CREATE TABLE AE (
                    STUDYID VARCHAR,
                    USUBJID VARCHAR,
                    AETERM VARCHAR
                )
            """)
            conn.execute("INSERT INTO AE VALUES ('S1', 'U1', 'HEADACHE')")

            conn.close()
            yield db_path

    def test_build_schema_map(self, temp_db):
        """Test building complete schema map."""
        with SchemaMapper(temp_db) as mapper:
            schema_map = mapper.build_schema_map()

            assert isinstance(schema_map, SchemaMap)
            assert len(schema_map.tables) == 2
            assert len(schema_map.columns) > 0

    def test_schema_map_tables(self, temp_db):
        """Test table info in schema map."""
        with SchemaMapper(temp_db) as mapper:
            schema_map = mapper.build_schema_map()

            assert "DM" in schema_map.tables
            dm_table = schema_map.tables["DM"]
            assert dm_table.name == "DM"
            assert dm_table.row_count == 2
            assert "USUBJID" in dm_table.columns

    def test_schema_map_columns(self, temp_db):
        """Test column info in schema map."""
        with SchemaMapper(temp_db) as mapper:
            schema_map = mapper.build_schema_map()

            # USUBJID appears in both tables
            assert "USUBJID" in schema_map.columns
            usubjid = schema_map.columns["USUBJID"]
            assert "DM" in usubjid.tables
            assert "AE" in usubjid.tables

    def test_schema_map_key_detection(self, temp_db):
        """Test key column detection."""
        with SchemaMapper(temp_db) as mapper:
            schema_map = mapper.build_schema_map()

            usubjid = schema_map.columns["USUBJID"]
            assert usubjid.is_key is True

            sex = schema_map.columns["SEX"]
            assert sex.is_key is False

    def test_schema_map_domain_type(self, temp_db):
        """Test domain type detection."""
        with SchemaMapper(temp_db) as mapper:
            schema_map = mapper.build_schema_map()

            dm_table = schema_map.tables["DM"]
            assert dm_table.domain_type == "SDTM"

            ae_table = schema_map.tables["AE"]
            assert ae_table.domain_type == "SDTM"


class TestSchemaMapPersistence:
    """Test saving and loading schema maps."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary DuckDB database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            conn = duckdb.connect(db_path)

            conn.execute("CREATE TABLE DM (USUBJID VARCHAR, SEX VARCHAR)")
            conn.execute("INSERT INTO DM VALUES ('U1', 'M')")

            conn.close()
            yield db_path

    def test_save_schema_map(self, temp_db):
        """Test saving schema map to JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "schema_map.json")

            with SchemaMapper(temp_db) as mapper:
                schema_map = mapper.build_schema_map()
                mapper.save_schema_map(schema_map, output_path)

            assert os.path.exists(output_path)

            with open(output_path, 'r') as f:
                data = json.load(f)

            assert "columns" in data
            assert "tables" in data
            assert "generated_at" in data

    def test_load_schema_map(self, temp_db):
        """Test loading schema map from JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "schema_map.json")

            with SchemaMapper(temp_db) as mapper:
                original = mapper.build_schema_map()
                mapper.save_schema_map(original, output_path)

            loaded = SchemaMapper.load_schema_map(output_path)

            assert len(loaded.tables) == len(original.tables)
            assert len(loaded.columns) == len(original.columns)
            assert "DM" in loaded.tables
            assert "USUBJID" in loaded.columns


class TestBuildSchemaMapFunction:
    """Test the convenience function."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary DuckDB database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            conn = duckdb.connect(db_path)

            conn.execute("CREATE TABLE DM (USUBJID VARCHAR)")
            conn.execute("INSERT INTO DM VALUES ('U1')")

            conn.close()
            yield db_path, tmpdir

    def test_build_schema_map_function(self, temp_db):
        """Test the convenience function."""
        db_path, tmpdir = temp_db
        output_path = os.path.join(tmpdir, "schema_map.json")

        schema_map = build_schema_map(db_path, output_path=output_path)

        assert isinstance(schema_map, SchemaMap)
        assert os.path.exists(output_path)


class TestSchemaMapDataclasses:
    """Test dataclass methods."""

    def test_column_info_to_dict(self):
        """Test ColumnInfo to_dict method."""
        col = ColumnInfo(
            name="AETERM",
            tables=["AE"],
            data_type="VARCHAR",
            is_key=False,
            description="Adverse Event Term",
            unique_values_count=100,
            sample_values=["HEADACHE", "NAUSEA"]
        )

        d = col.to_dict()
        assert d["name"] == "AETERM"
        assert d["tables"] == ["AE"]
        assert d["type"] == "VARCHAR"
        assert d["is_key"] is False
        assert d["description"] == "Adverse Event Term"

    def test_table_info_to_dict(self):
        """Test TableInfo to_dict method."""
        table = TableInfo(
            name="AE",
            columns=["USUBJID", "AETERM", "AESEV"],
            row_count=1000,
            description="Adverse Events",
            domain_type="SDTM",
            key_columns=["USUBJID"]
        )

        d = table.to_dict()
        assert d["name"] == "AE"
        assert d["columns"] == ["USUBJID", "AETERM", "AESEV"]
        assert d["row_count"] == 1000
        assert d["domain_type"] == "SDTM"

    def test_schema_map_to_dict(self):
        """Test SchemaMap to_dict method."""
        schema_map = SchemaMap(
            generated_at="2024-01-01T00:00:00",
            version="1.0"
        )
        schema_map.tables["DM"] = TableInfo(
            name="DM",
            columns=["USUBJID"],
            row_count=10,
            domain_type="SDTM"
        )
        schema_map.columns["USUBJID"] = ColumnInfo(
            name="USUBJID",
            tables=["DM"],
            data_type="VARCHAR",
            is_key=True
        )

        d = schema_map.to_dict()
        assert "tables" in d
        assert "columns" in d
        assert "generated_at" in d
        assert "version" in d
