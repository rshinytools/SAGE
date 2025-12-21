#!/usr/bin/env python3
# SAGE - Phase 2 Test Suite
# ==========================
# Tests all components of Factory 1: Data Foundry
"""
Test suite for Phase 2 components:
1. Date Handler - Date parsing and imputation
2. DuckDB Loader - Database operations
3. Data Validator - Validation checks
4. Integration - End-to-end pipeline with sample data
"""

import sys
from pathlib import Path
from datetime import datetime, date
import tempfile
import shutil

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np


def print_header(title):
    print()
    print("=" * 60)
    print(f" {title}")
    print("=" * 60)


def print_test(name, passed, details=""):
    status = "[PASS]" if passed else "[FAIL]"
    print(f"  {status} {name}")
    if details and not passed:
        print(f"         {details}")


def test_date_handler():
    """Test DateHandler module."""
    print_header("Test 1: Date Handler")

    from core.data.date_handler import DateHandler, ImputationRule, DatePrecision

    handler = DateHandler()
    all_passed = True

    # Test ISO date parsing
    test_cases = [
        ("2024-01-15", DatePrecision.FULL, 2024, 1, 15),
        ("2024-01", DatePrecision.MONTH, 2024, 1, None),
        ("2024", DatePrecision.YEAR, 2024, None, None),
        ("2024-01-15T10:30:00", DatePrecision.FULL, 2024, 1, 15),
        ("", DatePrecision.UNKNOWN, None, None, None),
        (None, DatePrecision.UNKNOWN, None, None, None),
    ]

    for value, expected_precision, exp_year, exp_month, exp_day in test_cases:
        result = handler.parse_date(value)
        passed = (
            result.precision == expected_precision and
            result.year == exp_year and
            result.month == exp_month and
            result.day == exp_day
        )
        all_passed = all_passed and passed
        print_test(f"parse_date('{value}')", passed,
                   f"Got: {result.precision.value}, {result.year}-{result.month}-{result.day}")

    # Test date imputation
    print()
    print("  Date Imputation Tests:")

    partial = handler.parse_date("2024-06")  # Month precision
    imputed_first = handler.impute_date(partial, ImputationRule.FIRST)
    imputed_last = handler.impute_date(partial, ImputationRule.LAST)
    imputed_middle = handler.impute_date(partial, ImputationRule.MIDDLE)

    print_test("Impute FIRST (2024-06 -> 2024-06-01)",
               imputed_first == date(2024, 6, 1))
    print_test("Impute LAST (2024-06 -> 2024-06-30)",
               imputed_last == date(2024, 6, 30))
    print_test("Impute MIDDLE (2024-06 -> 2024-06-15)",
               imputed_middle == date(2024, 6, 15))

    # Test SAS date conversion
    print()
    print("  SAS Date Conversion:")

    # SAS date 0 = 1960-01-01
    sas_result = handler.parse_date(0)
    print_test("SAS date 0 = 1960-01-01",
               sas_result.year == 1960 and sas_result.month == 1 and sas_result.day == 1)

    # Test DataFrame column standardization
    print()
    print("  DataFrame Column Standardization:")

    df = pd.DataFrame({
        'AESTDTC': ['2024-01-15', '2024-02', '2024', None, '']
    })
    df_result = handler.standardize_column(df, 'AESTDTC')

    print_test("Added ISO column", 'AESTDTC_ISO' in df_result.columns)
    print_test("Added IMPUTED column", 'AESTDTC_IMPUTED' in df_result.columns)
    print_test("Full date preserved", df_result['AESTDTC_ISO'].iloc[0] == '2024-01-15')
    print_test("Partial date formatted", df_result['AESTDTC_ISO'].iloc[1] == '2024-02')

    return all_passed


def test_duckdb_loader():
    """Test DuckDBLoader module."""
    print_header("Test 2: DuckDB Loader")

    from core.data.duckdb_loader import DuckDBLoader

    # Use temp directory
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test.duckdb"
    all_passed = True

    try:
        # Create test DataFrame
        df = pd.DataFrame({
            'USUBJID': ['SUBJ001', 'SUBJ002', 'SUBJ003'],
            'AGE': [45, 52, 38],
            'SEX': ['M', 'F', 'M'],
            'WEIGHT': [75.5, 62.3, 88.1]
        })

        with DuckDBLoader(str(db_path)) as loader:
            # Test load
            result = loader.load_dataframe(df, 'DM', source_file='test.sas7bdat')
            passed = result.success and result.rows_loaded == 3
            all_passed = all_passed and passed
            print_test("load_dataframe()", passed, result.error if not passed else "")

            # Test list tables
            tables = loader.list_tables()
            passed = 'DM' in tables
            all_passed = all_passed and passed
            print_test("list_tables()", passed)

            # Test get table info
            info = loader.get_table_info('DM')
            passed = info is not None and info.row_count == 3 and info.column_count == 4
            all_passed = all_passed and passed
            print_test("get_table_info()", passed)

            # Test query
            result_df = loader.query("SELECT COUNT(*) as cnt FROM DM")
            passed = result_df.iloc[0]['cnt'] == 3
            all_passed = all_passed and passed
            print_test("query()", passed)

            # Test sample data
            sample = loader.get_sample_data('DM', limit=2)
            passed = len(sample) == 2
            all_passed = all_passed and passed
            print_test("get_sample_data()", passed)

            # Test column statistics
            stats = loader.get_column_statistics('DM', 'AGE')
            passed = stats['min'] == 38 and stats['max'] == 52
            all_passed = all_passed and passed
            print_test("get_column_statistics()", passed)

            # Test validation
            validation = loader.validate_table('DM', expected_rows=3)
            passed = validation.is_valid
            all_passed = all_passed and passed
            print_test("validate_table()", passed)

            # Test drop table
            drop_result = loader.drop_table('DM')
            passed = drop_result and 'DM' not in loader.list_tables()
            all_passed = all_passed and passed
            print_test("drop_table()", passed)

    finally:
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    return all_passed


def test_data_validator():
    """Test DataValidator module."""
    print_header("Test 3: Data Validator")

    from core.data.duckdb_loader import DuckDBLoader
    from scripts.validators.data_validator import DataValidator, ValidationReport

    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test.duckdb"
    all_passed = True

    try:
        # Create test data with required SDTM DM columns
        source_df = pd.DataFrame({
            'STUDYID': ['STUDY01'] * 5,
            'USUBJID': [f'STUDY01-{i:03d}' for i in range(1, 6)],
            'SUBJID': [f'{i:03d}' for i in range(1, 6)],
            'SITEID': ['SITE01'] * 5,
            'AGE': [45, 52, 38, 41, 55],
            'SEX': ['M', 'F', 'M', 'F', 'M']
        })

        with DuckDBLoader(str(db_path)) as loader:
            loader.load_dataframe(source_df, 'DM')

            validator = DataValidator()

            # Test table validation
            result = validator.validate_table(source_df, loader, 'DM')
            passed = result.row_match and result.column_match
            all_passed = all_passed and passed
            print_test("validate_table() - row/column match", passed)

            # Test quality score (should be high)
            passed = result.quality_score >= 90.0
            all_passed = all_passed and passed
            print_test("quality_score >= 90%", passed, f"Got: {result.quality_score}")

            # Test DataFrame validation - with required columns
            is_valid, errors, warnings = validator.validate_dataframe(source_df, 'DM')
            passed = len(errors) == 0  # No missing required columns
            all_passed = all_passed and passed
            print_test("validate_dataframe() - required columns", passed,
                      f"Errors: {errors}" if errors else "")

            # Test report generation
            report = validator.create_report([result])
            passed = len(report.tables) == 1 and report.overall_quality_score >= 90
            all_passed = all_passed and passed
            print_test("create_report()", passed)

            # Test report save
            report_path = Path(temp_dir) / "report.json"
            report.save(str(report_path))
            passed = report_path.exists()
            all_passed = all_passed and passed
            print_test("report.save()", passed)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return all_passed


def test_integration():
    """Test end-to-end integration with sample clinical data."""
    print_header("Test 4: Integration (Sample Clinical Data)")

    from core.data.date_handler import DateHandler, ImputationRule
    from core.data.duckdb_loader import DuckDBLoader
    from scripts.validators.data_validator import DataValidator

    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "clinical.duckdb"
    all_passed = True

    try:
        # Create sample SDTM DM dataset
        dm_df = pd.DataFrame({
            'STUDYID': ['STUDY001'] * 10,
            'DOMAIN': ['DM'] * 10,
            'USUBJID': [f'STUDY001-{i:03d}' for i in range(1, 11)],
            'SUBJID': [f'{i:03d}' for i in range(1, 11)],
            'SITEID': ['SITE01', 'SITE01', 'SITE02', 'SITE02', 'SITE03'] * 2,
            'RFSTDTC': ['2024-01-15', '2024-01-20', '2024-02', '2024-02-10', '2024-03-01',
                        '2024-03-15', '2024-04', '2024-04-20', '2024-05-01', '2024-05-15'],
            'BRTHDTC': ['1975-06-15', '1980-03-20', '1965-11', '1990-07-04', '1985-01-01',
                        '1978-09-10', '1988-12-25', '1970-04-15', '1995-08-30', '1982-02-28'],
            'AGE': [48, 43, 58, 33, 39, 45, 35, 53, 28, 41],
            'SEX': ['M', 'F', 'M', 'F', 'M', 'F', 'M', 'F', 'M', 'F'],
            'RACE': ['WHITE', 'BLACK', 'ASIAN', 'WHITE', 'BLACK',
                     'WHITE', 'ASIAN', 'WHITE', 'BLACK', 'ASIAN']
        })

        # Create sample AE dataset
        ae_df = pd.DataFrame({
            'STUDYID': ['STUDY001'] * 15,
            'DOMAIN': ['AE'] * 15,
            'USUBJID': [f'STUDY001-{(i % 10) + 1:03d}' for i in range(15)],
            'AESEQ': list(range(1, 16)),
            'AETERM': ['Headache', 'Nausea', 'Fatigue', 'Dizziness', 'Rash',
                       'Fever', 'Cough', 'Pain', 'Insomnia', 'Anxiety',
                       'Headache', 'Nausea', 'Fever', 'Fatigue', 'Rash'],
            'AEDECOD': ['HEADACHE', 'NAUSEA', 'FATIGUE', 'DIZZINESS', 'RASH',
                        'PYREXIA', 'COUGH', 'PAIN', 'INSOMNIA', 'ANXIETY',
                        'HEADACHE', 'NAUSEA', 'PYREXIA', 'FATIGUE', 'RASH'],
            'AESTDTC': ['2024-01-20', '2024-01-25', '2024-02-01', '2024-02', '2024-02-15',
                        '2024-03-01', '2024-03-10', '2024-03', '2024-04-01', '2024-04-10',
                        '2024-04-20', '2024-05', '2024-05-10', '2024-05-15', '2024-05-20'],
            'AEENDTC': ['2024-01-25', '2024-01-27', '2024-02-05', '2024-02-10', '',
                        '2024-03-05', '2024-03-15', '2024-03-20', '2024-04-10', '',
                        '2024-04-25', '2024-05-08', '2024-05-15', '', '2024-05-25'],
            'AESEV': ['MILD', 'MILD', 'MODERATE', 'MILD', 'MODERATE',
                      'MODERATE', 'MILD', 'SEVERE', 'MILD', 'MILD',
                      'MILD', 'MODERATE', 'MODERATE', 'MILD', 'MILD']
        })

        # Create sample ADSL (ADaM)
        adsl_df = pd.DataFrame({
            'STUDYID': ['STUDY001'] * 10,
            'USUBJID': [f'STUDY001-{i:03d}' for i in range(1, 11)],
            'SUBJID': [f'{i:03d}' for i in range(1, 11)],
            'SITEID': ['SITE01', 'SITE01', 'SITE02', 'SITE02', 'SITE03'] * 2,
            'TRT01P': ['Drug A', 'Drug B', 'Drug A', 'Drug B', 'Placebo'] * 2,
            'TRT01A': ['Drug A', 'Drug B', 'Drug A', 'Drug B', 'Placebo'] * 2,
            'SAFFL': ['Y'] * 10,
            'ITTFL': ['Y'] * 10,
            'AGE': [48, 43, 58, 33, 39, 45, 35, 53, 28, 41],
            'AGEGR1': ['>=45', '<45', '>=45', '<45', '<45', '>=45', '<45', '>=45', '<45', '<45'],
            'SEX': ['M', 'F', 'M', 'F', 'M', 'F', 'M', 'F', 'M', 'F'],
            'RACE': ['WHITE', 'BLACK', 'ASIAN', 'WHITE', 'BLACK',
                     'WHITE', 'ASIAN', 'WHITE', 'BLACK', 'ASIAN'],
            'TRTSDT': [None, None, None, None, None, None, None, None, None, None],
            'TRTEDT': [None, None, None, None, None, None, None, None, None, None]
        })

        # Process dates
        date_handler = DateHandler(ImputationRule.FIRST)

        dm_df = date_handler.standardize_column(dm_df, 'RFSTDTC', add_precision_column=False)
        dm_df = date_handler.standardize_column(dm_df, 'BRTHDTC', add_precision_column=False)
        ae_df = date_handler.standardize_column(ae_df, 'AESTDTC', add_precision_column=False)
        ae_df = date_handler.standardize_column(ae_df, 'AEENDTC', add_precision_column=False)

        print_test("Date standardization completed", True)

        # Load into DuckDB
        with DuckDBLoader(str(db_path)) as loader:
            # Load DM
            result = loader.load_dataframe(dm_df, 'DM')
            passed = result.success and result.rows_loaded == 10
            all_passed = all_passed and passed
            print_test("Load DM (10 subjects)", passed)

            # Load AE
            result = loader.load_dataframe(ae_df, 'AE')
            passed = result.success and result.rows_loaded == 15
            all_passed = all_passed and passed
            print_test("Load AE (15 events)", passed)

            # Load ADSL
            result = loader.load_dataframe(adsl_df, 'ADSL')
            passed = result.success and result.rows_loaded == 10
            all_passed = all_passed and passed
            print_test("Load ADSL (10 subjects)", passed)

            # Test queries
            print()
            print("  Query Tests:")

            # Count subjects
            result = loader.query("SELECT COUNT(DISTINCT USUBJID) as n FROM DM")
            passed = result.iloc[0]['n'] == 10
            all_passed = all_passed and passed
            print_test("Count distinct subjects", passed)

            # Count AEs by severity
            result = loader.query("""
                SELECT AESEV, COUNT(*) as n
                FROM AE
                GROUP BY AESEV
                ORDER BY n DESC
            """)
            passed = len(result) == 3  # MILD, MODERATE, SEVERE
            all_passed = all_passed and passed
            print_test("Count AEs by severity", passed)

            # Join DM and AE
            result = loader.query("""
                SELECT dm.SEX, COUNT(ae.AESEQ) as ae_count
                FROM DM dm
                LEFT JOIN AE ae ON dm.USUBJID = ae.USUBJID
                GROUP BY dm.SEX
            """)
            passed = len(result) == 2  # M and F
            all_passed = all_passed and passed
            print_test("Join DM and AE", passed)

            # Treatment distribution
            result = loader.query("""
                SELECT TRT01P, COUNT(*) as n
                FROM ADSL
                GROUP BY TRT01P
            """)
            passed = len(result) == 3  # Drug A, Drug B, Placebo
            all_passed = all_passed and passed
            print_test("Treatment distribution from ADSL", passed)

            # Validate all tables
            print()
            print("  Validation:")

            validator = DataValidator()
            for table_name, source_df in [('DM', dm_df), ('AE', ae_df), ('ADSL', adsl_df)]:
                validation = validator.validate_table(source_df, loader, table_name)
                passed = validation.quality_score >= 80
                all_passed = all_passed and passed
                print_test(f"Validate {table_name} (score: {validation.quality_score}%)", passed)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return all_passed


def main():
    """Run all Phase 2 tests."""
    print()
    print("*" * 60)
    print("  SAGE Phase 2 Test Suite - Factory 1: Data Foundry")
    print("*" * 60)

    results = []

    # Run tests
    results.append(("Date Handler", test_date_handler()))
    results.append(("DuckDB Loader", test_duckdb_loader()))
    results.append(("Data Validator", test_data_validator()))
    results.append(("Integration", test_integration()))

    # Summary
    print_header("Test Summary")

    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)

    for name, passed in results:
        status = "[PASS]" if passed else "[FAIL]"
        print(f"  {status} {name}")

    print()
    print(f"  Total: {passed_count}/{total_count} test groups passed")

    if passed_count == total_count:
        print()
        print("  All Phase 2 tests PASSED!")
        return 0
    else:
        print()
        print("  Some tests FAILED. Check output above.")
        return 1


if __name__ == '__main__':
    sys.exit(main())
