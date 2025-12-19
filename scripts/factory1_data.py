#!/usr/bin/env python3
# SAGE - Factory 1: Data Foundry
# ==============================
# Main pipeline script for SAS to DuckDB conversion
"""
Data Factory Pipeline

This script processes clinical trial data from SAS7BDAT format into DuckDB:
1. Scans input directory for SAS files
2. Reads each file with proper encoding
3. Standardizes dates and handles partial dates
4. Validates data integrity
5. Loads into DuckDB database
6. Generates validation report

Usage:
    python scripts/factory1_data.py --input data/raw --output data/database/clinical.duckdb

    # Process specific files
    python scripts/factory1_data.py --input data/raw --files dm.sas7bdat ae.sas7bdat

    # With date imputation
    python scripts/factory1_data.py --input data/raw --impute-dates first
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any
import json

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data.sas_reader import SASReader
from core.data.date_handler import DateHandler, ImputationRule
from core.data.duckdb_loader import DuckDBLoader
from scripts.validators.data_validator import DataValidator, ValidationReport
from core.engine.cache import get_query_cache, reset_query_cache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataPipeline:
    """
    Main data processing pipeline.

    Orchestrates the flow from SAS files to DuckDB:
    SAS7BDAT → DataFrame → Date Standardization → Validation → DuckDB
    """

    # Date columns that should be standardized (common clinical data columns)
    DATE_COLUMNS = [
        # SDTM date columns
        'RFSTDTC', 'RFENDTC', 'RFXSTDTC', 'RFXENDTC',
        'RFICDTC', 'RFPENDTC', 'DTHDTC', 'BRTHDTC',
        'AESTDTC', 'AEENDTC', 'AEDTC',
        'CMSTDTC', 'CMENDTC',
        'LBDTC', 'VSDTC', 'EGDTC',
        'EXSTDTC', 'EXENDTC',
        'DSSTDTC', 'DSDTC',
        'MHSTDTC', 'MHENDTC',
        # ADaM date columns
        'TRTSDT', 'TRTEDT', 'ASTDT', 'AENDT',
        'ADT', 'STARTDT', 'ENDDT',
        'RANDDT', 'DTHDT',
    ]

    def __init__(self,
                 input_dir: str,
                 output_db: str,
                 processed_dir: Optional[str] = None,
                 imputation_rule: ImputationRule = ImputationRule.FIRST):
        """
        Initialize the data pipeline.

        Args:
            input_dir: Directory containing SAS files
            output_db: Path to output DuckDB database
            processed_dir: Directory for processed Parquet files (optional)
            imputation_rule: Rule for imputing partial dates
        """
        self.input_dir = Path(input_dir)
        self.output_db = Path(output_db)
        self.processed_dir = Path(processed_dir) if processed_dir else None
        self.imputation_rule = imputation_rule

        # Initialize components
        self.sas_reader = SASReader()
        self.date_handler = DateHandler(default_imputation=imputation_rule)
        self.validator = DataValidator()

        # Ensure directories exist
        self.output_db.parent.mkdir(parents=True, exist_ok=True)
        if self.processed_dir:
            self.processed_dir.mkdir(parents=True, exist_ok=True)

        # Results tracking
        self.results: List[Dict[str, Any]] = []
        self.validation_results: List[Any] = []

    def discover_files(self, specific_files: Optional[List[str]] = None) -> List[Path]:
        """
        Discover SAS files to process.

        Args:
            specific_files: Optional list of specific filenames to process

        Returns:
            List of file paths
        """
        if specific_files:
            files = []
            for filename in specific_files:
                filepath = self.input_dir / filename
                if filepath.exists():
                    files.append(filepath)
                else:
                    logger.warning(f"File not found: {filepath}")
            return files

        return list(self.input_dir.glob('*.sas7bdat'))

    def process_file(self, filepath: Path) -> Dict[str, Any]:
        """
        Process a single SAS file.

        Args:
            filepath: Path to the SAS file

        Returns:
            Dictionary with processing results
        """
        result = {
            'file': filepath.name,
            'table_name': filepath.stem.upper(),
            'status': 'pending',
            'rows_read': 0,
            'rows_loaded': 0,
            'errors': [],
            'warnings': [],
            'duration_seconds': 0
        }

        start_time = datetime.now()

        try:
            logger.info(f"Processing: {filepath.name}")

            # Step 1: Read SAS file
            read_result = self.sas_reader.read_file(str(filepath))

            if not read_result.success:
                result['status'] = 'failed'
                result['errors'].append(f"Read error: {read_result.error}")
                return result

            df = read_result.dataframe
            result['rows_read'] = len(df)
            result['warnings'].extend(read_result.warnings)

            logger.info(f"  Read {len(df)} rows, {len(df.columns)} columns")

            # Step 2: Standardize date columns
            df = self._standardize_dates(df)

            # Step 3: Save to Parquet (optional)
            if self.processed_dir:
                parquet_path = self.processed_dir / f"{filepath.stem}.parquet"
                success, error = self.sas_reader.to_parquet(
                    str(filepath), str(parquet_path)
                )
                if not success:
                    result['warnings'].append(f"Parquet save warning: {error}")
                else:
                    logger.info(f"  Saved Parquet: {parquet_path.name}")

            # Step 4: Load into DuckDB
            with DuckDBLoader(str(self.output_db)) as loader:
                load_result = loader.load_dataframe(
                    df,
                    result['table_name'],
                    source_file=str(filepath),
                    if_exists='replace'
                )

                if not load_result.success:
                    result['status'] = 'failed'
                    result['errors'].append(f"Load error: {load_result.error}")
                    return result

                result['rows_loaded'] = load_result.rows_loaded
                result['warnings'].extend(load_result.warnings)

                # Step 5: Validate
                validation = self.validator.validate_table(
                    df, loader, result['table_name'], str(filepath)
                )
                self.validation_results.append(validation)

                if not validation.is_valid:
                    result['warnings'].append(f"Validation issues: {validation.issues}")

                result['quality_score'] = validation.quality_score

            result['status'] = 'success'
            logger.info(f"  Loaded {result['rows_loaded']} rows into {result['table_name']}")

        except Exception as e:
            result['status'] = 'failed'
            result['errors'].append(str(e))
            logger.error(f"Error processing {filepath.name}: {e}")

        result['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        return result

    def _standardize_dates(self, df) -> 'pd.DataFrame':
        """Standardize date columns in the DataFrame."""
        import pandas as pd

        for col in df.columns:
            # Check if this is a date column
            col_upper = col.upper()
            is_date_col = (
                col_upper in self.DATE_COLUMNS or
                col_upper.endswith('DTC') or
                col_upper.endswith('DT') or
                'DATE' in col_upper
            )

            if is_date_col:
                try:
                    df = self.date_handler.standardize_column(
                        df, col,
                        imputation_rule=self.imputation_rule,
                        add_precision_column=False
                    )
                    logger.debug(f"  Standardized date column: {col}")
                except Exception as e:
                    logger.warning(f"  Could not standardize {col}: {e}")

        return df

    def run(self, specific_files: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run the complete pipeline.

        Args:
            specific_files: Optional list of specific files to process

        Returns:
            Dictionary with pipeline results
        """
        start_time = datetime.now()

        logger.info("=" * 60)
        logger.info("SAGE Data Factory - Pipeline Starting")
        logger.info("=" * 60)
        logger.info(f"Input directory: {self.input_dir}")
        logger.info(f"Output database: {self.output_db}")

        # Discover files
        files = self.discover_files(specific_files)

        if not files:
            logger.warning("No SAS files found to process")
            return {
                'status': 'no_files',
                'files_found': 0,
                'files_processed': 0
            }

        logger.info(f"Found {len(files)} file(s) to process")

        # Process each file
        for filepath in files:
            result = self.process_file(filepath)
            self.results.append(result)

        # Generate validation report
        if self.validation_results:
            report = self.validator.create_report(self.validation_results)
            report_path = self.output_db.parent / 'validation_report.json'
            report.save(str(report_path))
            logger.info(f"Saved validation report: {report_path}")

        # Clear query cache after data load (cached results are now stale)
        successful = sum(1 for r in self.results if r['status'] == 'success')
        if successful > 0:
            try:
                cache = get_query_cache(db_path=str(self.output_db))
                entries_cleared = len(cache)
                cache.clear()
                logger.info(f"Query cache cleared ({entries_cleared} entries) - data has changed")
            except Exception as e:
                logger.warning(f"Could not clear query cache: {e}")

        # Summary
        total_duration = (datetime.now() - start_time).total_seconds()
        successful = sum(1 for r in self.results if r['status'] == 'success')
        failed = sum(1 for r in self.results if r['status'] == 'failed')
        total_rows = sum(r['rows_loaded'] for r in self.results)

        summary = {
            'status': 'completed',
            'files_found': len(files),
            'files_processed': len(self.results),
            'files_successful': successful,
            'files_failed': failed,
            'total_rows_loaded': total_rows,
            'duration_seconds': round(total_duration, 2),
            'output_database': str(self.output_db),
            'results': self.results
        }

        logger.info("")
        logger.info("=" * 60)
        logger.info("Pipeline Complete")
        logger.info("=" * 60)
        logger.info(f"Files processed: {successful}/{len(files)} successful")
        logger.info(f"Total rows loaded: {total_rows:,}")
        logger.info(f"Duration: {total_duration:.2f} seconds")
        logger.info(f"Database: {self.output_db}")

        return summary


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description='SAGE Data Factory - SAS to DuckDB Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all SAS files in a directory
  python factory1_data.py --input data/raw --output data/database/clinical.duckdb

  # Process specific files
  python factory1_data.py --input data/raw --output data/database/clinical.duckdb --files dm.sas7bdat ae.sas7bdat

  # With date imputation rule
  python factory1_data.py --input data/raw --output data/database/clinical.duckdb --impute-dates last

  # Save intermediate Parquet files
  python factory1_data.py --input data/raw --output data/database/clinical.duckdb --parquet data/processed
        """
    )

    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input directory containing SAS7BDAT files'
    )

    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Output DuckDB database path'
    )

    parser.add_argument(
        '--files', '-f',
        nargs='+',
        help='Specific files to process (optional)'
    )

    parser.add_argument(
        '--parquet', '-p',
        help='Directory to save intermediate Parquet files (optional)'
    )

    parser.add_argument(
        '--impute-dates',
        choices=['first', 'last', 'middle', 'none'],
        default='first',
        help='Rule for imputing partial dates (default: first)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Map imputation rule
    imputation_map = {
        'first': ImputationRule.FIRST,
        'last': ImputationRule.LAST,
        'middle': ImputationRule.MIDDLE,
        'none': ImputationRule.NONE
    }

    # Create and run pipeline
    pipeline = DataPipeline(
        input_dir=args.input,
        output_db=args.output,
        processed_dir=args.parquet,
        imputation_rule=imputation_map[args.impute_dates]
    )

    result = pipeline.run(specific_files=args.files)

    # Exit with appropriate code
    if result['status'] == 'completed' and result.get('files_failed', 0) == 0:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
