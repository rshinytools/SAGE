#!/usr/bin/env python3
"""
Initialize CDISC Standards Library

This script imports SDTM IG and ADaM IG Excel files into the CDISC standards
database for use in auto-approval of metadata variables.

Usage:
    python scripts/init_cdisc_library.py

Options:
    --sdtm PATH     Path to SDTM IG Excel file
    --adam PATH     Path to ADaM IG Excel file
    --db PATH       Path to output SQLite database
    --clear         Clear existing data before import
    --stats         Show library statistics after import
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.metadata import CDISCLibrary, initialize_cdisc_library


def main():
    parser = argparse.ArgumentParser(
        description='Initialize CDISC Standards Library'
    )
    parser.add_argument(
        '--sdtm',
        default='CDISC/SDTMIG_v3.4.xlsx',
        help='Path to SDTM IG Excel file'
    )
    parser.add_argument(
        '--adam',
        default='CDISC/ADaMIG_v1.3.xlsx',
        help='Path to ADaM IG Excel file'
    )
    parser.add_argument(
        '--db',
        default='knowledge/cdisc_library.db',
        help='Path to output SQLite database'
    )
    parser.add_argument(
        '--clear',
        action='store_true',
        help='Clear existing data before import'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show library statistics after import'
    )
    parser.add_argument(
        '--user',
        default='admin',
        help='Username for audit trail'
    )

    args = parser.parse_args()

    # Resolve paths relative to project root
    db_path = project_root / args.db
    sdtm_path = project_root / args.sdtm if args.sdtm else None
    adam_path = project_root / args.adam if args.adam else None

    print("=" * 60)
    print("CDISC Standards Library Initialization")
    print("=" * 60)
    print()
    print(f"Database: {db_path}")
    print(f"SDTM IG:  {sdtm_path}")
    print(f"ADaM IG:  {adam_path}")
    print()

    # Initialize library
    library = CDISCLibrary(str(db_path))

    # Clear if requested
    if args.clear:
        print("Clearing existing data...")
        library.clear_standard('SDTM')
        library.clear_standard('ADaM')
        print()

    # Import SDTM
    if sdtm_path and sdtm_path.exists():
        print("Importing SDTM IG...")
        result = library.import_sdtm_ig(str(sdtm_path), args.user)
        print(f"  Version:   {result['version']}")
        print(f"  Domains:   {result['domains_imported']}")
        print(f"  Variables: {result['variables_imported']}")
        print()
    elif sdtm_path:
        print(f"WARNING: SDTM file not found: {sdtm_path}")
        print()

    # Import ADaM
    if adam_path and adam_path.exists():
        print("Importing ADaM IG...")
        result = library.import_adam_ig(str(adam_path), args.user)
        print(f"  Version:   {result['version']}")
        print(f"  Domains:   {result['domains_imported']}")
        print(f"  Variables: {result['variables_imported']}")
        print()
    elif adam_path:
        print(f"WARNING: ADaM file not found: {adam_path}")
        print()

    # Show statistics
    if args.stats or True:  # Always show stats
        stats = library.get_statistics()
        print("=" * 60)
        print("Library Statistics")
        print("=" * 60)
        print(f"Total Domains:   {stats['total_domains']}")
        print(f"Total Variables: {stats['total_variables']}")
        print()
        print("By Standard:")
        for standard, count in stats.get('domains_by_standard', {}).items():
            var_count = stats.get('variables_by_standard', {}).get(standard, 0)
            print(f"  {standard}: {count} domains, {var_count} variables")
        print()
        print("Import History:")
        for entry in stats.get('import_history', []):
            print(f"  {entry['imported_at']}: {entry['standard']} {entry['version']} "
                  f"({entry['domains']} domains, {entry['variables']} variables)")
        print()

    print("=" * 60)
    print("CDISC Library initialization complete!")
    print("=" * 60)

    # Test a few matches
    print()
    print("Testing Variable Matching:")
    print("-" * 40)

    test_cases = [
        ('DM', 'STUDYID', 'Study Identifier'),
        ('ADSL', 'USUBJID', 'Unique Subject Identifier'),
        ('AE', 'AETERM', 'Reported Term for the Adverse Event'),
        ('ADSL', 'TRTSDT', 'Treatment Start Date'),
        ('ADSL', 'CUSTOMVAR', 'Custom Study Variable'),  # Should not match
    ]

    for domain, name, label in test_cases:
        match = library.match_variable(domain, name, label)
        status = "✓" if match.matched else "✗"
        print(f"  {status} {domain}.{name}: {match.match_type} "
              f"(confidence: {match.confidence}%)")
        if match.reason:
            print(f"      → {match.reason[:60]}...")

    print()


if __name__ == '__main__':
    main()
