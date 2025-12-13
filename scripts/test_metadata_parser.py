#!/usr/bin/env python3
"""
Test script for AK112 spec metadata parsing.

Usage:
    python scripts/test_metadata_parser.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.metadata.excel_parser import ExcelParser


def test_adam_spec():
    """Test ADaM spec parsing."""
    print("=" * 60)
    print("TESTING ADaM SPEC")
    print("=" * 60)

    parser = ExcelParser()
    result = parser.parse_file('tests/ak112-301-adam-spec.xlsx')

    print(f"Success: {result.success}")
    print(f"Domains found: {len(result.domains)}")
    print(f"Codelists found: {len(result.codelists)}")
    print(f"Total variables: {sum(len(d.variables) for d in result.domains)}")

    if result.errors:
        print(f"Errors: {result.errors}")
    if result.warnings:
        print(f"Warnings: {result.warnings[:5]}")

    print("\nDomains:")
    for d in result.domains:
        vars_with_derivation = sum(1 for v in d.variables if v.derivation)
        vars_with_codelist = sum(1 for v in d.variables if v.codelist)
        print(f"  {d.name:12s}: {len(d.variables):3d} vars "
              f"({vars_with_derivation:3d} derivations, {vars_with_codelist:3d} codelists)")

    print(f"\nCodelists: {len(result.codelists)}")
    for cl in result.codelists[:10]:
        print(f"  {cl.name}: {len(cl.values)} values")

    return result


def test_sdtm_spec():
    """Test SDTM spec parsing."""
    print("\n" + "=" * 60)
    print("TESTING SDTM SPEC")
    print("=" * 60)

    parser = ExcelParser()
    result = parser.parse_file('tests/AK112-301 CSR SDTM Spec_V0.2.xlsx')

    print(f"Success: {result.success}")
    print(f"Domains found: {len(result.domains)}")
    print(f"Codelists found: {len(result.codelists)}")
    print(f"Total variables: {sum(len(d.variables) for d in result.domains)}")

    if result.errors:
        print(f"Errors: {result.errors}")
    if result.warnings:
        print(f"Warnings: {result.warnings[:5]}")

    print("\nDomains:")
    for d in result.domains:
        vars_with_derivation = sum(1 for v in d.variables if v.derivation)
        vars_with_codelist = sum(1 for v in d.variables if v.codelist)
        print(f"  {d.name:12s}: {len(d.variables):3d} vars "
              f"({vars_with_derivation:3d} derivations, {vars_with_codelist:3d} codelists)")

    return result


def main():
    """Run all tests."""
    adam_result = test_adam_spec()
    sdtm_result = test_sdtm_spec()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    total_domains = len(adam_result.domains) + len(sdtm_result.domains)
    total_variables = (sum(len(d.variables) for d in adam_result.domains) +
                      sum(len(d.variables) for d in sdtm_result.domains))
    total_codelists = len(adam_result.codelists) + len(sdtm_result.codelists)

    print(f"Total domains: {total_domains}")
    print(f"Total variables: {total_variables}")
    print(f"Total codelists: {total_codelists}")

    # Success criteria check
    print("\n" + "-" * 40)
    print("SUCCESS CRITERIA:")
    checks = [
        ("ADaM domains >= 15", len(adam_result.domains) >= 15),
        ("SDTM domains >= 30", len(sdtm_result.domains) >= 30),
        ("ADaM codelists >= 50", len(adam_result.codelists) >= 50),
        ("Total variables >= 2000", total_variables >= 2000),
    ]

    all_passed = True
    for name, passed in checks:
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {name}")
        if not passed:
            all_passed = False

    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
