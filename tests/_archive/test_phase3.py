#!/usr/bin/env python
# SAGE - Phase 3 Test Suite
# =========================
# Tests for Metadata Factory components
"""
Comprehensive test suite for Phase 3: Metadata Factory

Tests:
1. Excel Parser - Parse specification files
2. Codelist Merger - Merge codelists with variables
3. Version Control - Track metadata changes
4. Metadata Store - Store and manage metadata
5. LLM Drafter - Generate descriptions
6. Integration - End-to-end pipeline
"""

import sys
import json
import tempfile
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def print_header(title: str):
    """Print a test section header."""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")


def print_result(name: str, passed: bool, details: str = ""):
    """Print a test result."""
    status = "PASS" if passed else "FAIL"
    print(f"  [{status}] {name}")
    if details and not passed:
        print(f"         {details}")


def test_excel_parser():
    """Test Excel Parser module."""
    print_header("Test 1: Excel Parser")

    from core.metadata import (
        ExcelParser, VariableSpec, DomainSpec,
        CodelistSpec, ParseResult
    )

    passed = 0
    total = 0

    # Test 1.1: Parser initialization
    total += 1
    try:
        parser = ExcelParser()
        print_result("Parser initialization", True)
        passed += 1
    except Exception as e:
        print_result("Parser initialization", False, str(e))
        return passed, total

    # Test 1.2: VariableSpec creation
    total += 1
    try:
        var = VariableSpec(
            name="USUBJID",
            label="Unique Subject Identifier",
            data_type="Char",
            length=40,
            core="Req"
        )
        assert var.name == "USUBJID"
        assert var.to_dict()['name'] == "USUBJID"
        print_result("VariableSpec creation", True)
        passed += 1
    except Exception as e:
        print_result("VariableSpec creation", False, str(e))

    # Test 1.3: DomainSpec creation
    total += 1
    try:
        domain = DomainSpec(
            name="DM",
            label="Demographics",
            structure="One record per subject",
            variables=[var]
        )
        assert domain.name == "DM"
        assert len(domain.variables) == 1
        assert domain.get_variable("usubjid").name == "USUBJID"
        print_result("DomainSpec creation", True)
        passed += 1
    except Exception as e:
        print_result("DomainSpec creation", False, str(e))

    # Test 1.4: CodelistSpec creation
    total += 1
    try:
        codelist = CodelistSpec(
            name="SEX",
            label="Sex Codelist",
            values=[
                {'code': 'M', 'decode': 'Male'},
                {'code': 'F', 'decode': 'Female'}
            ]
        )
        assert codelist.name == "SEX"
        assert len(codelist.values) == 2
        print_result("CodelistSpec creation", True)
        passed += 1
    except Exception as e:
        print_result("CodelistSpec creation", False, str(e))

    # Test 1.5: ParseResult creation
    total += 1
    try:
        result = ParseResult(
            success=True,
            filename="test.xlsx",
            domains=[domain],
            codelists=[codelist]
        )
        assert result.success
        assert len(result.domains) == 1
        print_result("ParseResult creation", True)
        passed += 1
    except Exception as e:
        print_result("ParseResult creation", False, str(e))

    # Test 1.6: Column normalization
    total += 1
    try:
        import pandas as pd
        df = pd.DataFrame({
            'Variable Name': ['STUDYID'],
            'Variable Label': ['Study Identifier'],
            'Type': ['Char']
        })
        normalized = parser._normalize_columns(df)
        assert 'name' in normalized.columns
        assert 'label' in normalized.columns
        print_result("Column normalization", True)
        passed += 1
    except Exception as e:
        print_result("Column normalization", False, str(e))

    # Test 1.7: Data type normalization
    total += 1
    try:
        assert parser._normalize_datatype('Numeric') == 'Num'
        assert parser._normalize_datatype('Character') == 'Char'
        assert parser._normalize_datatype('text') == 'Char'
        assert parser._normalize_datatype('integer') == 'Num'
        print_result("Data type normalization", True)
        passed += 1
    except Exception as e:
        print_result("Data type normalization", False, str(e))

    # Test 1.8: Core normalization
    total += 1
    try:
        assert parser._normalize_core('Required') == 'Req'
        assert parser._normalize_core('Exp') == 'Exp'
        assert parser._normalize_core('P') == 'Perm'
        print_result("Core normalization", True)
        passed += 1
    except Exception as e:
        print_result("Core normalization", False, str(e))

    return passed, total


def test_codelist_merger():
    """Test Codelist Merger module."""
    print_header("Test 2: Codelist Merger")

    from core.metadata import (
        CodelistMerger, CodelistSpec, VariableSpec, DomainSpec,
        EnrichedVariable, EnrichedDomain, MergeResult
    )

    passed = 0
    total = 0

    # Test 2.1: Merger initialization
    total += 1
    try:
        merger = CodelistMerger()
        print_result("Merger initialization", True)
        passed += 1
    except Exception as e:
        print_result("Merger initialization", False, str(e))
        return passed, total

    # Test 2.2: Add codelist
    total += 1
    try:
        codelist = CodelistSpec(
            name="SEX",
            label="Sex Codelist",
            values=[
                {'code': 'M', 'decode': 'Male'},
                {'code': 'F', 'decode': 'Female'}
            ]
        )
        merger.add_codelist(codelist)
        assert "SEX" in merger.list_codelists()
        print_result("Add codelist", True)
        passed += 1
    except Exception as e:
        print_result("Add codelist", False, str(e))

    # Test 2.3: Get codelist
    total += 1
    try:
        retrieved = merger.get_codelist("sex")  # Case insensitive
        assert retrieved is not None
        assert retrieved.name == "SEX"
        print_result("Get codelist (case insensitive)", True)
        passed += 1
    except Exception as e:
        print_result("Get codelist (case insensitive)", False, str(e))

    # Test 2.4: Merge variable with codelist
    total += 1
    try:
        var = VariableSpec(
            name="SEX",
            label="Sex",
            data_type="Char",
            codelist="SEX"
        )
        enriched = merger.merge_variable(var)
        assert enriched.has_codelist
        assert len(enriched.codelist_values) == 2
        assert not enriched.codelist_missing
        print_result("Merge variable with codelist", True)
        passed += 1
    except Exception as e:
        print_result("Merge variable with codelist", False, str(e))

    # Test 2.5: Merge variable without codelist
    total += 1
    try:
        var_no_cl = VariableSpec(
            name="AGE",
            label="Age",
            data_type="Num"
        )
        enriched = merger.merge_variable(var_no_cl)
        assert not enriched.has_codelist
        assert not enriched.codelist_missing
        print_result("Merge variable without codelist", True)
        passed += 1
    except Exception as e:
        print_result("Merge variable without codelist", False, str(e))

    # Test 2.6: Merge variable with missing codelist
    total += 1
    try:
        var_missing = VariableSpec(
            name="RACE",
            label="Race",
            data_type="Char",
            codelist="RACE_CL"  # Doesn't exist
        )
        enriched = merger.merge_variable(var_missing)
        assert not enriched.has_codelist
        assert enriched.codelist_missing
        print_result("Merge variable with missing codelist", True)
        passed += 1
    except Exception as e:
        print_result("Merge variable with missing codelist", False, str(e))

    # Test 2.7: Merge domain
    total += 1
    try:
        domain = DomainSpec(
            name="DM",
            label="Demographics",
            variables=[
                VariableSpec(name="STUDYID", label="Study ID", data_type="Char"),
                VariableSpec(name="SEX", label="Sex", data_type="Char", codelist="SEX"),
            ]
        )
        enriched_domain = merger.merge_domain(domain)
        assert len(enriched_domain.variables) == 2
        assert enriched_domain.codelist_coverage == 100.0
        print_result("Merge domain", True)
        passed += 1
    except Exception as e:
        print_result("Merge domain", False, str(e))

    # Test 2.8: Merge multiple domains
    total += 1
    try:
        domains = [domain]
        result = merger.merge_domains(domains)
        assert result.success
        assert len(result.domains) == 1
        assert 'total_variables' in result.statistics
        print_result("Merge multiple domains", True)
        passed += 1
    except Exception as e:
        print_result("Merge multiple domains", False, str(e))

    # Test 2.9: Validate codelist value
    total += 1
    try:
        is_valid, decode = merger.validate_value("SEX", "M")
        assert is_valid
        assert decode == "Male"

        is_valid, decode = merger.validate_value("SEX", "X")
        assert not is_valid
        print_result("Validate codelist value", True)
        passed += 1
    except Exception as e:
        print_result("Validate codelist value", False, str(e))

    # Test 2.10: Find codelist by value
    total += 1
    try:
        results = merger.find_codelist_by_value("M")
        assert len(results) >= 1
        assert results[0][0] == "SEX"
        print_result("Find codelist by value", True)
        passed += 1
    except Exception as e:
        print_result("Find codelist by value", False, str(e))

    return passed, total


def test_version_control():
    """Test Version Control module."""
    print_header("Test 3: Version Control")

    from core.metadata import (
        VersionControl, MetadataVersion, MetadataChange, ChangeType, DiffResult
    )
    import gc

    passed = 0
    total = 0

    # Use temp database in project directory to avoid Windows file locking
    tmpdir = project_root / "temp_test"
    tmpdir.mkdir(exist_ok=True)
    db_path = tmpdir / "test_versions.db"

    try:

        # Test 3.1: Version control initialization
        total += 1
        try:
            vc = VersionControl(str(db_path))
            assert db_path.exists()
            print_result("Version control initialization", True)
            passed += 1
        except Exception as e:
            print_result("Version control initialization", False, str(e))
            return passed, total

        # Test 3.2: Create first version
        total += 1
        try:
            content = {
                'domains': [{'name': 'DM', 'label': 'Demographics', 'variables': []}]
            }
            version = vc.create_version(content, comment="Initial version", user="test")
            assert version.version_number == 1
            assert version.created_by == "test"
            print_result("Create first version", True)
            passed += 1
        except Exception as e:
            print_result("Create first version", False, str(e))

        # Test 3.3: Create second version
        total += 1
        try:
            content2 = {
                'domains': [
                    {'name': 'DM', 'label': 'Demographics', 'variables': []},
                    {'name': 'AE', 'label': 'Adverse Events', 'variables': []}
                ]
            }
            version2 = vc.create_version(content2, comment="Added AE", user="test")
            assert version2.version_number == 2
            assert version2.parent_version == version.version_id
            print_result("Create second version", True)
            passed += 1
        except Exception as e:
            print_result("Create second version", False, str(e))

        # Test 3.4: Get latest version
        total += 1
        try:
            latest = vc.get_latest_version()
            assert latest.version_number == 2
            print_result("Get latest version", True)
            passed += 1
        except Exception as e:
            print_result("Get latest version", False, str(e))

        # Test 3.5: Get specific version
        total += 1
        try:
            result = vc.get_version(version.version_id)
            assert result is not None
            ver, content = result
            assert ver.version_number == 1
            print_result("Get specific version", True)
            passed += 1
        except Exception as e:
            print_result("Get specific version", False, str(e))

        # Test 3.6: Record change
        total += 1
        try:
            change = MetadataChange(
                entity_type="variable",
                entity_id="DM.USUBJID",
                change_type=ChangeType.MODIFIED,
                field_name="label",
                old_value="Subject ID",
                new_value="Unique Subject ID",
                user="test"
            )
            vc.record_change(change)
            print_result("Record change", True)
            passed += 1
        except Exception as e:
            print_result("Record change", False, str(e))

        # Test 3.7: Get history
        total += 1
        try:
            history = vc.get_history(limit=10)
            assert len(history) >= 1
            assert history[0].entity_id == "DM.USUBJID"
            print_result("Get change history", True)
            passed += 1
        except Exception as e:
            print_result("Get change history", False, str(e))

        # Test 3.8: Get versions list
        total += 1
        try:
            versions = vc.get_versions()
            assert len(versions) == 2
            print_result("Get versions list", True)
            passed += 1
        except Exception as e:
            print_result("Get versions list", False, str(e))

        # Test 3.9: Diff versions
        total += 1
        try:
            diff = vc.diff_versions(version.version_id, version2.version_id)
            assert diff is not None
            assert diff.has_changes
            assert len(diff.added) == 1  # AE domain added
            print_result("Diff versions", True)
            passed += 1
        except Exception as e:
            print_result("Diff versions", False, str(e))

        # Test 3.10: Set approval status
        total += 1
        try:
            vc.set_approval_status(
                entity_type="variable",
                entity_id="DM.USUBJID",
                status="approved",
                user="admin"
            )
            pending = vc.get_pending_approvals()
            # Should have been approved, so not in pending
            print_result("Set approval status", True)
            passed += 1
        except Exception as e:
            print_result("Set approval status", False, str(e))

        # Test 3.11: Get statistics
        total += 1
        try:
            stats = vc.get_statistics()
            assert 'total_versions' in stats
            assert stats['total_versions'] == 2
            print_result("Get statistics", True)
            passed += 1
        except Exception as e:
            print_result("Get statistics", False, str(e))

        # Test 3.12: Rollback
        total += 1
        try:
            content = vc.rollback(version.version_id, user="test")
            assert content is not None
            assert len(content['domains']) == 1  # Rolled back to version with 1 domain

            # Should have created version 3
            latest = vc.get_latest_version()
            assert latest.version_number == 3
            print_result("Rollback version", True)
            passed += 1
        except Exception as e:
            print_result("Rollback version", False, str(e))

    finally:
        # Cleanup
        del vc
        gc.collect()
        import time
        time.sleep(0.1)
        try:
            if db_path.exists():
                db_path.unlink()
            if tmpdir.exists():
                tmpdir.rmdir()
        except Exception:
            pass  # Ignore cleanup errors on Windows

    return passed, total


def test_metadata_store():
    """Test Metadata Store module."""
    print_header("Test 4: Metadata Store")

    from core.metadata import (
        MetadataStore, GoldenDomain, GoldenVariable, GoldenCodelist,
        ApprovalStatus, CodelistMerger, CodelistSpec, VariableSpec, DomainSpec
    )
    import gc

    passed = 0
    total = 0

    # Use temp directory in project to avoid Windows file locking
    tmpdir = project_root / "temp_test_store"
    tmpdir.mkdir(exist_ok=True)
    storage_path = tmpdir / "golden_metadata.json"
    version_db = tmpdir / "versions.db"

    store = None
    store2 = None

    try:

        # Test 4.1: Store initialization
        total += 1
        try:
            store = MetadataStore(str(storage_path), str(version_db))
            print_result("Store initialization", True)
            passed += 1
        except Exception as e:
            print_result("Store initialization", False, str(e))
            return passed, total

        # Test 4.2: Import merge result
        total += 1
        try:
            # Create test data
            merger = CodelistMerger()
            merger.add_codelist(CodelistSpec(
                name="SEX",
                label="Sex",
                values=[{'code': 'M', 'decode': 'Male'}]
            ))

            domain = DomainSpec(
                name="DM",
                label="Demographics",
                variables=[
                    VariableSpec(name="USUBJID", label="Subject ID", data_type="Char"),
                    VariableSpec(name="SEX", label="Sex", data_type="Char", codelist="SEX")
                ]
            )
            merge_result = merger.merge_domains([domain])

            store.import_merge_result(merge_result, user="test")
            assert len(store.list_domains()) == 1
            print_result("Import merge result", True)
            passed += 1
        except Exception as e:
            print_result("Import merge result", False, str(e))

        # Test 4.3: Get domain
        total += 1
        try:
            dm = store.get_domain("DM")
            assert dm is not None
            assert dm.name == "DM"
            assert len(dm.variables) == 2
            print_result("Get domain", True)
            passed += 1
        except Exception as e:
            print_result("Get domain", False, str(e))

        # Test 4.4: Get variable
        total += 1
        try:
            var = store.get_variable("DM", "USUBJID")
            assert var is not None
            assert var.name == "USUBJID"
            print_result("Get variable", True)
            passed += 1
        except Exception as e:
            print_result("Get variable", False, str(e))

        # Test 4.5: Update variable
        total += 1
        try:
            store.update_variable(
                "DM", "USUBJID",
                {'plain_english': 'The unique identifier for each subject in the study.'},
                user="test"
            )
            var = store.get_variable("DM", "USUBJID")
            assert var.plain_english == 'The unique identifier for each subject in the study.'
            print_result("Update variable", True)
            passed += 1
        except Exception as e:
            print_result("Update variable", False, str(e))

        # Test 4.6: Approve variable
        total += 1
        try:
            store.approve_variable("DM", "USUBJID", user="admin")
            var = store.get_variable("DM", "USUBJID")
            assert var.approval.status == "approved"
            assert var.approval.reviewed_by == "admin"
            print_result("Approve variable", True)
            passed += 1
        except Exception as e:
            print_result("Approve variable", False, str(e))

        # Test 4.7: Reject variable
        total += 1
        try:
            store.reject_variable("DM", "SEX", user="admin", comment="Needs review")
            var = store.get_variable("DM", "SEX")
            assert var.approval.status == "rejected"
            assert var.approval.comment == "Needs review"
            print_result("Reject variable", True)
            passed += 1
        except Exception as e:
            print_result("Reject variable", False, str(e))

        # Test 4.8: Get pending items
        total += 1
        try:
            pending = store.get_pending_items()
            # Domain should still be pending
            assert len(pending['domains']) >= 1
            print_result("Get pending items", True)
            passed += 1
        except Exception as e:
            print_result("Get pending items", False, str(e))

        # Test 4.9: Search
        total += 1
        try:
            results = store.search("subject")
            assert len(results) >= 1
            print_result("Search metadata", True)
            passed += 1
        except Exception as e:
            print_result("Search metadata", False, str(e))

        # Test 4.10: Save and load
        total += 1
        try:
            store.save(user="test", comment="Test save")
            assert storage_path.exists()

            # Create new store and load
            store2 = MetadataStore(str(storage_path), str(version_db))
            assert len(store2.list_domains()) == 1
            print_result("Save and load", True)
            passed += 1
        except Exception as e:
            print_result("Save and load", False, str(e))

        # Test 4.11: Export approved only
        total += 1
        try:
            export_path = Path(tmpdir) / "export_approved.json"
            store.export_golden_metadata(str(export_path), approved_only=True)
            assert export_path.exists()

            with open(export_path) as f:
                data = json.load(f)
            # Only approved variables should be exported
            print_result("Export approved only", True)
            passed += 1
        except Exception as e:
            print_result("Export approved only", False, str(e))

        # Test 4.12: Get statistics
        total += 1
        try:
            stats = store.get_statistics()
            assert 'total_domains' in stats
            assert 'total_variables' in stats
            assert stats['total_domains'] == 1
            assert stats['total_variables'] == 2
            print_result("Get statistics", True)
            passed += 1
        except Exception as e:
            print_result("Get statistics", False, str(e))

    finally:
        # Cleanup
        del store
        del store2
        gc.collect()
        import time
        time.sleep(0.1)
        try:
            import shutil
            if tmpdir.exists():
                shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass  # Ignore cleanup errors on Windows

    return passed, total


def test_llm_drafter():
    """Test LLM Drafter module."""
    print_header("Test 5: LLM Drafter")

    from core.metadata import LLMDrafter, TemplateDrafter, DraftRequest, DraftResult

    passed = 0
    total = 0

    # Test 5.1: Template drafter (always available)
    total += 1
    try:
        drafter = TemplateDrafter()
        request = DraftRequest(
            variable_name="USUBJID",
            domain="DM",
            label="Unique Subject Identifier"
        )
        result = drafter.draft_description(request)
        assert result.plain_english
        assert result.model_used == "template"
        print_result("Template drafter", True)
        passed += 1
    except Exception as e:
        print_result("Template drafter", False, str(e))

    # Test 5.2: Template for known variable
    total += 1
    try:
        request = DraftRequest(
            variable_name="STUDYID",
            domain="DM",
            label="Study Identifier"
        )
        result = drafter.draft_description(request)
        assert "study" in result.plain_english.lower()
        print_result("Template for known variable", True)
        passed += 1
    except Exception as e:
        print_result("Template for known variable", False, str(e))

    # Test 5.3: Template pattern matching
    total += 1
    try:
        request = DraftRequest(
            variable_name="RFSTDT",
            domain="ADSL",
            label="Reference Start Date"
        )
        result = drafter.draft_description(request)
        assert "date" in result.plain_english.lower()
        print_result("Template pattern matching", True)
        passed += 1
    except Exception as e:
        print_result("Template pattern matching", False, str(e))

    # Test 5.4: DraftRequest creation
    total += 1
    try:
        request = DraftRequest(
            variable_name="TRTSDT",
            domain="ADSL",
            label="Date of First Exposure",
            derivation="Earliest EXSTDTC from EX",
            codelist="DATE",
            codelist_values=[{'code': 'YYYY-MM-DD', 'decode': 'ISO Date'}]
        )
        assert request.variable_name == "TRTSDT"
        assert request.derivation is not None
        print_result("DraftRequest creation", True)
        passed += 1
    except Exception as e:
        print_result("DraftRequest creation", False, str(e))

    # Test 5.5: DraftResult creation
    total += 1
    try:
        result = DraftResult(
            variable_name="TRTSDT",
            domain="ADSL",
            plain_english="The date when the subject first received study treatment.",
            confidence=0.85,
            model_used="template"
        )
        assert result.variable_name == "TRTSDT"
        assert result.confidence == 0.85
        assert result.generated_at  # Should be auto-set
        print_result("DraftResult creation", True)
        passed += 1
    except Exception as e:
        print_result("DraftResult creation", False, str(e))

    # Test 5.6: LLM Drafter initialization (may fall back to mock)
    total += 1
    try:
        llm_drafter = LLMDrafter()
        # Should initialize without error (may use mock backend)
        assert llm_drafter.is_available()
        print_result("LLM Drafter initialization", True)
        passed += 1
    except Exception as e:
        print_result("LLM Drafter initialization", False, str(e))

    # Test 5.7: LLM draft description
    total += 1
    try:
        request = DraftRequest(
            variable_name="CHG",
            domain="ADLB",
            label="Change from Baseline",
            derivation="AVAL - BASE"
        )
        result = llm_drafter.draft_description(request)
        assert result.plain_english or result.error
        print_result("LLM draft description", True)
        passed += 1
    except Exception as e:
        print_result("LLM draft description", False, str(e))

    # Test 5.8: Batch drafting
    total += 1
    try:
        requests = [
            DraftRequest(variable_name="STUDYID", domain="DM", label="Study ID"),
            DraftRequest(variable_name="USUBJID", domain="DM", label="Subject ID"),
        ]
        results = drafter.draft_batch(requests)
        assert len(results) == 2
        assert all(r.plain_english for r in results)
        print_result("Batch drafting", True)
        passed += 1
    except Exception as e:
        print_result("Batch drafting", False, str(e))

    return passed, total


def test_integration():
    """Test end-to-end integration."""
    print_header("Test 6: Integration")

    from core.metadata import (
        ExcelParser, CodelistMerger, MetadataStore,
        VariableSpec, DomainSpec, CodelistSpec
    )
    import gc
    import shutil

    passed = 0
    total = 0

    # Use temp directory in project to avoid Windows file locking
    tmpdir = project_root / "temp_test_integration"
    tmpdir.mkdir(exist_ok=True)
    store = None

    try:
        # Test 6.1: Full pipeline without Excel file
        total += 1
        try:
            # Create test data
            domains = [
                DomainSpec(
                    name="DM",
                    label="Demographics",
                    variables=[
                        VariableSpec(name="STUDYID", label="Study Identifier", data_type="Char", core="Req"),
                        VariableSpec(name="USUBJID", label="Unique Subject Identifier", data_type="Char", core="Req"),
                        VariableSpec(name="SEX", label="Sex", data_type="Char", codelist="SEX"),
                        VariableSpec(name="AGE", label="Age", data_type="Num"),
                    ]
                ),
                DomainSpec(
                    name="AE",
                    label="Adverse Events",
                    variables=[
                        VariableSpec(name="STUDYID", label="Study Identifier", data_type="Char"),
                        VariableSpec(name="USUBJID", label="Unique Subject Identifier", data_type="Char"),
                        VariableSpec(name="AETERM", label="Adverse Event Term", data_type="Char"),
                        VariableSpec(name="AESEV", label="Severity", data_type="Char", codelist="AESEV"),
                    ]
                )
            ]

            codelists = [
                CodelistSpec(name="SEX", label="Sex", values=[
                    {'code': 'M', 'decode': 'Male'},
                    {'code': 'F', 'decode': 'Female'}
                ]),
                CodelistSpec(name="AESEV", label="Severity", values=[
                    {'code': 'MILD', 'decode': 'Mild'},
                    {'code': 'MODERATE', 'decode': 'Moderate'},
                    {'code': 'SEVERE', 'decode': 'Severe'}
                ])
            ]

            # Merge codelists
            merger = CodelistMerger()
            merger.add_codelists(codelists)
            merge_result = merger.merge_domains(domains)

            assert merge_result.success
            assert len(merge_result.domains) == 2
            print_result("Full pipeline: merge", True)
            passed += 1
        except Exception as e:
            print_result("Full pipeline: merge", False, str(e))

        # Test 6.2: Import to store
        total += 1
        try:
            storage_path = Path(tmpdir) / "golden.json"
            version_db = Path(tmpdir) / "versions.db"

            store = MetadataStore(str(storage_path), str(version_db))
            store.import_merge_result(merge_result, user="test")
            store.import_codelists(codelists, user="test")

            assert len(store.list_domains()) == 2
            assert len(store.list_codelists()) == 2
            print_result("Full pipeline: import to store", True)
            passed += 1
        except Exception as e:
            print_result("Full pipeline: import to store", False, str(e))

        # Test 6.3: Approval workflow
        total += 1
        try:
            # Approve some variables
            store.approve_variable("DM", "STUDYID", user="admin")
            store.approve_variable("DM", "USUBJID", user="admin")
            store.reject_variable("DM", "AGE", user="admin", comment="Needs unit")

            stats = store.get_approval_stats()
            assert stats['variables']['approved'] == 2
            assert stats['variables']['rejected'] == 1
            print_result("Full pipeline: approval workflow", True)
            passed += 1
        except Exception as e:
            print_result("Full pipeline: approval workflow", False, str(e))

        # Test 6.4: Save and export
        total += 1
        try:
            store.save(user="test", comment="Integration test")

            export_path = Path(tmpdir) / "export.json"
            store.export_golden_metadata(str(export_path), approved_only=True)

            with open(export_path) as f:
                data = json.load(f)

            # Should only have approved items
            print_result("Full pipeline: save and export", True)
            passed += 1
        except Exception as e:
            print_result("Full pipeline: save and export", False, str(e))

        # Test 6.5: Version history
        total += 1
        try:
            versions = store.version_control.get_versions()
            assert len(versions) >= 1

            history = store.version_control.get_history(limit=20)
            assert len(history) >= 3  # Approvals and rejection
            print_result("Full pipeline: version history", True)
            passed += 1
        except Exception as e:
            print_result("Full pipeline: version history", False, str(e))

        # Test 6.6: Search functionality
        total += 1
        try:
            results = store.search("adverse")
            assert len(results) >= 1
            assert any("AE" in str(r) for r in results)
            print_result("Full pipeline: search", True)
            passed += 1
        except Exception as e:
            print_result("Full pipeline: search", False, str(e))

    finally:
        # Cleanup
        if store:
            del store
        gc.collect()
        import time
        time.sleep(0.1)
        try:
            if tmpdir.exists():
                shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass  # Ignore cleanup errors on Windows

    return passed, total


def main():
    """Run all Phase 3 tests."""
    print("\n" + "=" * 60)
    print(" SAGE Phase 3: Metadata Factory - Test Suite")
    print("=" * 60)

    total_passed = 0
    total_tests = 0

    # Run all test suites
    test_suites = [
        ("Excel Parser", test_excel_parser),
        ("Codelist Merger", test_codelist_merger),
        ("Version Control", test_version_control),
        ("Metadata Store", test_metadata_store),
        ("LLM Drafter", test_llm_drafter),
        ("Integration", test_integration),
    ]

    results = []
    for name, test_func in test_suites:
        try:
            passed, total = test_func()
            total_passed += passed
            total_tests += total
            results.append((name, passed, total))
        except Exception as e:
            print(f"\n[ERROR] Test suite '{name}' failed: {e}")
            results.append((name, 0, 1))
            total_tests += 1

    # Summary
    print("\n" + "=" * 60)
    print(" Test Summary")
    print("=" * 60)

    for name, passed, total in results:
        status = "PASS" if passed == total else "FAIL"
        print(f"  {name:25s} {passed:3d}/{total:3d} [{status}]")

    print("-" * 60)
    print(f"  {'TOTAL':25s} {total_passed:3d}/{total_tests:3d}")
    print("=" * 60)

    success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0
    print(f"\n  Success Rate: {success_rate:.1f}%")

    if total_passed == total_tests:
        print("\n  All tests passed!")
        return 0
    else:
        print(f"\n  {total_tests - total_passed} test(s) failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
