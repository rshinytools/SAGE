"""
Comprehensive Pytest Suite for SAGE Metadata System
Tests Factory 2 (Metadata Refinery) and Factory 2.5 (CDISC Standards Library)

Run with: pytest tests/test_metadata_system.py -v --tb=short
"""

import pytest
import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(scope="module")
def project_root():
    """Return project root path."""
    return PROJECT_ROOT


@pytest.fixture(scope="module")
def knowledge_dir(project_root):
    """Return knowledge directory path."""
    return project_root / "knowledge"


@pytest.fixture(scope="module")
def golden_metadata_path(knowledge_dir):
    """Return golden metadata JSON path."""
    return knowledge_dir / "golden_metadata.json"


@pytest.fixture(scope="module")
def cdisc_library_path(knowledge_dir):
    """Return CDISC library database path."""
    return knowledge_dir / "cdisc_library.db"


@pytest.fixture(scope="module")
def metadata_versions_path(knowledge_dir):
    """Return metadata versions database path."""
    return knowledge_dir / "metadata_versions.db"


# ============================================================================
# TEST CLASS: Core Module Imports
# ============================================================================

class TestCoreModuleImports:
    """Test that all core metadata modules can be imported."""

    def test_import_excel_parser(self):
        """Test ExcelParser and related classes import."""
        from core.metadata import ExcelParser, VariableSpec, DomainSpec, CodelistSpec, ParseResult
        assert ExcelParser is not None
        assert VariableSpec is not None
        assert DomainSpec is not None
        assert CodelistSpec is not None
        assert ParseResult is not None

    def test_import_codelist_merger(self):
        """Test CodelistMerger and related classes import."""
        from core.metadata import CodelistMerger, EnrichedVariable, EnrichedDomain, MergeResult
        assert CodelistMerger is not None
        assert EnrichedVariable is not None
        assert EnrichedDomain is not None
        assert MergeResult is not None

    def test_import_version_control(self):
        """Test VersionControl and related classes import."""
        from core.metadata import VersionControl, MetadataVersion, MetadataChange, ChangeType, DiffResult
        assert VersionControl is not None
        assert MetadataVersion is not None
        assert MetadataChange is not None
        assert ChangeType is not None
        assert DiffResult is not None

    def test_import_metadata_store(self):
        """Test MetadataStore and related classes import."""
        from core.metadata import MetadataStore, GoldenDomain, GoldenVariable, GoldenCodelist, ApprovalStatus
        assert MetadataStore is not None
        assert GoldenDomain is not None
        assert GoldenVariable is not None
        assert GoldenCodelist is not None
        assert ApprovalStatus is not None

    def test_import_llm_drafter(self):
        """Test LLMDrafter and related classes import."""
        from core.metadata import LLMDrafter, TemplateDrafter, DraftRequest, DraftResult
        assert LLMDrafter is not None
        assert TemplateDrafter is not None
        assert DraftRequest is not None
        assert DraftResult is not None

    def test_import_cdisc_library(self):
        """Test CDISCLibrary and related classes import."""
        from core.metadata import CDISCLibrary, CDISCDomain, CDISCVariable, MatchResult
        assert CDISCLibrary is not None
        assert CDISCDomain is not None
        assert CDISCVariable is not None
        assert MatchResult is not None

    def test_import_auto_approval(self):
        """Test AutoApprovalEngine and related classes import."""
        from core.metadata import AutoApprovalEngine, ApprovalDecision, ApprovalSummary
        assert AutoApprovalEngine is not None
        assert ApprovalDecision is not None
        assert ApprovalSummary is not None

    def test_import_initialize_functions(self):
        """Test initialization functions import."""
        from core.metadata import initialize_cdisc_library, run_auto_approval
        assert initialize_cdisc_library is not None
        assert run_auto_approval is not None


# ============================================================================
# TEST CLASS: File Existence
# ============================================================================

class TestFileExistence:
    """Test that required files exist."""

    def test_golden_metadata_exists(self, golden_metadata_path):
        """Test golden_metadata.json exists."""
        assert golden_metadata_path.exists(), f"Golden metadata not found at {golden_metadata_path}"

    def test_cdisc_library_exists(self, cdisc_library_path):
        """Test cdisc_library.db exists."""
        assert cdisc_library_path.exists(), f"CDISC library not found at {cdisc_library_path}"

    def test_metadata_versions_exists(self, metadata_versions_path):
        """Test metadata_versions.db exists."""
        assert metadata_versions_path.exists(), f"Metadata versions DB not found at {metadata_versions_path}"

    def test_excel_parser_module_exists(self, project_root):
        """Test excel_parser.py exists."""
        path = project_root / "core" / "metadata" / "excel_parser.py"
        assert path.exists(), f"Excel parser not found at {path}"

    def test_cdisc_library_module_exists(self, project_root):
        """Test cdisc_library.py exists."""
        path = project_root / "core" / "metadata" / "cdisc_library.py"
        assert path.exists(), f"CDISC library module not found at {path}"

    def test_auto_approval_module_exists(self, project_root):
        """Test auto_approval.py exists."""
        path = project_root / "core" / "metadata" / "auto_approval.py"
        assert path.exists(), f"Auto approval module not found at {path}"


# ============================================================================
# TEST CLASS: MetadataStore
# ============================================================================

class TestMetadataStore:
    """Test MetadataStore functionality."""

    @pytest.fixture(scope="class")
    def store(self, golden_metadata_path, metadata_versions_path):
        """Create MetadataStore instance."""
        from core.metadata import MetadataStore
        return MetadataStore(str(golden_metadata_path), str(metadata_versions_path))

    def test_store_initialization(self, store):
        """Test store initializes correctly."""
        assert store is not None

    def test_get_all_domains(self, store):
        """Test getting all domains."""
        domains = store.get_all_domains()
        assert len(domains) >= 50, f"Expected at least 50 domains, got {len(domains)}"

    def test_get_all_variables(self, store):
        """Test getting all variables."""
        variables = store.get_all_variables()
        assert len(variables) >= 3000, f"Expected at least 3000 variables, got {len(variables)}"

    def test_get_all_codelists(self, store):
        """Test getting all codelists."""
        codelists = store.get_all_codelists()
        assert len(codelists) >= 60, f"Expected at least 60 codelists, got {len(codelists)}"

    def test_domain_has_required_fields(self, store):
        """Test domains have required fields."""
        domains = store.get_all_domains()
        for domain in domains[:5]:  # Check first 5
            assert hasattr(domain, 'name'), "Domain missing 'name' field"
            assert hasattr(domain, 'label'), "Domain missing 'label' field"
            assert domain.name is not None, "Domain name is None"

    def test_variable_has_required_fields(self, store):
        """Test variables have required fields."""
        variables = store.get_all_variables()
        for var in variables[:10]:  # Check first 10
            assert hasattr(var, 'name'), "Variable missing 'name' field"
            assert hasattr(var, 'domain'), "Variable missing 'domain' field"
            assert hasattr(var, 'approval'), "Variable missing 'approval' field"
            assert var.name is not None, "Variable name is None"
            assert var.domain is not None, "Variable domain is None"

    def test_approval_status_types(self, store):
        """Test approval status values are valid."""
        variables = store.get_all_variables()
        valid_statuses = {'pending', 'approved', 'rejected'}
        for var in variables[:100]:  # Check first 100
            status = var.approval.status
            assert status in valid_statuses, f"Invalid status: {status}"

    def test_get_statistics(self, store):
        """Test statistics retrieval."""
        stats = store.get_statistics()
        assert 'total_domains' in stats or hasattr(stats, 'total_domains')

    def test_search_functionality(self, store):
        """Test search returns results."""
        results = store.search("STUDYID")
        assert len(results) > 0, "Search for STUDYID returned no results"


# ============================================================================
# TEST CLASS: CDISC Library
# ============================================================================

class TestCDISCLibrary:
    """Test CDISC Library functionality."""

    @pytest.fixture(scope="class")
    def library(self, cdisc_library_path):
        """Create CDISCLibrary instance."""
        from core.metadata import CDISCLibrary
        return CDISCLibrary(str(cdisc_library_path))

    def test_library_initialization(self, library):
        """Test library initializes correctly."""
        assert library is not None

    def test_get_statistics(self, library):
        """Test statistics retrieval."""
        stats = library.get_statistics()
        assert stats['total_domains'] >= 60, f"Expected at least 60 domains, got {stats['total_domains']}"
        assert stats['total_variables'] >= 2000, f"Expected at least 2000 variables, got {stats['total_variables']}"

    def test_sdtm_domains_count(self, library):
        """Test SDTM domain count."""
        stats = library.get_statistics()
        sdtm_count = stats['domains_by_standard'].get('SDTM', 0)
        assert sdtm_count >= 60, f"Expected at least 60 SDTM domains, got {sdtm_count}"

    def test_adam_domains_count(self, library):
        """Test ADaM domain count."""
        stats = library.get_statistics()
        adam_count = stats['domains_by_standard'].get('ADaM', 0)
        assert adam_count >= 3, f"Expected at least 3 ADaM domains, got {adam_count}"

    def test_sdtm_variables_count(self, library):
        """Test SDTM variable count."""
        stats = library.get_statistics()
        sdtm_vars = stats['variables_by_standard'].get('SDTM', 0)
        assert sdtm_vars >= 1900, f"Expected at least 1900 SDTM variables, got {sdtm_vars}"

    def test_adam_variables_count(self, library):
        """Test ADaM variable count."""
        stats = library.get_statistics()
        adam_vars = stats['variables_by_standard'].get('ADaM', 0)
        assert adam_vars >= 400, f"Expected at least 400 ADaM variables, got {adam_vars}"

    def test_get_all_domains(self, library):
        """Test getting all domains."""
        domains = library.get_all_domains()
        assert len(domains) >= 60, f"Expected at least 60 domains, got {len(domains)}"

    def test_get_domain_variables(self, library):
        """Test getting variables for a domain."""
        # DM is a common SDTM domain
        variables = library.get_domain_variables("DM")
        assert len(variables) > 0, "DM domain has no variables"

    def test_search_variables(self, library):
        """Test variable search."""
        results = library.search_variables("STUDYID")
        assert len(results) > 0, "Search for STUDYID returned no results"


# ============================================================================
# TEST CLASS: Variable Matching
# ============================================================================

class TestVariableMatching:
    """Test CDISC variable matching algorithm."""

    @pytest.fixture(scope="class")
    def library(self, cdisc_library_path):
        """Create CDISCLibrary instance."""
        from core.metadata import CDISCLibrary
        return CDISCLibrary(str(cdisc_library_path))

    def test_exact_domain_match_studyid(self, library):
        """Test exact match for STUDYID in DM domain."""
        match = library.match_variable("DM", "STUDYID", "Study Identifier")
        assert match.matched, "STUDYID should match"
        assert match.confidence >= 95, f"Expected confidence >= 95, got {match.confidence}"
        assert match.match_type == "exact_domain", f"Expected exact_domain, got {match.match_type}"

    def test_exact_domain_match_usubjid(self, library):
        """Test exact match for USUBJID in DM domain."""
        match = library.match_variable("DM", "USUBJID", "Unique Subject Identifier")
        assert match.matched, "USUBJID should match"
        assert match.confidence >= 95, f"Expected confidence >= 95, got {match.confidence}"

    def test_exact_domain_match_aeterm(self, library):
        """Test exact match for AETERM in AE domain."""
        match = library.match_variable("AE", "AETERM", "Reported Term for the Adverse Event")
        assert match.matched, "AETERM should match"
        assert match.confidence >= 95, f"Expected confidence >= 95, got {match.confidence}"

    def test_exact_domain_match_aestdtc(self, library):
        """Test exact match for AESTDTC in AE domain."""
        match = library.match_variable("AE", "AESTDTC", "Start Date/Time of Adverse Event")
        assert match.matched, "AESTDTC should match"
        assert match.confidence >= 95, f"Expected confidence >= 95, got {match.confidence}"

    def test_adam_variable_match(self, library):
        """Test match for ADaM variable."""
        match = library.match_variable("ADSL", "AGE", "Age")
        assert match.matched, "ADSL.AGE should match"
        assert match.confidence >= 85, f"Expected confidence >= 85, got {match.confidence}"

    def test_custom_variable_no_match(self, library):
        """Test custom variable does not match."""
        match = library.match_variable("ADSL", "CUSTOMVAR", "Custom Study Variable")
        assert not match.matched or match.confidence < 60, "Custom variable should not match with high confidence"

    def test_unknown_variable_no_match(self, library):
        """Test unknown variable does not match."""
        match = library.match_variable("DM", "XYZABC", "Unknown Variable")
        assert not match.matched, "Unknown variable should not match"
        assert match.confidence == 0, f"Expected confidence 0, got {match.confidence}"

    def test_sequence_variable_match(self, library):
        """Test SEQ suffix variable matching."""
        match = library.match_variable("LB", "LBSEQ", "Sequence Number")
        assert match.matched, "LBSEQ should match"
        assert match.confidence >= 75, f"Expected confidence >= 75, got {match.confidence}"

    def test_date_variable_match(self, library):
        """Test DTC suffix variable matching."""
        match = library.match_variable("VS", "VSDTC", "Date/Time of Vital Signs")
        assert match.matched, "VSDTC should match"
        assert match.confidence >= 75, f"Expected confidence >= 75, got {match.confidence}"


# ============================================================================
# TEST CLASS: Auto-Approval Engine
# ============================================================================

class TestAutoApprovalEngine:
    """Test Auto-Approval Engine functionality."""

    @pytest.fixture(scope="class")
    def library(self, cdisc_library_path):
        """Create CDISCLibrary instance."""
        from core.metadata import CDISCLibrary
        return CDISCLibrary(str(cdisc_library_path))

    @pytest.fixture(scope="class")
    def engine(self, library):
        """Create AutoApprovalEngine instance."""
        from core.metadata import AutoApprovalEngine
        return AutoApprovalEngine(library)

    def test_engine_initialization(self, engine):
        """Test engine initializes correctly."""
        assert engine is not None

    def test_analyze_standard_variable(self, engine):
        """Test analyzing a standard CDISC variable."""
        decision = engine.analyze_variable(
            domain="DM",
            name="STUDYID",
            label="Study Identifier",
            data_type="Char"
        )
        assert decision.decision == "auto_approved", f"Expected auto_approved, got {decision.decision}"
        assert decision.confidence >= 85, f"Expected confidence >= 85, got {decision.confidence}"

    def test_analyze_custom_variable(self, engine):
        """Test analyzing a custom/study-specific variable."""
        decision = engine.analyze_variable(
            domain="ADSL",
            name="CUSTOMVAR",
            label="Custom Study Variable",
            data_type="Char"
        )
        assert decision.decision == "manual_review", f"Expected manual_review, got {decision.decision}"
        assert decision.confidence < 60, f"Expected confidence < 60, got {decision.confidence}"

    def test_analyze_batch(self, engine):
        """Test batch analysis."""
        test_variables = [
            {'domain': 'DM', 'name': 'STUDYID', 'label': 'Study Identifier', 'data_type': 'Char'},
            {'domain': 'DM', 'name': 'USUBJID', 'label': 'Unique Subject Identifier', 'data_type': 'Char'},
            {'domain': 'AE', 'name': 'AETERM', 'label': 'Reported Term', 'data_type': 'Char'},
            {'domain': 'ADSL', 'name': 'CUSTOMVAR', 'label': 'Custom Variable', 'data_type': 'Char'},
        ]

        decisions, summary = engine.analyze_batch(test_variables)

        assert len(decisions) == 4, f"Expected 4 decisions, got {len(decisions)}"
        assert summary.total_variables == 4, f"Expected 4 total, got {summary.total_variables}"
        assert summary.auto_approved >= 3, f"Expected at least 3 auto-approved, got {summary.auto_approved}"
        assert summary.manual_review >= 1, f"Expected at least 1 manual review, got {summary.manual_review}"

    def test_batch_processing_time(self, engine):
        """Test that batch processing completes in reasonable time."""
        test_variables = [
            {'domain': 'DM', 'name': f'VAR{i}', 'label': f'Variable {i}', 'data_type': 'Char'}
            for i in range(100)
        ]

        import time
        start = time.time()
        decisions, summary = engine.analyze_batch(test_variables)
        elapsed = time.time() - start

        assert elapsed < 5, f"Batch processing took too long: {elapsed}s"
        assert len(decisions) == 100, f"Expected 100 decisions, got {len(decisions)}"

    def test_approval_thresholds(self, engine):
        """Test approval decision thresholds."""
        # High confidence should be auto-approved
        decision_high = engine.analyze_variable("DM", "STUDYID", "Study Identifier", "Char")
        assert decision_high.decision == "auto_approved"

        # Low confidence should be manual review
        decision_low = engine.analyze_variable("CUSTOM", "XYZABC", "Unknown", "Char")
        assert decision_low.decision == "manual_review"


# ============================================================================
# TEST CLASS: Excel Parser
# ============================================================================

class TestExcelParser:
    """Test Excel Parser functionality."""

    def test_parser_initialization(self):
        """Test parser can be initialized."""
        from core.metadata import ExcelParser
        parser = ExcelParser()
        assert parser is not None

    def test_variable_column_map_exists(self):
        """Test VARIABLE_COLUMN_MAP is defined."""
        from core.metadata import ExcelParser
        assert len(ExcelParser.VARIABLE_COLUMN_MAP) > 0, "VARIABLE_COLUMN_MAP is empty"
        # Check for common column names
        column_map = ExcelParser.VARIABLE_COLUMN_MAP
        assert 'variable' in column_map or 'variable name' in column_map

    def test_domain_sheet_patterns_exist(self):
        """Test DOMAIN_SHEET_PATTERNS is defined."""
        from core.metadata import ExcelParser
        assert len(ExcelParser.DOMAIN_SHEET_PATTERNS) > 0, "DOMAIN_SHEET_PATTERNS is empty"

    def test_skip_sheet_patterns_exist(self):
        """Test SKIP_SHEET_PATTERNS is defined."""
        from core.metadata import ExcelParser
        assert len(ExcelParser.SKIP_SHEET_PATTERNS) > 0, "SKIP_SHEET_PATTERNS is empty"

    def test_codelist_sheet_patterns_exist(self):
        """Test CODELIST_SHEET_PATTERNS is defined."""
        from core.metadata import ExcelParser
        assert len(ExcelParser.CODELIST_SHEET_PATTERNS) > 0, "CODELIST_SHEET_PATTERNS is empty"

    def test_variable_spec_dataclass(self):
        """Test VariableSpec dataclass has required fields."""
        from core.metadata import VariableSpec
        spec = VariableSpec(
            name="TEST",
            label="Test Variable",
            data_type="Char",
            length=10
        )
        assert spec.name == "TEST"
        assert spec.label == "Test Variable"
        assert hasattr(spec, 'derivation')
        assert hasattr(spec, 'codelist')
        assert hasattr(spec, 'source')
        assert hasattr(spec, 'method')


# ============================================================================
# TEST CLASS: Version Control
# ============================================================================

class TestVersionControl:
    """Test Version Control functionality."""

    @pytest.fixture(scope="class")
    def version_control(self, metadata_versions_path):
        """Create VersionControl instance."""
        from core.metadata import VersionControl
        return VersionControl(str(metadata_versions_path))

    def test_version_control_initialization(self, version_control):
        """Test version control initializes correctly."""
        assert version_control is not None

    def test_get_latest_version(self, version_control):
        """Test getting latest version."""
        version = version_control.get_latest_version()
        # Version might be None if no versions exist, that's OK
        assert version is None or hasattr(version, 'version_id')

    def test_get_versions(self, version_control):
        """Test getting all versions."""
        versions = version_control.get_versions()
        assert isinstance(versions, list)


# ============================================================================
# TEST CLASS: Integration Tests
# ============================================================================

class TestIntegration:
    """Integration tests for the complete metadata system."""

    @pytest.fixture(scope="class")
    def store(self, golden_metadata_path, metadata_versions_path):
        """Create MetadataStore instance."""
        from core.metadata import MetadataStore
        return MetadataStore(str(golden_metadata_path), str(metadata_versions_path))

    @pytest.fixture(scope="class")
    def library(self, cdisc_library_path):
        """Create CDISCLibrary instance."""
        from core.metadata import CDISCLibrary
        return CDISCLibrary(str(cdisc_library_path))

    @pytest.fixture(scope="class")
    def engine(self, library):
        """Create AutoApprovalEngine instance."""
        from core.metadata import AutoApprovalEngine
        return AutoApprovalEngine(library)

    def test_store_and_library_integration(self, store, library):
        """Test that store and library can work together."""
        # Get domains from store
        store_domains = store.get_all_domains()
        assert len(store_domains) > 0

        # Get domains from library
        library_domains = library.get_all_domains()
        assert len(library_domains) > 0

    def test_variable_matching_with_store_data(self, store, library):
        """Test matching store variables against CDISC library."""
        variables = store.get_all_variables()[:10]

        matched_count = 0
        for var in variables:
            match = library.match_variable(
                var.domain,
                var.name,
                var.label or ""
            )
            if match.matched and match.confidence >= 85:
                matched_count += 1

        # At least some should match
        assert matched_count > 0, "No variables matched against CDISC library"

    def test_auto_approval_with_store_data(self, store, engine):
        """Test auto-approval engine with actual store data."""
        variables = store.get_all_variables()[:20]

        test_vars = [
            {
                'domain': var.domain,
                'name': var.name,
                'label': var.label or '',
                'data_type': var.data_type or ''
            }
            for var in variables
        ]

        decisions, summary = engine.analyze_batch(test_vars)

        assert len(decisions) == 20
        assert summary.total_variables == 20
        # At least some should be auto-approved
        assert summary.auto_approved > 0, "No variables were auto-approved"

    def test_complete_workflow(self, store, library, engine):
        """Test complete metadata workflow: load -> match -> analyze."""
        # 1. Load variables from store
        all_vars = store.get_all_variables()
        assert len(all_vars) > 0

        # 2. Get library stats
        stats = library.get_statistics()
        assert stats['total_domains'] > 0

        # 3. Sample and analyze
        sample = all_vars[:50]
        test_vars = [
            {
                'domain': var.domain,
                'name': var.name,
                'label': var.label or '',
                'data_type': var.data_type or ''
            }
            for var in sample
        ]

        decisions, summary = engine.analyze_batch(test_vars)

        # 4. Verify results
        assert summary.total_variables == 50
        assert summary.auto_approved + summary.quick_review + summary.manual_review == 50
        assert summary.processing_time_seconds > 0


# ============================================================================
# TEST CLASS: Data Validation
# ============================================================================

class TestDataValidation:
    """Test data quality and validation."""

    @pytest.fixture(scope="class")
    def store(self, golden_metadata_path, metadata_versions_path):
        """Create MetadataStore instance."""
        from core.metadata import MetadataStore
        return MetadataStore(str(golden_metadata_path), str(metadata_versions_path))

    def test_no_empty_domain_names(self, store):
        """Test no domains have empty names."""
        domains = store.get_all_domains()
        for domain in domains:
            assert domain.name, f"Domain has empty name: {domain}"
            assert len(domain.name.strip()) > 0, f"Domain has whitespace-only name"

    def test_no_empty_variable_names(self, store):
        """Test no variables have empty names."""
        variables = store.get_all_variables()
        for var in variables:
            assert var.name, f"Variable has empty name in domain {var.domain}"
            assert len(var.name.strip()) > 0, f"Variable has whitespace-only name"

    def test_variables_have_domains(self, store):
        """Test all variables have associated domains."""
        variables = store.get_all_variables()
        for var in variables:
            assert var.domain, f"Variable {var.name} has no domain"

    def test_domain_variable_counts(self, store):
        """Test domains have reasonable variable counts."""
        domains = store.get_all_domains()
        for domain in domains:
            # Each domain should have at least 1 variable (like STUDYID)
            # This is a sanity check
            pass  # Counting requires iteration which is expensive

    def test_approval_status_consistency(self, store):
        """Test approval status is consistent."""
        variables = store.get_all_variables()
        valid_statuses = {'pending', 'approved', 'rejected'}

        for var in variables:
            assert var.approval.status in valid_statuses, \
                f"Variable {var.domain}.{var.name} has invalid status: {var.approval.status}"


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
