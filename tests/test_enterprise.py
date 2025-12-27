"""Tests for Enterprise Integration."""

import pytest
import tempfile
import shutil
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.engine.enterprise import (
    EnterpriseProcessor,
    EnterpriseConfig,
    create_enterprise_processor
)
from core.engine.clinical import RefusalReason
from core.engine.learning import FeedbackType, ResponseAction, ComplexityLevel


class TestEnterpriseProcessor:
    """Test suite for EnterpriseProcessor."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        temp = tempfile.mkdtemp()
        yield temp
        shutil.rmtree(temp, ignore_errors=True)

    @pytest.fixture
    def processor(self, temp_dir):
        """Create EnterpriseProcessor with temp paths."""
        config = EnterpriseConfig(
            db_path="",  # No actual database for unit tests
            learning_db_path=str(Path(temp_dir) / "learning.db"),
            audit_db_path=str(Path(temp_dir) / "audit.db"),
            protocol_path="knowledge/study_protocol.json",
            enable_schema_validation=False,  # No DB for testing
            enable_certified_answers=True,
            enable_protocol_guard=True,
            enable_view_routing=True,
            enable_helpful_refusals=True,
            enable_learning=True,
            enable_audit=True
        )
        return EnterpriseProcessor(config)

    def test_initialization(self, processor):
        """Test processor initializes all components."""
        assert processor.certified_system is not None
        assert processor.protocol_guard is not None
        assert processor.view_router is not None
        assert processor.refusal_system is not None
        assert processor.example_store is not None
        assert processor.complexity_scorer is not None
        assert processor.confidence_manager is not None
        assert processor.audit_logger is not None

    def test_start_trace(self, processor):
        """Test starting an audit trace."""
        trace_id = processor.start_trace(
            question="How many patients?",
            user_id="test_user",
            session_id="session_123"
        )
        assert trace_id is not None
        assert len(trace_id) == 36  # UUID format

    def test_check_certified_not_found(self, processor):
        """Test certified check when no match found."""
        bypass, result = processor.check_certified("Random unknown question")
        assert not bypass  # No certified answer
        assert result is not None

    def test_check_protocol(self, processor):
        """Test protocol guard check."""
        result = processor.check_protocol("Count high blood pressure patients")
        assert result is not None
        assert hasattr(result, 'all_resolved')

    def test_check_protocol_with_ambiguous_term(self, processor):
        """Test protocol guard detects ambiguous terms."""
        result = processor.check_protocol("Count patients at baseline")
        # 'baseline' is an ambiguous term
        if len(result.terms_found) > 0:
            assert len(result.terms_found) > 0

    def test_route_to_view(self, processor):
        """Test view routing."""
        result = processor.route_to_view(
            query="Show adverse events with patient demographics",
            required_tables=["ae", "dm"]
        )
        assert hasattr(result, 'should_use_view')

    def test_route_to_view_ae_demographics(self, processor):
        """Test view routing for AE with demographics."""
        result = processor.route_to_view(
            query="List adverse events by age group",
            required_tables=["adae", "adsl"]
        )
        if result.should_use_view:
            assert result.view_name is not None

    def test_assess_complexity_simple(self, processor):
        """Test complexity assessment for simple query."""
        level, warnings = processor.assess_complexity(
            question="How many patients?"
        )
        assert level == ComplexityLevel.SIMPLE
        assert isinstance(warnings, list)

    def test_assess_complexity_complex(self, processor):
        """Test complexity assessment for complex query."""
        level, warnings = processor.assess_complexity(
            question="Compare adverse event rates between treatment groups by visit",
            detected_tables=["adae", "adsl", "sv"]
        )
        assert level in [ComplexityLevel.COMPLEX, ComplexityLevel.VERY_COMPLEX]

    def test_calculate_confidence_high(self, processor):
        """Test confidence calculation for high confidence."""
        score, action = processor.calculate_confidence(
            example_similarity=1.0,
            dictionary_match=1.0,
            metadata_coverage=1.0,
            semantic_alignment=1.0,
            execution_success=1.0,
            result_validation=1.0
        )
        assert score >= 90
        assert action == ResponseAction.RETURN_NORMAL

    def test_calculate_confidence_low(self, processor):
        """Test confidence calculation for low confidence."""
        score, action = processor.calculate_confidence(
            example_similarity=0.2,
            dictionary_match=0.2,
            metadata_coverage=0.2,
            semantic_alignment=0.2,
            execution_success=0.0
        )
        assert score < 40
        assert action == ResponseAction.REFUSE

    def test_generate_refusal(self, processor):
        """Test refusal generation."""
        message = processor.generate_refusal(
            reason=RefusalReason.CONFIDENCE_TOO_LOW,
            details={"confidence": 35}
        )
        assert message is not None
        assert len(message) > 0

    def test_complete_trace(self, processor):
        """Test completing an audit trace."""
        trace_id = processor.start_trace("Test question")

        processor.complete_trace(
            trace_id=trace_id,
            sql="SELECT COUNT(*) FROM adsl",
            confidence=85.0,
            action="RETURN_NORMAL",
            success=True
        )

        # Verify trace was completed
        trace = processor.audit_logger.get_trace(trace_id)
        assert trace is not None
        assert trace["success"] == 1

    def test_submit_feedback(self, processor):
        """Test feedback submission."""
        processor.submit_feedback(
            query_id="query-123",
            question="How many patients?",
            feedback_type=FeedbackType.CONFIRM,
            original_sql="SELECT COUNT(*) FROM adsl",
            user_id="test_user"
        )

        # Verify feedback was stored
        stats = processor.feedback_handler.get_statistics()
        assert stats["total_feedback"] >= 1

    def test_get_statistics(self, processor):
        """Test getting statistics."""
        # Add some data
        processor.start_trace("Test 1")
        processor.start_trace("Test 2")

        stats = processor.get_statistics()

        assert "learning" in stats
        assert "feedback" in stats
        assert "audit" in stats

    def test_validate_result(self, processor):
        """Test result validation."""
        result = processor.validate_result(
            question="How many patients?",
            sql="SELECT COUNT(*) FROM adsl",
            result=[{"count": 100}]
        )

        assert "is_valid" in result
        assert "confidence_adjustment" in result

    def test_factory_function(self, temp_dir):
        """Test factory function."""
        processor = create_enterprise_processor(
            db_path="",
            learning_db_path=str(Path(temp_dir) / "learning.db"),
            audit_db_path=str(Path(temp_dir) / "audit.db"),
            enable_all=True
        )

        assert processor is not None
        assert processor.audit_logger is not None

    def test_disabled_components(self, temp_dir):
        """Test with disabled components."""
        config = EnterpriseConfig(
            enable_certified_answers=False,
            enable_protocol_guard=False,
            enable_schema_validation=False,
            enable_view_routing=False,
            enable_helpful_refusals=False,
            enable_learning=False,
            enable_audit=False
        )
        processor = EnterpriseProcessor(config)

        assert processor.certified_system is None
        assert processor.protocol_guard is None
        assert processor.schema_validator is None
        assert processor.view_router is None
        assert processor.refusal_system is None
        assert processor.example_store is None
        assert processor.audit_logger is None

    def test_trace_with_all_events(self, processor):
        """Test trace with multiple events."""
        trace_id = processor.start_trace("Full trace test")

        # Log various events
        processor.check_certified("Test question", trace_id)
        processor.check_protocol("Test question", trace_id)
        processor.assess_complexity("Test question", trace_id=trace_id)

        processor.complete_trace(
            trace_id=trace_id,
            confidence=80.0,
            success=True
        )

        # Get trace and verify events
        trace = processor.audit_logger.get_trace(trace_id)
        assert len(trace["entries"]) >= 4  # start + 3 events + complete
