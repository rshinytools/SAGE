# tests/factory4/test_accuracy_components.py
"""
Tests for SAGE Accuracy Components

Tests for:
- QueryAnalyzer: Structured query understanding
- ClarificationManager: Ambiguous query handling
- AnswerVerifier: Result validation
- SAGEResponse: Unified response format
- SessionMemory: Conversation context
- DataDrivenKnowledge: Learning from data
"""

import pytest
from datetime import datetime
from dataclasses import asdict

from core.engine.query_analyzer import (
    QueryAnalyzer, QueryAnalysis, QueryIntent, QuerySubject,
    QueryCondition, Ambiguity
)
from core.engine.clarification_manager import (
    ClarificationManager, ClarificationRequest, ClarificationQuestion,
    ClarificationOption
)
from core.engine.answer_verifier import (
    AnswerVerifier, VerificationResult, VerificationCheck
)
from core.engine.response_format import (
    SAGEResponse, SAGEResponseBuilder, ResponseType,
    MethodologyInfo, EntityResolution,
    standardize_confidence, get_confidence_level, confidence_to_color
)
from core.engine.session_memory import (
    SessionMemory, SessionManager, SessionContext,
    ConversationTurn, get_session_manager
)
from core.engine.data_knowledge import (
    DataKnowledge, ColumnKnowledge, PatternKnowledge, Suggestion,
    SuggestionEngine
)
from core.engine.models import ExecutionResult
from core.engine.pipeline import InferencePipeline, PipelineConfig


# =============================================================================
# QUERY ANALYZER TESTS
# =============================================================================

class TestQueryIntent:
    """Test QueryIntent enum."""

    def test_intent_values(self):
        """All intent values should be valid."""
        intents = [
            QueryIntent.COUNT_SUBJECTS,
            QueryIntent.COUNT_EVENTS,
            QueryIntent.COUNT_RECORDS,
            QueryIntent.LIST_VALUES,
            QueryIntent.LIST_RECORDS,
            QueryIntent.LIST_TOP_N,
            QueryIntent.DISTRIBUTION,
            QueryIntent.COMPARE_GROUPS,
            QueryIntent.TREND_OVER_TIME,
            QueryIntent.EXISTS,
            QueryIntent.AVERAGE,
            QueryIntent.UNCLEAR
        ]
        assert len(intents) == 12

    def test_intent_to_string(self):
        """Intent should convert to string."""
        assert QueryIntent.COUNT_SUBJECTS.value == "count_subjects"
        assert QueryIntent.LIST_TOP_N.value == "list_top_n"


class TestQuerySubject:
    """Test QuerySubject enum."""

    def test_subject_values(self):
        """All subject values should be valid."""
        subjects = [
            QuerySubject.SUBJECTS,
            QuerySubject.EVENTS,
            QuerySubject.RECORDS,
            QuerySubject.SITES,
            QuerySubject.PARAMETERS
        ]
        assert len(subjects) == 5


class TestQueryCondition:
    """Test QueryCondition dataclass."""

    def test_condition_creation(self):
        """Should create condition with all fields."""
        condition = QueryCondition(
            original_text="sick from drug",
            cdisc_concept="Treatment-related AE",
            column="AEREL",
            operator="IN",
            values=["DRUG", "PROBABLE", "POSSIBLE"],
            mapping_confidence=0.9
        )
        assert condition.column == "AEREL"
        assert len(condition.values) == 3
        assert condition.mapping_confidence == 0.9


class TestAmbiguity:
    """Test Ambiguity dataclass."""

    def test_ambiguity_creation(self):
        """Should create ambiguity with options."""
        ambiguity = Ambiguity(
            text="sick",
            interpretations=["adverse event", "serious adverse event"],
            clarification_needed=True
        )
        assert ambiguity.clarification_needed is True
        assert len(ambiguity.interpretations) == 2


class TestQueryAnalysis:
    """Test QueryAnalysis dataclass."""

    def test_analysis_creation(self):
        """Should create full analysis."""
        analysis = QueryAnalysis(
            original_query="How many patients had nausea?",
            intent=QueryIntent.COUNT_SUBJECTS,
            subject=QuerySubject.SUBJECTS,
            conditions=[
                QueryCondition(
                    original_text="nausea",
                    cdisc_concept="Adverse Event Term",
                    column="AEDECOD",
                    operator="=",
                    values=["NAUSEA"],
                    mapping_confidence=0.95
                )
            ],
            understanding_confidence=0.92,
            suggested_table="ADAE",
            suggested_population="Safety"
        )
        assert analysis.intent == QueryIntent.COUNT_SUBJECTS
        assert analysis.understanding_confidence == 0.92
        assert len(analysis.conditions) == 1

    def test_analysis_needs_clarification_low_confidence(self):
        """Should flag for clarification when confidence is low."""
        analysis = QueryAnalysis(
            original_query="How many got sick?",
            intent=QueryIntent.UNCLEAR,
            subject=QuerySubject.SUBJECTS,  # Default to SUBJECTS for unclear queries
            conditions=[],
            understanding_confidence=0.4,
            needs_clarification=True
        )
        assert analysis.needs_clarification is True


# =============================================================================
# CLARIFICATION MANAGER TESTS
# =============================================================================

class TestClarificationManager:
    """Test ClarificationManager class."""

    @pytest.fixture
    def manager(self):
        return ClarificationManager()

    def test_needs_clarification_low_confidence(self, manager):
        """Should need clarification when confidence is low."""
        analysis = QueryAnalysis(
            original_query="test",
            intent=QueryIntent.COUNT_SUBJECTS,
            subject=QuerySubject.SUBJECTS,
            conditions=[],
            understanding_confidence=0.5
        )
        assert manager.needs_clarification(analysis) is True

    def test_no_clarification_high_confidence(self, manager):
        """Should not need clarification when confidence is high."""
        analysis = QueryAnalysis(
            original_query="How many patients had nausea?",
            intent=QueryIntent.COUNT_SUBJECTS,
            subject=QuerySubject.SUBJECTS,
            conditions=[],
            understanding_confidence=0.9
        )
        assert manager.needs_clarification(analysis) is False

    def test_needs_clarification_with_ambiguities(self, manager):
        """Should need clarification when ambiguities detected."""
        analysis = QueryAnalysis(
            original_query="test",
            intent=QueryIntent.COUNT_SUBJECTS,
            subject=QuerySubject.SUBJECTS,
            conditions=[],
            understanding_confidence=0.8,
            ambiguities=[
                Ambiguity(
                    text="sick",
                    interpretations=["AE", "SAE"],
                    clarification_needed=True
                )
            ]
        )
        assert manager.needs_clarification(analysis) is True

    def test_needs_clarification_unclear_intent(self, manager):
        """Should need clarification when intent is unclear."""
        analysis = QueryAnalysis(
            original_query="test",
            intent=QueryIntent.UNCLEAR,
            subject=QuerySubject.SUBJECTS,  # Default to SUBJECTS
            conditions=[],
            understanding_confidence=0.8
        )
        assert manager.needs_clarification(analysis) is True

    def test_generate_clarification_request(self, manager):
        """Should generate clarification request."""
        analysis = QueryAnalysis(
            original_query="test query",
            intent=QueryIntent.UNCLEAR,
            subject=QuerySubject.SUBJECTS,  # Default to SUBJECTS
            conditions=[],
            understanding_confidence=0.4
        )
        request = manager.generate_clarification_request(analysis)
        assert isinstance(request, ClarificationRequest)
        assert request.original_query == "test query"
        assert len(request.questions) > 0


class TestClarificationRequest:
    """Test ClarificationRequest dataclass."""

    def test_to_dict(self):
        """Should convert to dictionary."""
        request = ClarificationRequest(
            original_query="test",
            message="Need clarification",
            questions=[
                ClarificationQuestion(
                    question="Do you mean?",
                    context="Term is ambiguous",
                    options=[
                        ClarificationOption(id=1, text="Option A", interpretation="A"),
                        ClarificationOption(id=2, text="Option B", interpretation="B")
                    ]
                )
            ]
        )
        result = request.to_dict()
        assert result['type'] == 'clarification_needed'
        assert len(result['questions']) == 1


# =============================================================================
# ANSWER VERIFIER TESTS
# =============================================================================

class TestAnswerVerifier:
    """Test AnswerVerifier class."""

    @pytest.fixture
    def verifier(self):
        return AnswerVerifier()

    def test_verify_success(self, verifier):
        """Should verify successful execution."""
        analysis = QueryAnalysis(
            original_query="How many patients had nausea?",
            intent=QueryIntent.COUNT_SUBJECTS,
            subject=QuerySubject.SUBJECTS,
            conditions=[],
            understanding_confidence=0.9
        )
        execution = ExecutionResult(
            success=True,
            data=[{"count": 82}],
            row_count=1
        )
        sql = "SELECT COUNT(DISTINCT USUBJID) FROM ADAE WHERE AEDECOD='NAUSEA'"

        result = verifier.verify(
            query="How many patients had nausea?",
            analysis=analysis,
            sql=sql,
            result=execution
        )

        assert isinstance(result, VerificationResult)
        assert len(result.checks) > 0
        assert result.overall_score > 0

    def test_verify_failed_execution(self, verifier):
        """Should handle failed execution."""
        analysis = QueryAnalysis(
            original_query="test",
            intent=QueryIntent.COUNT_SUBJECTS,
            subject=QuerySubject.SUBJECTS,
            conditions=[],
            understanding_confidence=0.9
        )
        execution = ExecutionResult(
            success=False,
            error_message="Table not found"
        )

        result = verifier.verify(
            query="test",
            analysis=analysis,
            sql="SELECT * FROM MISSING",
            result=execution
        )

        assert result.passed is False

    def test_verify_intent_mismatch(self, verifier):
        """Should detect intent mismatch."""
        analysis = QueryAnalysis(
            original_query="Count patients",
            intent=QueryIntent.COUNT_SUBJECTS,
            subject=QuerySubject.SUBJECTS,
            conditions=[],
            understanding_confidence=0.9
        )
        execution = ExecutionResult(
            success=True,
            data=[{"name": "John"}],
            row_count=1
        )
        # SQL without COUNT
        sql = "SELECT USUBJID FROM ADAE"

        result = verifier.verify(
            query="Count patients",
            analysis=analysis,
            sql=sql,
            result=execution
        )

        # Should have low score for intent mismatch
        intent_check = next((c for c in result.checks if c.name == "intent_match"), None)
        assert intent_check is not None
        assert intent_check.score < 1.0


class TestVerificationCheck:
    """Test VerificationCheck dataclass."""

    def test_check_creation(self):
        """Should create check with all fields."""
        check = VerificationCheck(
            name="intent_match",
            passed=True,
            score=1.0,
            weight=0.3
        )
        assert check.passed is True
        assert check.weight == 0.3


# =============================================================================
# SAGE RESPONSE TESTS
# =============================================================================

class TestSAGEResponse:
    """Test SAGEResponse class."""

    def test_response_creation(self):
        """Should create response with all fields."""
        response = SAGEResponse(
            query="How many patients?",
            type=ResponseType.ANSWER,
            answer="82 patients",
            confidence_score=95,
            confidence_level="high"
        )
        assert response.query == "How many patients?"
        assert response.confidence_score == 95

    def test_response_to_dict(self):
        """Should convert to dictionary."""
        response = SAGEResponse(
            query="test",
            type=ResponseType.ANSWER,
            answer="42",
            confidence_score=85,
            confidence_level="high"
        )
        result = response.to_dict()
        assert result['query'] == "test"
        assert result['type'] == "answer"
        assert result['confidence']['score'] == 85

    def test_response_to_api_response(self):
        """Should convert to API format."""
        response = SAGEResponse(
            query="test",
            type=ResponseType.ANSWER,
            answer="result"
        )
        api_response = response.to_api_response()
        assert 'response_id' in api_response


class TestSAGEResponseBuilder:
    """Test SAGEResponseBuilder class."""

    def test_build_success(self):
        """Should build successful response."""
        response = SAGEResponseBuilder.success(
            query="How many?",
            answer="82 patients",
            confidence_score=90,
            confidence_level="high"
        )
        assert response.type == ResponseType.ANSWER
        assert response.confidence_score == 90

    def test_build_clarification(self):
        """Should build clarification response."""
        response = SAGEResponseBuilder.clarification_needed(
            query="test",
            message="Need more info",
            questions=[{"question": "Do you mean?", "options": []}]
        )
        assert response.type == ResponseType.CLARIFICATION

    def test_build_error(self):
        """Should build error response."""
        response = SAGEResponseBuilder.error(
            query="test",
            message="Something went wrong",
            stage="execution"
        )
        assert response.type == ResponseType.ERROR
        assert response.error is not None

    def test_build_greeting(self):
        """Should build greeting response."""
        response = SAGEResponseBuilder.greeting(
            query="Hi",
            answer="Hello!"
        )
        assert response.type == ResponseType.GREETING
        assert response.confidence_score == 100

    def test_build_help(self):
        """Should build help response."""
        response = SAGEResponseBuilder.help_response(
            query="help",
            answer="I can help with..."
        )
        assert response.type == ResponseType.HELP


class TestConfidenceHelpers:
    """Test confidence helper functions."""

    def test_standardize_from_decimal(self):
        """Should convert 0-1 to 0-100."""
        assert standardize_confidence(0.95) == 95
        assert standardize_confidence(0.5) == 50

    def test_standardize_from_percentage(self):
        """Should keep 0-100 as is."""
        assert standardize_confidence(95) == 95
        assert standardize_confidence(50) == 50

    def test_standardize_clamps_values(self):
        """Should clamp to 0-100."""
        assert standardize_confidence(150) == 100
        assert standardize_confidence(-10) == 0

    def test_get_confidence_level(self):
        """Should return correct level."""
        assert get_confidence_level(95) == "high"
        assert get_confidence_level(80) == "medium"
        assert get_confidence_level(60) == "low"
        assert get_confidence_level(30) == "very_low"

    def test_confidence_to_color(self):
        """Should return correct color."""
        assert confidence_to_color("high") == "green"
        assert confidence_to_color("medium") == "yellow"
        assert confidence_to_color("low") == "orange"
        assert confidence_to_color("very_low") == "red"


# =============================================================================
# SESSION MEMORY TESTS
# =============================================================================

class TestSessionMemory:
    """Test SessionMemory class."""

    @pytest.fixture
    def session(self):
        return SessionMemory()

    def test_session_creation(self, session):
        """Should create session with ID."""
        assert session.session_id is not None
        assert session.session_id.startswith("sess_")

    def test_add_turn(self, session):
        """Should add conversation turn."""
        session.add_turn(
            query="How many patients?",
            response_type="answer",
            answer="82 patients"
        )
        assert len(session.history) == 1
        assert session.history[0].query == "How many patients?"

    def test_resolve_references_those(self, session):
        """Reference resolution is now handled by LLM QueryAnalyzer, not pattern matching.

        The resolve_references method was intentionally changed to return (query, False)
        because the LLM-based QueryAnalyzer handles follow-up detection. This allows the
        LLM to understand the infinite ways users might phrase follow-up questions.
        See query_analyzer.py CONTEXT_AWARE_ANALYSIS_PROMPT for follow-up detection.
        """
        # First query establishes context
        session.add_turn(
            query="How many patients had nausea?",
            response_type="answer",
            answer="82 subjects had nausea",
            entities=["NAUSEA"]
        )
        # Second query uses reference - LLM handles this now, not pattern matching
        resolved, modified = session.resolve_references("How many of those were serious?")
        # Pattern matching removed - LLM handles reference resolution via QueryAnalyzer
        assert modified is False
        assert resolved == "How many of those were serious?"

    def test_resolve_references_no_context(self):
        """Should not modify when no context."""
        session = SessionMemory()
        resolved, modified = session.resolve_references("How many patients?")
        assert modified is False
        assert resolved == "How many patients?"

    def test_has_context(self, session):
        """Should track context availability."""
        assert session.has_context() is False
        session.add_turn(query="test", response_type="answer", answer="result")
        assert session.has_context() is True

    def test_get_last_turn(self, session):
        """Should get last turn."""
        session.add_turn(query="first", response_type="answer", answer="1")
        session.add_turn(query="second", response_type="answer", answer="2")
        last = session.get_last_turn()
        assert last.query == "second"

    def test_clear(self, session):
        """Should clear history."""
        session.add_turn(query="test", response_type="answer", answer="result")
        session.clear()
        assert len(session.history) == 0
        assert session.has_context() is False

    def test_to_dict(self, session):
        """Should convert to dictionary."""
        session.add_turn(query="test", response_type="answer", answer="result")
        result = session.to_dict()
        assert 'session_id' in result
        assert result['turn_count'] == 1


class TestSessionManager:
    """Test SessionManager class."""

    @pytest.fixture
    def manager(self):
        return SessionManager()

    def test_create_session(self, manager):
        """Should create new session."""
        session = manager.create_session()
        assert session is not None
        assert session.session_id in manager.sessions

    def test_get_session_creates_new(self, manager):
        """Should create session if not exists."""
        session = manager.get_session("new_session")
        assert session is not None
        assert session.session_id == "new_session"

    def test_get_session_returns_existing(self, manager):
        """Should return existing session."""
        session1 = manager.get_session("test_id")
        session1.add_turn(query="test", response_type="answer", answer="result")
        session2 = manager.get_session("test_id")
        assert len(session2.history) == 1

    def test_remove_session(self, manager):
        """Should remove session."""
        manager.create_session()
        sid = list(manager.sessions.keys())[0]
        manager.remove_session(sid)
        assert sid not in manager.sessions

    def test_get_active_count(self, manager):
        """Should count active sessions."""
        manager.create_session()
        manager.create_session()
        assert manager.get_active_session_count() == 2


class TestPipelineSessionSwitching:
    """Test pipeline's session switching for multi-conversation support."""

    @pytest.fixture
    def mock_pipeline(self, mock_available_tables):
        """Create a mock pipeline with session support enabled."""
        config = PipelineConfig(
            db_path="",
            metadata_path="",
            use_mock=True,
            available_tables=mock_available_tables,
            enable_session_memory=True
        )
        return InferencePipeline(config)

    def test_switch_session_creates_new(self, mock_pipeline):
        """Switching to a new session should create it."""
        original_session = mock_pipeline.session_id
        mock_pipeline.switch_session("new-session-123")
        assert mock_pipeline.session_id == "new-session-123"
        assert mock_pipeline.session_id != original_session

    def test_switch_session_same_id_no_change(self, mock_pipeline):
        """Switching to same session should not change anything."""
        mock_pipeline.switch_session("same-session")
        first_session = mock_pipeline.session
        mock_pipeline.switch_session("same-session")
        # Should be the same session object
        assert mock_pipeline.session is first_session

    def test_process_with_session_switches(self, mock_pipeline):
        """process_with_session should switch to specified session."""
        result = mock_pipeline.process_with_session(
            "How many patients had headaches?",
            session_id="conversation-456"
        )
        assert mock_pipeline.session_id == "conversation-456"
        assert result is not None

    def test_process_with_session_none_uses_current(self, mock_pipeline):
        """process_with_session with None session uses current session."""
        original_session = mock_pipeline.session_id
        result = mock_pipeline.process_with_session("How many patients?")
        assert mock_pipeline.session_id == original_session

    def test_session_context_persists_across_calls(self, mock_pipeline):
        """Session ID should persist across multiple calls with same conversation."""
        session_id = "persistent-session"

        # First call with session
        mock_pipeline.process_with_session(
            "How many patients had headaches?",
            session_id=session_id
        )

        # Verify we're using the right session
        assert mock_pipeline.session_id == session_id
        first_session = mock_pipeline.session

        # Second call with same session
        mock_pipeline.process_with_session(
            "List them",
            session_id=session_id
        )

        # Should still be using same session (not created a new one)
        assert mock_pipeline.session_id == session_id
        assert mock_pipeline.session is first_session

    def test_different_sessions_isolated(self, mock_pipeline):
        """Different conversation IDs should use different sessions."""
        # Call with session A
        mock_pipeline.process_with_session(
            "How many patients had headaches?",
            session_id="session-A"
        )
        session_a = mock_pipeline.session

        # Call with session B
        mock_pipeline.process_with_session(
            "Show lab values",
            session_id="session-B"
        )
        session_b = mock_pipeline.session

        # Sessions should be different
        assert session_a is not session_b
        assert mock_pipeline.session_id == "session-B"

        # Switch back to A
        mock_pipeline.process_with_session(
            "Any more?",
            session_id="session-A"
        )
        # Should get back session A
        assert mock_pipeline.session is session_a


# =============================================================================
# DATA KNOWLEDGE TESTS
# =============================================================================

class TestDataKnowledge:
    """Test DataKnowledge class."""

    @pytest.fixture
    def knowledge(self):
        return DataKnowledge()

    def test_add_column_knowledge(self, knowledge):
        """Should add column knowledge."""
        col_knowledge = ColumnKnowledge(
            table="ADAE",
            column="AEDECOD",
            data_type="VARCHAR",
            distinct_count=50,
            sample_values=["NAUSEA", "HEADACHE", "FATIGUE"]
        )
        knowledge.add_column_knowledge("ADAE", "AEDECOD", col_knowledge)
        assert "ADAE" in knowledge.columns
        assert "AEDECOD" in knowledge.columns["ADAE"]

    def test_find_column_for_value(self, knowledge):
        """Should find column containing value."""
        col_knowledge = ColumnKnowledge(
            table="ADAE",
            column="AEDECOD",
            data_type="VARCHAR",
            sample_values=["NAUSEA", "HEADACHE"]
        )
        knowledge.add_column_knowledge("ADAE", "AEDECOD", col_knowledge)

        results = knowledge.find_column_for_value("nausea")
        assert len(results) > 0
        assert results[0][0] == "ADAE"
        assert results[0][1] == "AEDECOD"

    def test_add_pattern(self, knowledge):
        """Should add discovered pattern."""
        pattern = PatternKnowledge(
            pattern_type="value_location",
            description="AE terms are in AEDECOD",
            table="ADAE",
            column="AEDECOD"
        )
        knowledge.add_pattern(pattern)
        assert len(knowledge.patterns) == 1

    def test_to_dict(self, knowledge):
        """Should convert to dictionary."""
        result = knowledge.to_dict()
        assert 'tables' in result
        assert 'total_columns' in result
        assert 'patterns_count' in result


class TestColumnKnowledge:
    """Test ColumnKnowledge dataclass."""

    def test_creation(self):
        """Should create with all fields."""
        col = ColumnKnowledge(
            table="ADAE",
            column="AEDECOD",
            data_type="VARCHAR",
            distinct_count=100,
            sample_values=["A", "B", "C"],
            value_frequencies={"A": 50, "B": 30, "C": 20},
            has_nulls=True,
            null_percentage=5.0
        )
        assert col.distinct_count == 100
        assert col.has_nulls is True


class TestSuggestionEngine:
    """Test SuggestionEngine class."""

    @pytest.fixture
    def engine(self):
        knowledge = DataKnowledge()
        col_knowledge = ColumnKnowledge(
            table="ADAE",
            column="AEDECOD",
            data_type="VARCHAR",
            sample_values=["NAUSEA", "HEADACHE", "VOMITING"]
        )
        knowledge.add_column_knowledge("ADAE", "AEDECOD", col_knowledge)
        return SuggestionEngine(knowledge)

    def test_find_column(self, engine):
        """Should find column for term."""
        result = engine.find_column("nausea")
        assert result is not None
        assert result[0] == "ADAE"
        assert result[1] == "AEDECOD"


# =============================================================================
# PIPELINE INTEGRATION TESTS
# =============================================================================

class TestPipelineAccuracyComponents:
    """Test accuracy components in pipeline."""

    @pytest.fixture
    def mock_pipeline(self):
        from core.engine.pipeline import InferencePipeline, PipelineConfig
        config = PipelineConfig(
            use_mock=True,
            enable_query_analysis=False,  # Disable to avoid Claude calls in unit tests
            enable_clarification=True,
            enable_verification=True,
            enable_session_memory=True,
            enable_data_knowledge=False
        )
        return InferencePipeline(config=config)

    def test_pipeline_has_accuracy_components(self, mock_pipeline):
        """Pipeline should initialize accuracy components."""
        assert mock_pipeline.clarification_manager is not None
        assert mock_pipeline.answer_verifier is not None
        assert mock_pipeline.session is not None

    def test_pipeline_session_id(self, mock_pipeline):
        """Pipeline should have session ID."""
        assert mock_pipeline.session_id is not None
        assert mock_pipeline.session_id.startswith("sess_")

    def test_pipeline_is_ready(self, mock_pipeline):
        """Pipeline should report component status."""
        status = mock_pipeline.is_ready()
        assert 'clarification_manager' in status
        assert 'answer_verifier' in status
        assert 'session_memory' in status

    def test_pipeline_get_session_context(self, mock_pipeline):
        """Pipeline should provide session context."""
        context = mock_pipeline.get_session_context()
        assert context is not None
        assert 'session_id' in context


class TestPipelineConfigAccuracyOptions:
    """Test PipelineConfig accuracy options."""

    def test_default_accuracy_options(self):
        from core.engine.pipeline import PipelineConfig
        config = PipelineConfig()
        assert config.enable_query_analysis is True
        assert config.enable_clarification is True
        assert config.enable_verification is True
        assert config.enable_session_memory is True
        assert config.enable_data_knowledge is True

    def test_clarification_threshold(self):
        from core.engine.pipeline import PipelineConfig
        config = PipelineConfig(clarification_threshold=0.8)
        assert config.clarification_threshold == 0.8

    def test_verification_threshold(self):
        from core.engine.pipeline import PipelineConfig
        config = PipelineConfig(verification_threshold=0.6)
        assert config.verification_threshold == 0.6
