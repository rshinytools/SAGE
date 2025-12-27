"""
Enterprise Integration Module for SAGE.

Integrates all enterprise-grade components for clinical accuracy:
- CertifiedAnswerSystem: 100% accuracy for known queries
- ProtocolGuard: Resolve ambiguous clinical terms
- SchemaValidator: Validate SQL against live schema
- GoldenViewRouter: Route complex queries to pre-joined views
- HelpfulRefusalSystem: Generate actionable refusals
- Learning components: Continuous improvement
- AuditTraceLogger: 21 CFR Part 11 compliant logging
"""

import logging
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from pathlib import Path

# Clinical components
from .clinical import (
    ProtocolGuard,
    ProtocolGuardResult,
    CertifiedAnswerSystem,
    CertificationResult,
    CertificationLevel,
    SchemaValidator,
    SchemaValidationResult,
    GoldenViewRouter,
    ViewRoutingResult,
    HelpfulRefusalSystem,
    RefusalReason
)

# Learning components
from .learning import (
    ExampleStore,
    ComplexityScorer,
    ComplexityLevel,
    SemanticValidator,
    ValidationResult as SemanticValidationResult,
    ResultValidator,
    ConfidenceManager,
    ResponseAction,
    FeedbackHandler,
    FeedbackType
)

# Audit components
from .audit import (
    AuditTraceLogger,
    AuditEvent,
    AuditLevel
)

logger = logging.getLogger(__name__)


@dataclass
class EnterpriseConfig:
    """Configuration for enterprise features."""

    # Enable certified answer bypass (skip LLM for high-match queries)
    enable_certified_answers: bool = True
    certified_threshold: float = 0.98

    # Enable protocol guard (resolve ambiguous terms)
    enable_protocol_guard: bool = True
    protocol_path: str = "knowledge/study_protocol.json"

    # Enable schema validation (validate SQL against live schema)
    enable_schema_validation: bool = True

    # Enable golden view routing (use pre-joined views)
    enable_view_routing: bool = True

    # Enable helpful refusals (actionable error messages)
    enable_helpful_refusals: bool = True

    # Enable learning components
    enable_learning: bool = True
    learning_db_path: str = "data/learning.db"
    chroma_path: str = "knowledge/chroma"

    # Enable audit logging
    enable_audit: bool = True
    audit_db_path: str = "data/audit.db"

    # Database path for schema validation
    db_path: str = ""


@dataclass
class EnterpriseResult:
    """Result from enterprise processing."""
    # Certified answer bypass
    certified: bool = False
    certification_level: Optional[CertificationLevel] = None
    certified_sql: Optional[str] = None

    # Protocol resolution
    protocol_checked: bool = False
    terms_resolved: Dict[str, str] = field(default_factory=dict)
    clarification_needed: bool = False
    clarification_questions: List[str] = field(default_factory=list)

    # Schema validation
    schema_valid: bool = True
    schema_issues: List[str] = field(default_factory=list)
    repaired_sql: Optional[str] = None

    # View routing
    view_routed: bool = False
    routed_sql: Optional[str] = None
    view_used: Optional[str] = None

    # Complexity assessment
    complexity: Optional[ComplexityLevel] = None
    complexity_warnings: List[str] = field(default_factory=list)

    # Confidence calculation
    confidence_score: Optional[float] = None
    response_action: Optional[ResponseAction] = None

    # Audit trace
    trace_id: Optional[str] = None


class EnterpriseProcessor:
    """
    Enterprise-grade query processor.

    Wraps all enterprise components and provides a single interface
    for the pipeline to use.
    """

    def __init__(self, config: EnterpriseConfig = None):
        """
        Initialize enterprise processor.

        Args:
            config: Enterprise configuration
        """
        self.config = config or EnterpriseConfig()
        self._init_components()

    def _init_components(self):
        """Initialize all enterprise components."""

        # Certified Answer System
        if self.config.enable_certified_answers:
            # Pass example_store if learning is enabled, threshold is class-level constant
            self.certified_system = CertifiedAnswerSystem()
            logger.info("CertifiedAnswerSystem enabled")
        else:
            self.certified_system = None

        # Protocol Guard
        if self.config.enable_protocol_guard:
            self.protocol_guard = ProtocolGuard(
                protocol_path=self.config.protocol_path
            )
            logger.info("ProtocolGuard enabled")
        else:
            self.protocol_guard = None

        # Schema Validator
        if self.config.enable_schema_validation and self.config.db_path:
            self.schema_validator = SchemaValidator(
                db_path=self.config.db_path
            )
            logger.info("SchemaValidator enabled")
        else:
            self.schema_validator = None

        # Golden View Router
        if self.config.enable_view_routing:
            self.view_router = GoldenViewRouter()
            logger.info("GoldenViewRouter enabled")
        else:
            self.view_router = None

        # Helpful Refusal System
        if self.config.enable_helpful_refusals:
            self.refusal_system = HelpfulRefusalSystem()
            logger.info("HelpfulRefusalSystem enabled")
        else:
            self.refusal_system = None

        # Learning Components
        if self.config.enable_learning:
            self.example_store = ExampleStore(
                db_path=self.config.learning_db_path,
                chroma_path=self.config.chroma_path
            )
            self.complexity_scorer = ComplexityScorer()
            self.semantic_validator = SemanticValidator()
            self.result_validator = ResultValidator(
                db_path=self.config.learning_db_path
            )
            self.confidence_manager = ConfidenceManager()
            self.feedback_handler = FeedbackHandler(
                db_path=self.config.learning_db_path
            )
            logger.info("Learning components enabled")
        else:
            self.example_store = None
            self.complexity_scorer = None
            self.semantic_validator = None
            self.result_validator = None
            self.confidence_manager = None
            self.feedback_handler = None

        # Audit Logger
        if self.config.enable_audit:
            self.audit_logger = AuditTraceLogger(
                db_path=self.config.audit_db_path
            )
            logger.info("AuditTraceLogger enabled")
        else:
            self.audit_logger = None

    def start_trace(
        self,
        question: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> str:
        """
        Start an audit trace for a query.

        Args:
            question: User's question
            user_id: User ID
            session_id: Session ID

        Returns:
            Trace ID
        """
        if self.audit_logger:
            return self.audit_logger.start_trace(
                question=question,
                user_id=user_id,
                session_id=session_id
            )
        return ""

    def check_certified(
        self,
        question: str,
        trace_id: str = ""
    ) -> Tuple[bool, Optional[CertificationResult]]:
        """
        Check if question has a certified answer.

        Args:
            question: User's question
            trace_id: Audit trace ID

        Returns:
            Tuple of (should_bypass_llm, certification_result)
        """
        if not self.certified_system:
            return False, None

        result = self.certified_system.check_certification(question)

        if self.audit_logger and trace_id:
            self.audit_logger.log(
                trace_id=trace_id,
                event=AuditEvent.QUERY_CERTIFIED,
                level=AuditLevel.INFO,
                component="certified_answer",
                message=f"Certification check: {result.certification_level.value}",
                details={
                    "bypass_llm": result.bypass_llm,
                    "confidence": result.confidence,
                    "level": result.certification_level.value
                }
            )

        return result.bypass_llm, result

    def check_protocol(
        self,
        query: str,
        trace_id: str = ""
    ) -> ProtocolGuardResult:
        """
        Check query for ambiguous clinical terms.

        Args:
            query: User's query
            trace_id: Audit trace ID

        Returns:
            ProtocolGuardResult with resolved terms
        """
        if not self.protocol_guard:
            return ProtocolGuardResult(
                query=query,
                enhanced_query=query,
                terms_found=[],
                resolutions=[],
                all_resolved=True,
                clarifications_needed=[],
                sql_enhancements={}
            )

        result = self.protocol_guard.check_query(query)

        if self.audit_logger and trace_id:
            self.audit_logger.log(
                trace_id=trace_id,
                event=AuditEvent.PROTOCOL_CHECKED,
                level=AuditLevel.INFO,
                component="protocol_guard",
                message=f"Protocol check: {len(result.terms_found)} terms found",
                details={
                    "all_resolved": result.all_resolved,
                    "clarifications_needed": result.clarifications_needed,
                    "terms": [t.term for t in result.terms_found]
                }
            )

        return result

    def validate_schema(
        self,
        sql: str,
        trace_id: str = ""
    ) -> SchemaValidationResult:
        """
        Validate SQL against current database schema.

        Args:
            sql: SQL to validate
            trace_id: Audit trace ID

        Returns:
            SchemaValidationResult
        """
        if not self.schema_validator:
            return SchemaValidationResult(
                is_valid=True,
                issues=[],
                can_auto_repair=False
            )

        result = self.schema_validator.validate(sql)

        if self.audit_logger and trace_id:
            self.audit_logger.log(
                trace_id=trace_id,
                event=AuditEvent.SCHEMA_VALIDATED,
                level=AuditLevel.INFO if result.is_valid else AuditLevel.WARNING,
                component="schema_validator",
                message=f"Schema validation: {'valid' if result.is_valid else 'invalid'}",
                details={
                    "is_valid": result.is_valid,
                    "issues": result.issues,
                    "can_auto_repair": result.can_auto_repair
                }
            )

        return result

    def route_to_view(
        self,
        query: str,
        required_tables: List[str],
        trace_id: str = ""
    ) -> ViewRoutingResult:
        """
        Check if query should use a golden view.

        Args:
            query: User's query
            required_tables: Tables required by the query
            trace_id: Audit trace ID

        Returns:
            ViewRoutingResult
        """
        if not self.view_router:
            return ViewRoutingResult(
                should_use_view=False
            )

        result = self.view_router.route(query, required_tables)

        if self.audit_logger and trace_id and result.should_use_view:
            self.audit_logger.log(
                trace_id=trace_id,
                event=AuditEvent.VIEW_ROUTED,
                level=AuditLevel.INFO,
                component="view_router",
                message=f"Query routed to view: {result.view_name}",
                details={
                    "view_name": result.view_name,
                    "reason": result.reason
                }
            )

        return result

    def assess_complexity(
        self,
        question: str,
        detected_tables: List[str] = None,
        detected_columns: List[str] = None,
        trace_id: str = ""
    ) -> Tuple[ComplexityLevel, List[str]]:
        """
        Assess query complexity.

        Args:
            question: User's question
            detected_tables: Tables detected in query
            detected_columns: Columns detected in query
            trace_id: Audit trace ID

        Returns:
            Tuple of (complexity_level, warnings)
        """
        if not self.complexity_scorer:
            return ComplexityLevel.MODERATE, []

        assessment = self.complexity_scorer.assess(
            question=question,
            detected_tables=detected_tables,
            detected_columns=detected_columns
        )

        if self.audit_logger and trace_id:
            self.audit_logger.log(
                trace_id=trace_id,
                event=AuditEvent.CONFIDENCE_CALCULATED,
                level=AuditLevel.INFO,
                component="complexity_scorer",
                message=f"Complexity: {assessment.level.value}",
                details={
                    "level": assessment.level.value,
                    "score": assessment.score,
                    "threshold": assessment.recommended_threshold
                }
            )

        return assessment.level, assessment.warnings

    def validate_result(
        self,
        question: str,
        sql: str,
        result: Any,
        trace_id: str = ""
    ) -> Dict[str, Any]:
        """
        Validate query result for sanity.

        Args:
            question: Original question
            sql: SQL executed
            result: Query result
            trace_id: Audit trace ID

        Returns:
            Validation result dict
        """
        if not self.result_validator:
            return {"is_valid": True}

        validation = self.result_validator.validate(
            question=question,
            sql=sql,
            result=result
        )

        if self.audit_logger and trace_id:
            self.audit_logger.log(
                trace_id=trace_id,
                event=AuditEvent.RESULT_VALIDATED,
                level=AuditLevel.INFO if validation.is_valid else AuditLevel.WARNING,
                component="result_validator",
                message=f"Result validation: {'passed' if validation.is_valid else 'warnings'}",
                details={
                    "is_valid": validation.is_valid,
                    "adjustment": validation.confidence_adjustment,
                    "anomalies": validation.anomalies
                }
            )

        return {
            "is_valid": validation.is_valid,
            "confidence_adjustment": validation.confidence_adjustment,
            "warnings": validation.warnings,
            "anomalies": validation.anomalies
        }

    def calculate_confidence(
        self,
        example_similarity: float = 0.0,
        dictionary_match: float = 0.0,
        metadata_coverage: float = 0.0,
        semantic_alignment: float = 0.0,
        complexity_match: float = 1.0,
        execution_success: float = 0.0,
        result_validation: float = 1.0,
        result_sanity: float = 1.0,
        complexity_adjustment: float = 0.0,
        result_adjustment: float = 0.0,
        trace_id: str = ""
    ) -> Tuple[float, ResponseAction]:
        """
        Calculate final confidence score.

        Args:
            Various confidence component scores
            trace_id: Audit trace ID

        Returns:
            Tuple of (confidence_percentage, response_action)
        """
        if not self.confidence_manager:
            return 75.0, ResponseAction.RETURN_WITH_WARNING

        result = self.confidence_manager.calculate(
            example_similarity=example_similarity,
            dictionary_match=dictionary_match,
            metadata_coverage=metadata_coverage,
            semantic_alignment=semantic_alignment,
            complexity_match=complexity_match,
            execution_success=execution_success,
            result_validation=result_validation,
            result_sanity=result_sanity,
            complexity_adjustment=complexity_adjustment,
            result_adjustment=result_adjustment
        )

        if self.audit_logger and trace_id:
            self.audit_logger.log(
                trace_id=trace_id,
                event=AuditEvent.CONFIDENCE_CALCULATED,
                level=AuditLevel.INFO,
                component="confidence_manager",
                message=f"Confidence: {result.score:.1f}% - {result.action.value}",
                details={
                    "score": result.score,
                    "action": result.action.value,
                    "components": result.components
                }
            )

        return result.score, result.action

    def generate_refusal(
        self,
        reason: RefusalReason,
        details: Dict[str, Any] = None,
        trace_id: str = ""
    ) -> str:
        """
        Generate a helpful refusal message.

        Args:
            reason: Reason for refusal
            details: Additional context
            trace_id: Audit trace ID

        Returns:
            User-friendly refusal message
        """
        if not self.refusal_system:
            return "I cannot complete this request. Please try rephrasing your question."

        refusal = self.refusal_system.generate_refusal(
            reason=reason,
            context=details
        )

        if self.audit_logger and trace_id:
            self.audit_logger.log(
                trace_id=trace_id,
                event=AuditEvent.REFUSAL_GENERATED,
                level=AuditLevel.WARNING,
                component="refusal_system",
                message=f"Refusal generated: {reason.value}",
                details={"reason": reason.value}
            )

        return refusal.message

    def complete_trace(
        self,
        trace_id: str,
        sql: Optional[str] = None,
        confidence: Optional[float] = None,
        action: Optional[str] = None,
        success: bool = True
    ):
        """
        Complete an audit trace.

        Args:
            trace_id: Trace ID
            sql: Final SQL executed
            confidence: Final confidence score
            action: Action taken
            success: Whether query succeeded
        """
        if self.audit_logger and trace_id:
            self.audit_logger.complete_trace(
                trace_id=trace_id,
                sql=sql,
                confidence=confidence,
                action=action,
                success=success
            )

    def submit_feedback(
        self,
        query_id: str,
        question: str,
        feedback_type: FeedbackType,
        original_sql: Optional[str] = None,
        corrected_sql: Optional[str] = None,
        rating: Optional[int] = None,
        user_id: Optional[str] = None
    ):
        """
        Submit user feedback.

        Args:
            query_id: Query ID
            question: Original question
            feedback_type: Type of feedback
            original_sql: Original SQL
            corrected_sql: User-provided correction
            rating: Star rating
            user_id: User ID
        """
        if self.feedback_handler:
            self.feedback_handler.submit_feedback(
                query_id=query_id,
                question=question,
                feedback_type=feedback_type,
                original_sql=original_sql,
                corrected_sql=corrected_sql,
                rating=rating,
                user_id=user_id
            )

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get enterprise component statistics.

        Returns:
            Statistics dict
        """
        stats = {}

        if self.example_store:
            stats["learning"] = self.example_store.get_statistics()

        if self.feedback_handler:
            stats["feedback"] = self.feedback_handler.get_statistics()

        if self.audit_logger:
            stats["audit"] = self.audit_logger.get_statistics()

        return stats


def create_enterprise_processor(
    db_path: str,
    protocol_path: str = "knowledge/study_protocol.json",
    learning_db_path: str = "data/learning.db",
    audit_db_path: str = "data/audit.db",
    enable_all: bool = True
) -> EnterpriseProcessor:
    """
    Factory function to create an enterprise processor.

    Args:
        db_path: Path to DuckDB database
        protocol_path: Path to study protocol JSON
        learning_db_path: Path to learning database
        audit_db_path: Path to audit database
        enable_all: Enable all features

    Returns:
        Configured EnterpriseProcessor
    """
    config = EnterpriseConfig(
        db_path=db_path,
        protocol_path=protocol_path,
        learning_db_path=learning_db_path,
        audit_db_path=audit_db_path,
        enable_certified_answers=enable_all,
        enable_protocol_guard=enable_all,
        enable_schema_validation=enable_all,
        enable_view_routing=enable_all,
        enable_helpful_refusals=enable_all,
        enable_learning=enable_all,
        enable_audit=enable_all
    )

    return EnterpriseProcessor(config)
