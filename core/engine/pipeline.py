# SAGE - Inference Pipeline
# ==========================
"""
Inference Pipeline
==================
Main orchestrator for the 9-step query processing pipeline.

Pipeline Steps:
1. Input Sanitization
2. Entity Extraction
3. Intent Classification / Routing
4. Context Building
5. SQL Generation
6. SQL Validation
7. Execution
8. Confidence Scoring
9. Explanation Generation

This is the main entry point for Factory 4.
"""

import time
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from pathlib import Path

from .models import PipelineResult, EntityMatch, ConfidenceLevel
from .clinical_config import ClinicalQueryConfig, DEFAULT_CLINICAL_CONFIG
from .input_sanitizer import InputSanitizer, SanitizerConfig
from .entity_extractor import EntityExtractor, SimpleEntityExtractor
from .table_resolver import TableResolver, TableResolution
from .context_builder import ContextBuilder
from .sql_generator import SQLGenerator, OllamaConfig, MockSQLGenerator
from .sql_validator import SQLValidator, ValidatorConfig
from .executor import SQLExecutor, ExecutorConfig, MockExecutor
from .confidence_scorer import ConfidenceScorer, ScorerConfig
from .explanation_generator import ExplanationGenerator, ResponseBuilder

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for the inference pipeline."""
    # Database path
    db_path: str = ""

    # Metadata path
    metadata_path: str = ""

    # Ollama configuration
    ollama_host: str = "http://localhost:11434"
    ollama_model: str = "deepseek-r1:8b"
    ollama_fallback: str = "llama3.1:8b-instruct-q8_0"

    # Clinical configuration
    clinical_config: ClinicalQueryConfig = field(default_factory=lambda: DEFAULT_CLINICAL_CONFIG)

    # Use mock components for testing
    use_mock: bool = False

    # Timeout settings
    query_timeout_seconds: int = 30

    # Logging
    audit_log_path: Optional[str] = None


class InferencePipeline:
    """
    Main inference pipeline for SAGE.

    Orchestrates all 9 steps of query processing:
    1. Sanitize input
    2. Extract entities
    3. Route query
    4. Build context
    5. Generate SQL
    6. Validate SQL
    7. Execute SQL
    8. Score confidence
    9. Generate explanation

    Example:
        pipeline = InferencePipeline(config)
        result = pipeline.process("How many patients had headaches?")
        print(result.answer)
    """

    def __init__(self,
                 config: PipelineConfig = None,
                 fuzzy_matcher=None,
                 meddra_lookup=None):
        """
        Initialize inference pipeline.

        Args:
            config: Pipeline configuration
            fuzzy_matcher: Factory 3 fuzzy matcher instance
            meddra_lookup: Factory 3.5 MedDRA lookup instance
        """
        self.config = config or PipelineConfig()

        # Initialize components
        self._init_components(fuzzy_matcher, meddra_lookup)

    def _init_components(self, fuzzy_matcher, meddra_lookup):
        """Initialize all pipeline components."""
        # Step 1: Input Sanitizer
        self.sanitizer = InputSanitizer(SanitizerConfig())

        # Step 2: Entity Extractor
        if fuzzy_matcher or meddra_lookup:
            self.entity_extractor = EntityExtractor(
                fuzzy_matcher=fuzzy_matcher,
                meddra_lookup=meddra_lookup
            )
        else:
            self.entity_extractor = SimpleEntityExtractor()

        # Step 3: Table Resolver (Clinical Rules Engine)
        self.table_resolver = TableResolver(
            available_tables=self._get_available_tables(),
            config=self.config.clinical_config
        )

        # Step 4: Context Builder
        self.context_builder = ContextBuilder(
            metadata_path=self.config.metadata_path,
            db_path=self.config.db_path,
            config=self.config.clinical_config
        )

        # Step 5: SQL Generator
        if self.config.use_mock:
            self.sql_generator = MockSQLGenerator()
        else:
            self.sql_generator = SQLGenerator(OllamaConfig(
                host=self.config.ollama_host,
                model=self.config.ollama_model,
                fallback_model=self.config.ollama_fallback,
                timeout=self.config.query_timeout_seconds
            ))

        # Step 6: SQL Validator
        self.sql_validator = SQLValidator(
            available_tables=self._get_table_columns(),
            config=ValidatorConfig()
        )

        # Step 7: Executor
        if self.config.use_mock:
            self.executor = MockExecutor()
        else:
            self.executor = SQLExecutor(
                db_path=self.config.db_path,
                config=ExecutorConfig(timeout_seconds=self.config.query_timeout_seconds)
            )

        # Step 8: Confidence Scorer
        self.confidence_scorer = ConfidenceScorer(ScorerConfig())

        # Step 9: Explanation Generator
        self.response_builder = ResponseBuilder(ExplanationGenerator())

    def _get_available_tables(self) -> Dict[str, List[str]]:
        """Get available tables from database."""
        if self.config.use_mock:
            return {
                'ADAE': ['USUBJID', 'AEDECOD', 'ATOXGR', 'SAFFL', 'TRTEMFL'],
                'ADSL': ['USUBJID', 'AGE', 'SEX', 'SAFFL', 'ITTFL'],
                'ADLB': ['USUBJID', 'PARAMCD', 'AVAL', 'SAFFL'],
                'DM': ['USUBJID', 'AGE', 'SEX', 'RACE'],
                'AE': ['USUBJID', 'AEDECOD', 'AETOXGR', 'AESER'],
            }

        if not self.config.db_path:
            return {}

        try:
            import duckdb
            conn = duckdb.connect(self.config.db_path, read_only=True)
            try:
                tables = {}
                # Get all tables
                result = conn.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                """)
                for row in result.fetchall():
                    table_name = row[0]
                    # Get columns for each table
                    cols = conn.execute(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """)
                    tables[table_name] = [c[0] for c in cols.fetchall()]
                return tables
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return {}

    def _get_table_columns(self) -> Dict[str, List[str]]:
        """Get table columns for validator."""
        return self._get_available_tables()

    def process(self, query: str) -> PipelineResult:
        """
        Process a natural language query.

        Args:
            query: User's natural language query

        Returns:
            PipelineResult with answer, data, and methodology
        """
        start_time = time.time()
        pipeline_stages = {}

        try:
            # STEP 1: Input Sanitization
            logger.info("Step 1: Sanitizing input")
            sanitization = self.sanitizer.sanitize(query)
            pipeline_stages['sanitization'] = {
                'success': sanitization.is_safe,
                'time_ms': 0
            }

            if not sanitization.is_safe:
                return self._build_error_result(
                    query=query,
                    error=sanitization.blocked_reason,
                    stage="sanitization",
                    start_time=start_time
                )

            clean_query = sanitization.sanitized_query

            # STEP 2: Entity Extraction
            logger.info("Step 2: Extracting entities")
            step_start = time.time()
            extraction = self.entity_extractor.extract(clean_query)
            pipeline_stages['entity_extraction'] = {
                'success': True,
                'time_ms': (time.time() - step_start) * 1000,
                'entities_found': len(extraction.entities)
            }

            # STEP 3: Table Resolution (Clinical Rules Engine)
            logger.info("Step 3: Resolving table and columns")
            step_start = time.time()
            table_resolution = self.table_resolver.resolve(
                query=clean_query
            )
            pipeline_stages['table_resolution'] = {
                'success': table_resolution.selected_table is not None,
                'time_ms': (time.time() - step_start) * 1000,
                'table': table_resolution.selected_table,
                'population': table_resolution.population_name
            }

            if not table_resolution.selected_table:
                return self._build_error_result(
                    query=query,
                    error="Could not determine appropriate table for query",
                    stage="table_resolution",
                    start_time=start_time
                )

            # STEP 4: Context Building
            logger.info("Step 4: Building LLM context")
            step_start = time.time()
            context = self.context_builder.build(
                query=clean_query,
                table_resolution=table_resolution,
                entities=extraction.entities
            )
            pipeline_stages['context_building'] = {
                'success': True,
                'time_ms': (time.time() - step_start) * 1000,
                'token_estimate': context.token_count_estimate
            }

            # STEP 5: SQL Generation
            logger.info("Step 5: Generating SQL")
            step_start = time.time()
            generated = self.sql_generator.generate(context)
            pipeline_stages['sql_generation'] = {
                'success': generated.sql is not None,
                'time_ms': (time.time() - step_start) * 1000,
                'model': generated.model_used
            }

            if not generated.sql:
                return self._build_error_result(
                    query=query,
                    error="SQL generation failed",
                    stage="sql_generation",
                    start_time=start_time
                )

            # STEP 6: SQL Validation
            logger.info("Step 6: Validating SQL")
            step_start = time.time()
            validation = self.sql_validator.validate(generated.sql)
            pipeline_stages['sql_validation'] = {
                'success': validation.is_valid,
                'time_ms': (time.time() - step_start) * 1000,
                'errors': validation.errors,
                'warnings': validation.warnings
            }

            if not validation.is_valid:
                return self._build_error_result(
                    query=query,
                    error=f"SQL validation failed: {', '.join(validation.errors)}",
                    stage="sql_validation",
                    start_time=start_time
                )

            # STEP 7: Execution
            logger.info("Step 7: Executing SQL")
            step_start = time.time()
            execution = self.executor.execute(validation.validated_sql)
            pipeline_stages['execution'] = {
                'success': execution.success,
                'time_ms': (time.time() - step_start) * 1000,
                'row_count': execution.row_count
            }

            if not execution.success:
                return self._build_error_result(
                    query=query,
                    error=execution.error_message,
                    stage="execution",
                    start_time=start_time
                )

            # STEP 8: Confidence Scoring
            logger.info("Step 8: Calculating confidence score")
            step_start = time.time()
            confidence = self.confidence_scorer.score(
                entities=extraction.entities,
                table_resolution=table_resolution,
                validation=validation,
                execution=execution
            )
            pipeline_stages['confidence_scoring'] = {
                'success': True,
                'time_ms': (time.time() - step_start) * 1000,
                'score': confidence.overall_score,
                'level': confidence.level.value
            }

            # STEP 9: Explanation Generation
            logger.info("Step 9: Generating explanation")
            step_start = time.time()
            response = self.response_builder.build(
                query=query,
                result=execution,
                table_resolution=table_resolution,
                confidence=confidence,
                entities=extraction.entities,
                sql=validation.validated_sql
            )
            pipeline_stages['explanation'] = {
                'success': True,
                'time_ms': (time.time() - step_start) * 1000
            }

            # Build final result
            total_time = (time.time() - start_time) * 1000

            return PipelineResult(
                success=True,
                query=query,
                answer=response['answer'],
                data=response['data'],
                row_count=response['row_count'],
                sql=validation.validated_sql,
                methodology=response['methodology'],
                confidence=response['confidence'],
                warnings=response['warnings'],
                pipeline_stages=pipeline_stages,
                total_time_ms=total_time
            )

        except Exception as e:
            logger.exception(f"Pipeline error: {e}")
            return self._build_error_result(
                query=query,
                error=str(e),
                stage="unknown",
                start_time=start_time
            )

    def _build_error_result(self,
                             query: str,
                             error: str,
                             stage: str,
                             start_time: float
                            ) -> PipelineResult:
        """Build error result."""
        total_time = (time.time() - start_time) * 1000

        error_response = self.response_builder.build_error_response(
            query=query,
            error=error,
            stage=stage
        )

        return PipelineResult(
            success=False,
            query=query,
            answer=error_response['answer'],
            data=None,
            row_count=0,
            sql=None,
            methodology=None,
            confidence={
                'score': 0,
                'level': 'very_low',
                'explanation': f"Query failed at {stage} stage: {error}"
            },
            warnings=[f"Error at {stage}: {error}"],
            error=error,
            error_stage=stage,
            pipeline_stages={stage: {'success': False, 'error': error}},
            total_time_ms=total_time
        )

    def is_ready(self) -> Dict[str, bool]:
        """Check if all components are ready."""
        status = {
            'database': False,
            'ollama': False,
            'metadata': False
        }

        # Check database
        if self.config.db_path:
            status['database'] = self.executor.validate_connection()

        # Check Ollama
        if not self.config.use_mock:
            status['ollama'] = self.sql_generator.is_available()
        else:
            status['ollama'] = True

        # Check metadata
        if self.config.metadata_path:
            status['metadata'] = Path(self.config.metadata_path).exists()

        return status


def load_factory3_components(
    fuzzy_index_path: str = None,
    db_path: str = None
):
    """
    Load Factory 3 and Factory 3.5 components if available.

    Args:
        fuzzy_index_path: Path to fuzzy_index.pkl
        db_path: Path to DuckDB database (for MedDRA lookup)

    Returns:
        Tuple of (fuzzy_matcher, meddra_lookup) - either may be None if unavailable
    """
    fuzzy_matcher = None
    meddra_lookup = None

    # Try to load FuzzyMatcher from Factory 3
    if fuzzy_index_path and Path(fuzzy_index_path).exists():
        try:
            from core.dictionary.fuzzy_matcher import FuzzyMatcher
            fuzzy_matcher = FuzzyMatcher.load(fuzzy_index_path)
            logger.info(f"Loaded FuzzyMatcher from {fuzzy_index_path} with {len(fuzzy_matcher)} entries")
        except Exception as e:
            logger.warning(f"Could not load FuzzyMatcher: {e}")

    # Try to load MedDRALookup from Factory 3.5
    if db_path and Path(db_path).exists():
        try:
            from core.meddra.lookup import MedDRALookup
            meddra_lookup = MedDRALookup(db_path)
            logger.info(f"Initialized MedDRALookup with database: {db_path}")
        except Exception as e:
            logger.warning(f"Could not load MedDRALookup: {e}")

    return fuzzy_matcher, meddra_lookup


def create_pipeline(
    db_path: str,
    metadata_path: str = None,
    ollama_host: str = "http://localhost:11434",
    use_mock: bool = False,
    fuzzy_matcher=None,
    meddra_lookup=None,
    fuzzy_index_path: str = None,
    auto_load_factory3: bool = True
) -> InferencePipeline:
    """
    Factory function to create a configured pipeline.

    Args:
        db_path: Path to DuckDB database
        metadata_path: Path to golden_metadata.json
        ollama_host: Ollama API host
        use_mock: Use mock components for testing
        fuzzy_matcher: Factory 3 fuzzy matcher (pre-loaded)
        meddra_lookup: Factory 3.5 MedDRA lookup (pre-loaded)
        fuzzy_index_path: Path to fuzzy_index.pkl (auto-loads if provided)
        auto_load_factory3: Automatically try to load Factory 3/3.5 components

    Returns:
        Configured InferencePipeline
    """
    # Auto-load Factory 3/3.5 components if not provided
    if auto_load_factory3 and not use_mock:
        if fuzzy_matcher is None and fuzzy_index_path:
            fuzzy_matcher, _ = load_factory3_components(fuzzy_index_path=fuzzy_index_path)
        if meddra_lookup is None and db_path:
            _, meddra_lookup = load_factory3_components(db_path=db_path)

    config = PipelineConfig(
        db_path=db_path,
        metadata_path=metadata_path or "",
        ollama_host=ollama_host,
        use_mock=use_mock
    )

    return InferencePipeline(
        config=config,
        fuzzy_matcher=fuzzy_matcher,
        meddra_lookup=meddra_lookup
    )
