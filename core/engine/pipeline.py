# SAGE - Inference Pipeline
# ==========================
"""
Inference Pipeline
==================
Main orchestrator for the query processing pipeline.

Pipeline Steps:
0. Intent Classification (Claude-based) - Determines if query is clinical data or conversational
0.5 Cache Check - Return cached result if available
1. Input Sanitization - Security checks
1.5 Query Analysis - Structured query understanding (NEW)
1.7 Clarification Check - Ask for clarification if needed (NEW)
2. Entity Extraction - Clinical term resolution
3. Table Resolution - Clinical Rules Engine
4. Context Building - LLM context preparation
5. SQL Generation - Claude-powered SQL generation
6. SQL Validation - Safety and correctness checks
7. Execution - DuckDB query execution
7.5 Answer Verification - Verify result accuracy (NEW)
8. Confidence Scoring - Result reliability assessment
9. Explanation Generation - Human-readable response

For non-clinical queries (greetings, help, identity questions), Claude generates
natural conversational responses without going through the full pipeline.

New Accuracy Features:
- QueryAnalyzer: Structured understanding before SQL generation
- ClarificationManager: Ask when query is ambiguous
- AnswerVerifier: Verify results before returning
- SessionMemory: Track conversation context
- DataDrivenKnowledge: Learn from actual data values

This is the main entry point for Factory 4.
"""

import re
import time
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from pathlib import Path

from .models import PipelineResult, EntityMatch, ConfidenceLevel, EntityExtractionResult
from .clinical_config import ClinicalQueryConfig, DEFAULT_CLINICAL_CONFIG, QueryDomain, PopulationType
from .input_sanitizer import InputSanitizer, SanitizerConfig
from .entity_extractor import EntityExtractor, SimpleEntityExtractor
from .table_resolver import TableResolver, TableResolution
from .context_builder import ContextBuilder
from .sql_generator import (
    MockSQLGenerator, GenerationResult,
    LLMError, LLMTimeoutError, LLMConnectionError, LLMModelError
)
from .llm_providers import (
    LLMConfig, LLMProvider, LLMRequest, create_llm_provider, get_current_provider
)
from .sql_validator import SQLValidator, ValidatorConfig
from .executor import SQLExecutor, ExecutorConfig, MockExecutor
from .confidence_scorer import ConfidenceScorer, ScorerConfig
from .explanation_generator import ExplanationGenerator, ResponseBuilder, init_naming_service
from .cache import QueryCache, get_query_cache

# New Accuracy Components
from .query_analyzer import QueryAnalyzer, QueryAnalysis, QueryIntent, QuerySubject
from .clarification_manager import ClarificationManager
from .answer_verifier import AnswerVerifier
from .response_format import (
    SAGEResponse, SAGEResponseBuilder, ResponseType,
    MethodologyInfo, EntityResolution, standardize_confidence, get_confidence_level
)
from .session_memory import SessionMemory, SessionManager, get_session_manager
from .data_knowledge import DataKnowledge, DataKnowledgeLearner, get_data_knowledge

# Factory 4.5: LLM-Enhanced Features
from .synonym_resolver import SynonymResolver, create_synonym_resolver
from .explanation_enricher import ExplanationEnricher, create_explanation_enricher
from .query_disambiguator import QueryDisambiguator, create_query_disambiguator
from .error_humanizer import ErrorHumanizer, create_error_humanizer

logger = logging.getLogger(__name__)


# =============================================================================
# INSTANT RESPONSE PATTERNS (Checked BEFORE LLM classification)
# =============================================================================
# These patterns provide sub-100ms responses for simple non-clinical queries

INSTANT_PATTERNS = {
    'greeting': re.compile(r'^(hi|hello|hey|good\s*(morning|afternoon|evening)|howdy)[\s!?.]*$', re.I),
    'thanks': re.compile(r'^(thanks?|thank\s*you|thx|cheers)[\s!?.]*$', re.I),
    'bye': re.compile(r'^(bye|goodbye|see\s*you|later)[\s!?.]*$', re.I),
}

INSTANT_RESPONSES = {
    'greeting': """Hello! I'm SAGE, your clinical data assistant.

**I can help you with:**
- Counting patients/subjects with specific conditions
- Analyzing adverse events by severity, type, or outcome
- Demographics breakdowns (age, sex, race)
- Population queries (safety, ITT, enrolled)

**Example questions:**
- "How many subjects are in the safety population?"
- "How many patients had serious adverse events?"
- "What is the age distribution?"

What would you like to know?""",

    'thanks': "You're welcome! Let me know if you have more questions about your clinical data.",

    'bye': "Goodbye! Feel free to return when you have clinical data questions.",
}


# =============================================================================
# INTENT CLASSIFIER (Step 0)
# =============================================================================
# Uses Claude to classify query intent - no hard-coded patterns needed

INTENT_CLASSIFICATION_PROMPT = """You are an intent classifier for SAGE, a clinical trial data analysis system.

Classify the user's query into ONE of these categories:
- CLINICAL_DATA: Questions that require querying clinical trial databases (SDTM/ADaM)
  Examples: "How many patients had nausea?", "Show adverse events", "Count subjects in safety population"
  ALSO includes follow-up queries that reference previous results:
  - "list them", "show them", "can you list them", "who are they"
  - "of those, how many...", "out of these", "among them"
  - Any query referencing "them", "those", "these" in the context of previous clinical data
- GREETING: Greetings or salutations (hi, hello, hey, good morning, etc.)
- HELP: Questions about what the system can do or how to use it (but NOT "list them" type follow-ups)
- IDENTITY: Questions about what AI/model/system this is
- FAREWELL: Goodbyes or thank you messages
- STATUS: System status checks (ping, are you there, etc.)
- GENERAL: Other non-clinical questions that need a conversational response

IMPORTANT: If the query contains "them", "those", "these" and could be referencing previous clinical data results, classify as CLINICAL_DATA.

Respond with ONLY the category name, nothing else.

User query: {query}"""

SAGE_SYSTEM_CONTEXT = """You are SAGE (Study Analytics Generative Engine), a clinical data analysis assistant.

About SAGE:
- Translates natural language questions into SQL queries for clinical trial data
- Uses Claude (Anthropic) for intelligent query generation
- All calculations are performed deterministically by code, not AI inference
- Only database schema metadata is sent to the cloud - clinical data stays local
- Works with CDISC SDTM and ADaM datasets

Your capabilities:
- Count patients/subjects meeting specific criteria
- List adverse events by severity and type
- Analyze demographics (age, gender, race)
- Query lab values and vital signs
- Filter by population flags (safety, ITT, etc.)

Example questions users can ask:
- "How many patients had serious adverse events?"
- "What are the top 5 most common adverse events?"
- "How many female patients had adverse events?"
- "How many patients are over 65 years old?"
- "Show me the gender distribution"

Important: You are a query translator, not a medical expert. You execute queries against data and present results with confidence scores.

Respond naturally and helpfully to the user's message. Keep responses concise but informative."""


# =============================================================================
# SELF-CORRECTION CONSTANTS
# =============================================================================

MAX_CORRECTION_ATTEMPTS = 3  # Maximum times to retry SQL generation after execution error

SQL_CORRECTION_PROMPT = """Your previous SQL query failed with the following error:

**Error:** {error}

**Original Query:** {original_query}

**Previous SQL that failed:**
```sql
{failed_sql}
```

Please analyze the error and generate a corrected SQL query. Common issues:
- Table/column names are case-sensitive in DuckDB
- Use exact column names from the schema provided
- Ensure WHERE conditions match the data types
- For text comparisons, use UPPER() or LOWER() for case-insensitive matching

Generate a corrected SQL query that will work with DuckDB."""


# =============================================================================
# CLAUDE SQL GENERATOR
# =============================================================================

class ClaudeSQLGenerator:
    """
    SQL Generator using Claude (Anthropic API).

    Wraps the Claude provider from llm_providers.py to provide
    SQL generation capabilities for the pipeline.
    """

    def __init__(self, timeout: int = 60):
        """
        Initialize Claude SQL generator.

        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self._provider = None
        self._init_provider()

    def _init_provider(self):
        """Initialize the Claude provider."""
        try:
            config = LLMConfig.from_env()
            config.timeout = self.timeout
            self._provider = create_llm_provider(config)
            logger.info(f"Claude SQL generator initialized: {self._provider.get_model_name()}")
        except Exception as e:
            logger.error(f"Failed to initialize Claude provider: {e}")
            raise

    def is_available(self) -> bool:
        """Check if Claude is available."""
        return self._provider is not None and self._provider.is_available()

    def generate(self, context) -> GenerationResult:
        """
        Generate SQL using Claude.

        Args:
            context: LLMContext with system_prompt, user_prompt, and schema info

        Returns:
            GenerationResult with SQL and metadata
        """
        start_time = time.time()

        # Use the prompts already prepared by ContextBuilder
        system_prompt = context.system_prompt if hasattr(context, 'system_prompt') else self._build_system_prompt()
        user_prompt = context.user_prompt if hasattr(context, 'user_prompt') else ""

        # If no user_prompt, try to build from context attributes
        if not user_prompt:
            user_prompt = self._build_user_prompt(context)

        try:
            # Call Claude
            request = LLMRequest(
                prompt=user_prompt,
                system_prompt=system_prompt,
                max_tokens=2000,
                temperature=0.1
            )

            response = self._provider.generate(request)
            generation_time = (time.time() - start_time) * 1000

            # Extract SQL from response
            sql = self._extract_sql(response.content)

            return GenerationResult(
                sql=sql,
                raw_response=response.content,
                model_used=response.model,
                generation_time_ms=generation_time,
                success=bool(sql),
                error=None if sql else "No SQL found in response"
            )

        except Exception as e:
            generation_time = (time.time() - start_time) * 1000
            error_msg = str(e)

            # Map to appropriate error type
            if "connect" in error_msg.lower() or "connection" in error_msg.lower():
                raise LLMConnectionError(f"Cannot connect to Claude API: {error_msg}")
            elif "timeout" in error_msg.lower():
                raise LLMTimeoutError(f"Claude API timeout: {error_msg}")
            else:
                raise LLMError(f"Claude API error: {error_msg}")

    def _build_system_prompt(self) -> str:
        """Build system prompt for SQL generation."""
        return """You are a SQL expert for clinical trial data analysis.
Your task is to generate DuckDB-compatible SQL queries based on CDISC SDTM/ADaM data structures.

IMPORTANT RULES:
1. Only output valid SQL - no explanations unless asked
2. Use standard CDISC variable names (USUBJID, AEDECOD, SAFFL, etc.)
3. Always consider safety population (SAFFL='Y') when relevant
4. Use COUNT(DISTINCT USUBJID) for patient counts
5. Output SQL wrapped in ```sql ... ``` code blocks
6. Never use DELETE, UPDATE, DROP, or other destructive operations"""

    def _build_user_prompt(self, context: 'GenerationContext') -> str:
        """Build user prompt from generation context."""
        parts = []

        # Add schema information
        if hasattr(context, 'schema_context') and context.schema_context:
            parts.append(f"DATABASE SCHEMA:\n{context.schema_context}")

        # Add table information
        if hasattr(context, 'tables') and context.tables:
            tables_info = ", ".join(context.tables)
            parts.append(f"RELEVANT TABLES: {tables_info}")

        # Add the query
        if hasattr(context, 'query') and context.query:
            parts.append(f"USER QUESTION: {context.query}")

        # Add any extracted entities
        if hasattr(context, 'entities') and context.entities:
            entity_info = ", ".join([f"{e.original} -> {e.matched}" for e in context.entities])
            parts.append(f"MATCHED TERMS: {entity_info}")

        parts.append("\nGenerate a SQL query to answer this question. Output only the SQL wrapped in ```sql ... ``` blocks.")

        return "\n\n".join(parts)

    def _extract_sql(self, response: str) -> Optional[str]:
        """Extract SQL from Claude's response."""
        import re

        # Try to extract from code blocks
        sql_pattern = r'```sql\s*(.*?)\s*```'
        matches = re.findall(sql_pattern, response, re.DOTALL | re.IGNORECASE)

        if matches:
            return matches[0].strip()

        # Try generic code blocks
        code_pattern = r'```\s*(.*?)\s*```'
        matches = re.findall(code_pattern, response, re.DOTALL)

        for match in matches:
            if 'SELECT' in match.upper():
                return match.strip()

        # Try to find raw SQL
        if 'SELECT' in response.upper():
            lines = response.split('\n')
            sql_lines = []
            in_sql = False

            for line in lines:
                if 'SELECT' in line.upper() and not in_sql:
                    in_sql = True
                if in_sql:
                    sql_lines.append(line)
                    if ';' in line:
                        break

            if sql_lines:
                return '\n'.join(sql_lines).strip()

        return None

    def generate_correction(self, original_query: str, failed_sql: str,
                           error: str, context) -> GenerationResult:
        """
        Generate a corrected SQL query after execution failure.

        Args:
            original_query: User's original natural language query
            failed_sql: The SQL that failed to execute
            error: Error message from execution
            context: Original LLMContext with schema info

        Returns:
            GenerationResult with corrected SQL
        """
        start_time = time.time()

        # Build correction prompt
        correction_prompt = SQL_CORRECTION_PROMPT.format(
            error=error,
            original_query=original_query,
            failed_sql=failed_sql
        )

        # Get schema context from original
        system_prompt = context.system_prompt if hasattr(context, 'system_prompt') else self._build_system_prompt()

        # Add correction instruction to system prompt
        system_prompt += "\n\nIMPORTANT: You must fix the error in the previous SQL. Be precise with column and table names."

        try:
            request = LLMRequest(
                prompt=correction_prompt,
                system_prompt=system_prompt,
                max_tokens=2000,
                temperature=0.0  # More deterministic for corrections
            )

            response = self._provider.generate(request)
            generation_time = (time.time() - start_time) * 1000

            sql = self._extract_sql(response.content)

            return GenerationResult(
                sql=sql,
                raw_response=response.content,
                model_used=response.model,
                generation_time_ms=generation_time,
                success=bool(sql),
                error=None if sql else "No SQL found in correction response",
                reasoning="SQL correction attempt"
            )

        except Exception as e:
            generation_time = (time.time() - start_time) * 1000
            return GenerationResult(
                sql=None,
                raw_response="",
                model_used="claude",
                generation_time_ms=generation_time,
                success=False,
                error=f"Correction failed: {str(e)}"
            )


@dataclass
class PipelineConfig:
    """Configuration for the inference pipeline."""
    # Database path
    db_path: str = ""

    # Metadata path (golden_metadata.json from Factory 2)
    metadata_path: str = ""

    # CDISC library database path (for standard variable definitions)
    cdisc_db_path: str = ""

    # Clinical configuration
    clinical_config: ClinicalQueryConfig = field(default_factory=lambda: DEFAULT_CLINICAL_CONFIG)

    # Use mock components for testing
    use_mock: bool = False

    # Timeout settings (increased for slow reasoning models like deepseek-r1)
    query_timeout_seconds: int = 240

    # Logging
    audit_log_path: Optional[str] = None

    # Pre-loaded available tables (to avoid DuckDB lock conflicts)
    # Format: {'TABLE_NAME': ['COL1', 'COL2', ...], ...}
    available_tables: Optional[Dict[str, List[str]]] = None

    # Shared DuckDB connection (to avoid lock conflicts)
    db_connection: Any = None

    # Cache configuration
    enable_cache: bool = True
    cache_ttl_seconds: int = 3600  # 1 hour default
    cache_max_size: int = 1000

    # === New Accuracy Features ===

    # Enable query analysis (structured understanding before SQL)
    enable_query_analysis: bool = True

    # Enable clarification requests when query is ambiguous
    enable_clarification: bool = True
    clarification_threshold: float = 0.7  # Below this confidence, ask for clarification

    # Enable answer verification (verify results before returning)
    enable_verification: bool = True
    verification_threshold: float = 0.7  # Below this, flag as low confidence

    # Enable session memory (track conversation context)
    enable_session_memory: bool = True

    # Enable data-driven knowledge (learn from actual data)
    enable_data_knowledge: bool = True

    # === Factory 4.5: LLM-Enhanced Features ===

    # Enable synonym resolution (LLM-suggested synonyms validated against data)
    enable_synonym_resolution: bool = True

    # Enable metadata-based column explanations in Details tab
    enable_explanation_enrichment: bool = True

    # Enable natural language error messages
    enable_error_humanization: bool = True


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
                 meddra_lookup=None,
                 session_id: str = None):
        """
        Initialize inference pipeline.

        Args:
            config: Pipeline configuration
            fuzzy_matcher: Factory 3 fuzzy matcher instance
            meddra_lookup: Factory 3.5 MedDRA lookup instance
            session_id: Optional session ID for conversation memory
        """
        self.config = config or PipelineConfig()
        self.session_id = session_id

        # Initialize cache with data version tracking (use global singleton)
        if self.config.enable_cache:
            self.cache = get_query_cache(
                max_size=self.config.cache_max_size,
                default_ttl=self.config.cache_ttl_seconds,
                db_path=self.config.db_path  # Enable data version tracking
            )
            logger.info(f"Query cache enabled (global singleton, db_path={self.config.db_path})")
        else:
            self.cache = None
            logger.info("Query cache disabled")

        # Initialize components
        self._init_components(fuzzy_matcher, meddra_lookup)

        # Initialize new accuracy components
        self._init_accuracy_components()

    def _init_components(self, fuzzy_matcher, meddra_lookup):
        """Initialize all pipeline components."""
        # Initialize clinical naming service for user-friendly labels
        init_naming_service(
            metadata_path=self.config.metadata_path,
            cdisc_db_path=self.config.cdisc_db_path
        )
        logger.info("Clinical naming service initialized")

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

        # Step 5: SQL Generator (using Claude)
        if self.config.use_mock:
            self.sql_generator = MockSQLGenerator()
        else:
            self.sql_generator = ClaudeSQLGenerator(
                timeout=self.config.query_timeout_seconds
            )

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
                config=ExecutorConfig(timeout_seconds=self.config.query_timeout_seconds),
                connection=self.config.db_connection  # Use shared connection if provided
            )

        # Step 8: Confidence Scorer
        self.confidence_scorer = ConfidenceScorer(ScorerConfig())

        # Step 9: Explanation Generator
        self.response_builder = ResponseBuilder(ExplanationGenerator())

    def _init_accuracy_components(self):
        """Initialize new accuracy components."""

        # Query Analyzer - Structured query understanding
        if self.config.enable_query_analysis and not self.config.use_mock:
            self.query_analyzer = QueryAnalyzer()
            logger.info("Query analyzer enabled")
        else:
            self.query_analyzer = None

        # Clarification Manager - Ask when uncertain
        if self.config.enable_clarification:
            self.clarification_manager = ClarificationManager()
            self.clarification_manager.CLARIFICATION_THRESHOLD = self.config.clarification_threshold
            logger.info(f"Clarification manager enabled (threshold={self.config.clarification_threshold})")
        else:
            self.clarification_manager = None

        # Answer Verifier - Verify before returning
        if self.config.enable_verification:
            self.answer_verifier = AnswerVerifier(db_path=self.config.db_path)
            logger.info("Answer verifier enabled")
        else:
            self.answer_verifier = None

        # Session Memory - Track conversation context
        if self.config.enable_session_memory:
            session_manager = get_session_manager()
            if self.session_id:
                self.session = session_manager.get_session(self.session_id)
            else:
                self.session = session_manager.create_session()
            self.session_id = self.session.session_id
            logger.info(f"Session memory enabled (session_id={self.session_id})")
        else:
            self.session = None

        # Data-Driven Knowledge - Learn from actual data
        if self.config.enable_data_knowledge and self.config.db_path:
            try:
                self.data_knowledge = get_data_knowledge(self.config.db_path)
                if self.data_knowledge:
                    logger.info(f"Data knowledge loaded: {self.data_knowledge.to_dict()}")
                else:
                    logger.info("Data knowledge not available")
            except Exception as e:
                logger.warning(f"Could not load data knowledge: {e}")
                self.data_knowledge = None
        else:
            self.data_knowledge = None

        # Factory 4.5: LLM-Enhanced Features
        self._init_llm_enhanced_components()

    def _init_llm_enhanced_components(self):
        """Initialize Factory 4.5 LLM-enhanced components."""

        # Synonym Resolver - LLM-suggested synonyms validated against data
        if self.config.enable_synonym_resolution:
            try:
                self.synonym_resolver = create_synonym_resolver(
                    db_path=self.config.db_path,
                    fuzzy_matcher=getattr(self.entity_extractor, 'fuzzy_matcher', None)
                )
                logger.info("Synonym resolver enabled")
            except Exception as e:
                logger.warning(f"Could not initialize synonym resolver: {e}")
                self.synonym_resolver = None
        else:
            self.synonym_resolver = None

        # Explanation Enricher - Metadata-based column explanations
        if self.config.enable_explanation_enrichment:
            try:
                self.explanation_enricher = create_explanation_enricher(
                    metadata_path=self.config.metadata_path
                )
                logger.info("Explanation enricher enabled")
            except Exception as e:
                logger.warning(f"Could not initialize explanation enricher: {e}")
                self.explanation_enricher = None
        else:
            self.explanation_enricher = None

        # Error Humanizer - Natural language error messages
        if self.config.enable_error_humanization:
            try:
                available_tables = list(self._get_available_tables().keys())
                self.error_humanizer = create_error_humanizer(
                    available_tables=available_tables
                )
                logger.info("Error humanizer enabled")
            except Exception as e:
                logger.warning(f"Could not initialize error humanizer: {e}")
                self.error_humanizer = None
        else:
            self.error_humanizer = None

    def _get_available_tables(self) -> Dict[str, List[str]]:
        """Get available tables from database or config."""
        # First check if tables were pre-loaded (to avoid DuckDB lock conflicts)
        if self.config.available_tables:
            logger.info(f"Using pre-loaded tables: {list(self.config.available_tables.keys())}")
            return self.config.available_tables

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

    def _validate_and_inject_filters(
        self,
        sql: str,
        accumulated_filters: Optional[str],
        force_inject: bool = False
    ) -> tuple[str, List[str]]:
        """
        Validate that accumulated filters are present in SQL.

        If filters are missing, inject them into the WHERE clause.
        This is a safety net in case the LLM fails to include required filters.

        CRITICAL FIX: This is now called unconditionally when accumulated_filters
        exist and there's session context. We don't rely solely on LLM classification.

        Args:
            sql: Generated SQL query
            accumulated_filters: SQL conditions that must be preserved (e.g., "ITTFL = 'Y' AND AGE >= 65")
            force_inject: Force filter injection (overrides all checks)

        Returns:
            Tuple of (modified_sql, warnings)
        """
        warnings = []

        # Only skip if we have no filters or no SQL
        if not accumulated_filters or not sql:
            return sql, warnings

        import re
        sql_upper = sql.upper()

        # Columns that exist in ADSL (population/demographics)
        ADSL_COLUMNS = {'ITTFL', 'SAFFL', 'EFFFL', 'PPROTFL', 'AGE', 'SEX', 'RACE', 'ETHNIC', 'ARM', 'TRT01A'}
        # Columns that exist in ADAE (adverse events)
        ADAE_COLUMNS = {'AEDECOD', 'AELLT', 'AEBODSYS', 'AESER', 'AEREL', 'ATOXGR', 'AETOXGR', 'AESEV', 'TRTEMFL'}

        # Detect the main table and its alias from the SQL
        # Pattern: FROM table [AS] alias  or  FROM table alias
        from_match = re.search(r'\bFROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', sql, re.IGNORECASE)
        main_table = from_match.group(1).upper() if from_match else None
        main_alias = from_match.group(2) if from_match and from_match.group(2) else None

        # Check if there's a JOIN in the query (indicates aliases are being used)
        has_join = re.search(r'\bJOIN\b', sql, re.IGNORECASE) is not None

        # Parse individual filter conditions from accumulated_filters
        filter_parts = [f.strip() for f in accumulated_filters.split(' AND ') if f.strip()]

        missing_filters = []
        subquery_filters = []  # Filters that need subquery approach

        for filter_part in filter_parts:
            filter_upper = filter_part.upper()

            # Check if filter is already in SQL
            if filter_upper in sql_upper:
                continue

            # Extract column name
            column_match = re.match(r"(\w+)\s*(=|>=|<=|>|<|LIKE|IN)\s*(.+)", filter_part, re.IGNORECASE)
            if column_match:
                column, op, value = column_match.groups()
                column_upper = column.upper()

                # Check if column is referenced with any alias
                alias_pattern = rf'\b\w+\.{column_upper}\b'
                if re.search(alias_pattern, sql_upper):
                    if value.strip().upper() in sql_upper:
                        continue

                # Determine if this is a cross-table filter
                # If querying ADAE but filter is for ADSL column, use subquery
                if main_table == 'ADAE' and column_upper in ADSL_COLUMNS:
                    # Use subquery approach for ADSL filters on ADAE queries
                    subquery_filters.append(filter_part)
                    logger.info(f"Filter '{filter_part}' needs subquery (ADSL filter on ADAE query)")
                    continue
                elif main_table == 'ADSL' and column_upper in ADAE_COLUMNS:
                    # Use subquery approach for ADAE filters on ADSL queries
                    subquery_filters.append(filter_part)
                    logger.info(f"Filter '{filter_part}' needs subquery (ADAE filter on ADSL query)")
                    continue

            # Filter can be directly added
            missing_filters.append(filter_part)
            logger.warning(f"Filter '{filter_part}' not found in generated SQL")

        # Build subquery conditions
        subquery_sql = ""
        if subquery_filters:
            # Determine source table for subquery
            if main_table == 'ADAE':
                source_table = 'ADSL'
            else:
                source_table = 'ADAE'

            subquery_conditions = ' AND '.join(subquery_filters)

            # Use qualified USUBJID reference if there's a JOIN or aliases
            # This prevents "Ambiguous reference" errors when main query has JOINs
            if has_join and main_alias:
                usubjid_ref = f"{main_alias}.USUBJID"
            elif has_join:
                # Has join but no detected alias for main table - use table name
                usubjid_ref = f"{main_table}.USUBJID"
            else:
                usubjid_ref = "USUBJID"

            subquery_sql = f"{usubjid_ref} IN (SELECT USUBJID FROM {source_table} WHERE {subquery_conditions})"
            warnings.append(f"Used subquery for cross-table filters: {', '.join(subquery_filters)}")
            logger.info(f"Built subquery for cross-table filters: {subquery_sql}")

        # Combine direct filters and subquery
        all_conditions = []
        if missing_filters:
            all_conditions.extend(missing_filters)
            warnings.append(f"Injected missing filters: {', '.join(missing_filters)}")
        if subquery_sql:
            all_conditions.append(subquery_sql)

        # If we have conditions to inject
        if all_conditions:
            logger.warning(f"Injecting conditions into SQL: {all_conditions}")

            # Find WHERE clause and add conditions
            where_match = re.search(r'\bWHERE\b', sql, re.IGNORECASE)
            if where_match:
                # Insert conditions after WHERE
                where_pos = where_match.end()
                conditions_sql = ' AND '.join(all_conditions)
                sql = f"{sql[:where_pos]} {conditions_sql} AND {sql[where_pos:]}"
            else:
                # No WHERE clause - add one before LIMIT/ORDER/GROUP
                limit_match = re.search(r'\b(LIMIT|ORDER BY|GROUP BY)\b', sql, re.IGNORECASE)
                if limit_match:
                    insert_pos = limit_match.start()
                    conditions_sql = ' AND '.join(all_conditions)
                    sql = f"{sql[:insert_pos]} WHERE {conditions_sql} {sql[insert_pos:]}"
                else:
                    # Add WHERE at the end
                    conditions_sql = ' AND '.join(all_conditions)
                    sql = f"{sql} WHERE {conditions_sql}"

            logger.info(f"SQL after filter injection: {sql[:300]}...")

        return sql, warnings

    def _execute_with_self_correction(
        self,
        query: str,
        context,
        pipeline_stages: Dict[str, Any],
        accumulated_filters: Optional[str] = None,
        preserve_filters: bool = False
    ) -> tuple[Optional[Any], Optional[Any], Optional[str], Dict[str, Any]]:
        """
        Execute SQL generation with self-correction loop.

        If SQL fails to execute, feeds error back to Claude for correction.
        Retries up to MAX_CORRECTION_ATTEMPTS times.

        Args:
            query: Original user query
            context: LLM context with schema info
            pipeline_stages: Dict to update with stage info
            accumulated_filters: SQL conditions that must be preserved for refinement queries
            preserve_filters: Whether to validate and inject missing filters

        Returns:
            Tuple of (validation_result, execution_result, final_sql, correction_info)
        """
        correction_info = {
            'attempts': 0,
            'corrections': [],
            'final_success': False,
            'filter_warnings': []
        }

        # Initial SQL generation
        logger.info("Step 5: Generating SQL")
        step_start = time.time()
        generated = self.sql_generator.generate(context)
        pipeline_stages['sql_generation'] = {
            'success': generated.sql is not None,
            'time_ms': (time.time() - step_start) * 1000,
            'model': generated.model_used
        }

        if not generated.sql:
            return None, None, None, correction_info

        current_sql = generated.sql

        # Step 5.5: Validate and inject accumulated filters if needed
        # CRITICAL FIX: Always inject filters when we have accumulated_filters AND session context
        # Don't rely solely on preserve_filters flag which depends on LLM classification
        # This ensures filters are preserved even if the LLM misclassifies the query intent
        has_session_context = self.session and self.session.has_context()
        should_inject_filters = accumulated_filters and (preserve_filters or has_session_context)

        if should_inject_filters:
            logger.info(f"Step 5.5: Validating accumulated filters (preserve_filters={preserve_filters}, has_context={has_session_context})")
            current_sql, filter_warnings = self._validate_and_inject_filters(
                sql=current_sql,
                accumulated_filters=accumulated_filters,
                force_inject=True  # Always inject missing filters
            )
            correction_info['filter_warnings'] = filter_warnings
            if filter_warnings:
                pipeline_stages['filter_validation'] = {
                    'success': True,
                    'warnings': filter_warnings,
                    'filters_injected': len(filter_warnings) > 0,
                    'preserve_filters_flag': preserve_filters,
                    'has_session_context': has_session_context
                }

        last_error = None

        # Self-correction loop
        for attempt in range(MAX_CORRECTION_ATTEMPTS):
            correction_info['attempts'] = attempt + 1

            # Step 6: SQL Validation
            logger.info(f"Step 6: Validating SQL (attempt {attempt + 1})")
            step_start = time.time()
            validation = self.sql_validator.validate(current_sql)
            pipeline_stages['sql_validation'] = {
                'success': validation.is_valid,
                'time_ms': (time.time() - step_start) * 1000,
                'errors': validation.errors,
                'warnings': validation.warnings,
                'attempt': attempt + 1
            }

            if not validation.is_valid:
                last_error = f"Validation failed: {', '.join(validation.errors)}"
                logger.warning(f"SQL validation failed on attempt {attempt + 1}: {last_error}")

                # Try to correct
                if attempt < MAX_CORRECTION_ATTEMPTS - 1:
                    logger.info(f"Attempting SQL correction (attempt {attempt + 2})")
                    correction = self.sql_generator.generate_correction(
                        original_query=query,
                        failed_sql=current_sql,
                        error=last_error,
                        context=context
                    )
                    if correction.sql:
                        correction_info['corrections'].append({
                            'attempt': attempt + 1,
                            'error': last_error,
                            'corrected_sql': correction.sql
                        })
                        current_sql = correction.sql
                        continue
                # Can't correct further
                break

            # Step 7: Execution
            logger.info(f"Step 7: Executing SQL (attempt {attempt + 1})")
            step_start = time.time()
            execution = self.executor.execute(validation.validated_sql)
            pipeline_stages['execution'] = {
                'success': execution.success,
                'time_ms': (time.time() - step_start) * 1000,
                'row_count': execution.row_count,
                'attempt': attempt + 1
            }

            if execution.success:
                # Success!
                correction_info['final_success'] = True
                logger.info(f"SQL executed successfully on attempt {attempt + 1}")
                return validation, execution, validation.validated_sql, correction_info

            # Execution failed - try to correct
            last_error = execution.error_message
            logger.warning(f"SQL execution failed on attempt {attempt + 1}: {last_error}")

            if attempt < MAX_CORRECTION_ATTEMPTS - 1:
                logger.info(f"Attempting SQL correction after execution error (attempt {attempt + 2})")
                correction = self.sql_generator.generate_correction(
                    original_query=query,
                    failed_sql=validation.validated_sql,
                    error=last_error,
                    context=context
                )
                if correction.sql:
                    correction_info['corrections'].append({
                        'attempt': attempt + 1,
                        'error': last_error,
                        'corrected_sql': correction.sql
                    })
                    current_sql = correction.sql
                    continue

        # All attempts failed
        logger.error(f"SQL self-correction exhausted after {correction_info['attempts']} attempts")

        # Return last validation and a failed execution result
        from .models import ExecutionResult
        failed_execution = ExecutionResult(
            success=False,
            error_message=f"Query failed after {correction_info['attempts']} attempts. Last error: {last_error}"
        )
        return validation, failed_execution, current_sql, correction_info

    def _classify_intent(self, query: str) -> tuple[str, float]:
        """
        Use Claude to classify query intent.

        Args:
            query: User's input query

        Returns:
            Tuple of (intent_category, classification_time_ms)
        """
        start_time = time.time()

        try:
            # Use the SQL generator's provider for classification
            if hasattr(self.sql_generator, '_provider') and self.sql_generator._provider:
                request = LLMRequest(
                    prompt=INTENT_CLASSIFICATION_PROMPT.format(query=query),
                    max_tokens=20,  # Only need a single word
                    temperature=0.0  # Deterministic classification
                )
                response = self.sql_generator._provider.generate(request)
                intent = response.content.strip().upper()

                # Normalize intent
                valid_intents = ['CLINICAL_DATA', 'GREETING', 'HELP', 'IDENTITY', 'FAREWELL', 'STATUS', 'GENERAL']
                if intent not in valid_intents:
                    # Default to clinical data if unclear
                    intent = 'CLINICAL_DATA'

                elapsed_ms = (time.time() - start_time) * 1000
                logger.info(f"Intent classified as '{intent}' in {elapsed_ms:.0f}ms")
                return intent, elapsed_ms
            else:
                # Fallback: assume clinical data query
                return 'CLINICAL_DATA', 0

        except Exception as e:
            logger.warning(f"Intent classification failed: {e}, assuming CLINICAL_DATA")
            return 'CLINICAL_DATA', (time.time() - start_time) * 1000

    def _generate_conversational_response(self, query: str, intent: str) -> str:
        """
        Generate a natural conversational response for non-clinical queries.

        Args:
            query: User's input query
            intent: Classified intent category

        Returns:
            Natural language response from Claude
        """
        try:
            if hasattr(self.sql_generator, '_provider') and self.sql_generator._provider:
                request = LLMRequest(
                    prompt=query,
                    system_prompt=SAGE_SYSTEM_CONTEXT,
                    max_tokens=500,
                    temperature=0.7  # Slightly creative for natural responses
                )
                response = self.sql_generator._provider.generate(request)
                return response.content.strip()
            else:
                # Fallback response
                return "I'm SAGE, a clinical data analysis assistant. How can I help you query your clinical trial data?"

        except Exception as e:
            logger.error(f"Conversational response generation failed: {e}")
            return "I'm SAGE, a clinical data analysis assistant. Please ask me questions about your clinical trial data."

    def _check_instant_response(self, query: str, start_time: float) -> Optional[PipelineResult]:
        """
        Check for instant response patterns (no LLM needed).

        Uses regex patterns to detect simple greetings, thanks, and farewells
        for sub-100ms responses.

        Args:
            query: User's input query
            start_time: Pipeline start time for timing

        Returns:
            PipelineResult if instant pattern matched, None otherwise
        """
        query_clean = query.strip()

        for category, pattern in INSTANT_PATTERNS.items():
            if pattern.match(query_clean):
                elapsed_ms = (time.time() - start_time) * 1000
                logger.info(f"Instant response: {category} ({elapsed_ms:.1f}ms)")

                return PipelineResult(
                    success=True,
                    query=query,
                    answer=INSTANT_RESPONSES[category],
                    data=None,
                    row_count=0,
                    sql=None,
                    methodology=None,
                    confidence={
                        'score': 100,
                        'level': 'high',
                        'explanation': 'Instant response - pattern matched'
                    },
                    warnings=[],
                    pipeline_stages={
                        'instant_response': {
                            'success': True,
                            'time_ms': elapsed_ms,
                            'category': category
                        }
                    },
                    total_time_ms=elapsed_ms,
                    metadata={
                        'response_type': category,
                        'instant': True,
                        'pipeline_used': False
                    }
                )

        return None

    def _check_non_clinical(self, query: str) -> Optional[PipelineResult]:
        """
        Use Claude to classify query intent and handle non-clinical queries.

        This is Step 0 of the pipeline - uses LLM to intelligently determine
        if a query needs the full clinical data pipeline or just a conversational response.

        Args:
            query: User's input query

        Returns:
            PipelineResult if non-clinical query detected, None otherwise
        """
        # Classify intent using Claude
        intent, classification_time = self._classify_intent(query)

        # If it's a clinical data query, return None to continue with pipeline
        if intent == 'CLINICAL_DATA':
            return None

        # For non-clinical queries, generate a natural response
        logger.info(f"Non-clinical query detected: {intent}")

        response_start = time.time()
        answer = self._generate_conversational_response(query, intent)
        response_time = (time.time() - response_start) * 1000

        total_time = classification_time + response_time

        return PipelineResult(
            success=True,
            query=query,
            answer=answer,
            data=None,
            row_count=0,
            sql=None,
            methodology=None,
            confidence={
                'score': 100,
                'level': 'high',
                'explanation': 'Direct response - no data query required'
            },
            warnings=[],
            pipeline_stages={
                'intent_classification': {
                    'success': True,
                    'time_ms': classification_time,
                    'intent': intent
                },
                'response_generation': {
                    'success': True,
                    'time_ms': response_time
                }
            },
            total_time_ms=total_time,
            metadata={
                'response_type': intent.lower(),
                'intent': intent,
                'pipeline_used': False
            }
        )

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
            # Initialize working variables
            query_analysis = None
            direct_sql = None  # For follow-up queries that need direct SQL execution
            working_query = query  # Query to use for processing
            accumulated_filters = None  # Filters from previous queries for follow-ups
            conversation_context = None  # Conversation context for LLM

            # Note: Reference resolution (e.g., "list them", "of those") is now handled
            # by the LLM-based QueryAnalyzer at Step 1.5, not by pattern matching.
            # This allows the LLM to understand the infinite ways users phrase follow-ups.

            # STEP -1: Instant Response Check (regex, no LLM needed)
            # Check for simple greetings/thanks/bye patterns for sub-100ms response
            instant_result = self._check_instant_response(query, start_time)
            if instant_result:
                logger.info("Instant response matched - returning immediately")
                return instant_result

            # STEP 0: Non-Clinical Query Routing (LLM-based classification)
            # Uses LLM to classify intent - help, identity, etc. get conversational responses
            if not direct_sql:
                logger.info("Step 0: Checking for non-clinical query (LLM)")
                non_clinical_result = self._check_non_clinical(working_query)
                if non_clinical_result:
                    logger.info(f"Non-clinical query handled instantly")
                    return non_clinical_result

            # STEP 0.5: Check Cache
            if self.cache is not None and not direct_sql:
                logger.info("Step 0.5: Checking cache")
                # Include session_id in cache key for session-scoped caching
                # This ensures context-dependent queries (like "list them") are isolated per session
                cached_result = self.cache.get(query, session_id=self.session_id)
                if cached_result:
                    logger.info("Cache HIT - returning cached result")
                    # Reconstruct PipelineResult from cached dict
                    result = PipelineResult(**cached_result)
                    result.metadata['cache_hit'] = True
                    result.metadata['cache_key'] = self.cache._hash(query, session_id=self.session_id)
                    return result

            # STEP 1: Input Sanitization
            logger.info("Step 1: Sanitizing input")
            sanitization = self.sanitizer.sanitize(working_query)
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

            # STEP 1.5: Query Analysis (Structured Understanding)
            # Skip if we already have direct SQL from pattern matching
            if self.query_analyzer and not direct_sql:
                logger.info("Step 1.5: Analyzing query structure")
                step_start = time.time()
                try:
                    # Pass conversation context to analyzer for follow-up detection
                    previous_query = None
                    previous_sql = None
                    previous_result = None
                    conversation_context = None
                    accumulated_filters = None

                    if self.session and self.session.has_context():
                        previous_query = self.session.context.last_query
                        previous_sql = self.session.context.last_sql
                        previous_result = f"{self.session.context.last_count or 0} subjects"
                        # Get rich conversation context for LLM
                        conversation_context = self.session.get_conversation_context_for_llm()
                        accumulated_filters = self.session.context.get_accumulated_filters_sql()

                    query_analysis = self.query_analyzer.analyze(
                        clean_query,
                        previous_query=previous_query,
                        previous_sql=previous_sql,
                        previous_result=previous_result,
                        conversation_context=conversation_context,
                        accumulated_filters=accumulated_filters
                    )
                    pipeline_stages['query_analysis'] = {
                        'success': True,
                        'time_ms': (time.time() - step_start) * 1000,
                        'intent': query_analysis.intent.value if query_analysis.intent else 'unknown',
                        'subject': query_analysis.subject.value if query_analysis.subject else 'unknown',
                        'confidence': query_analysis.understanding_confidence,
                        'conditions_count': len(query_analysis.conditions),
                        'ambiguities_count': len(query_analysis.ambiguities),
                        'references_previous': query_analysis.references_previous,
                        'preserve_filters': query_analysis.preserve_filters
                    }

                    # Check if LLM detected a follow-up query
                    from .query_analyzer import QueryIntent

                    # DETAIL_PREVIOUS: Transform COUNT to SELECT with same filters
                    if query_analysis.intent == QueryIntent.DETAIL_PREVIOUS and self.session and self.session.context.last_sql:
                        logger.info("LLM detected DETAIL_PREVIOUS intent - transforming SQL")
                        list_sql = self.session._transform_count_to_select(self.session.context.last_sql)
                        if list_sql:
                            direct_sql = list_sql
                            working_query = "List subjects from previous query"
                            pipeline_stages['query_analysis']['follow_up_detected'] = True
                            logger.info(f"Transformed to list SQL: {direct_sql[:100]}...")

                    # REFINE_PREVIOUS: Keep accumulated filters and add new condition
                    elif query_analysis.intent == QueryIntent.REFINE_PREVIOUS and self.session:
                        logger.info("LLM detected REFINE_PREVIOUS intent - will preserve accumulated filters")
                        pipeline_stages['query_analysis']['refine_detected'] = True
                        pipeline_stages['query_analysis']['accumulated_filters'] = accumulated_filters
                        # The accumulated filters will be passed to context_builder and sql_generator

                    # STEP 1.7: Clarification Check
                    # Skip clarification for follow-up queries that reference previous results
                    if (self.clarification_manager and
                        self.clarification_manager.needs_clarification(query_analysis) and
                        not query_analysis.references_previous):
                        logger.info("Step 1.7: Generating clarification request")
                        clarification_request = self.clarification_manager.generate_clarification_request(query_analysis)

                        # Return clarification response instead of proceeding
                        return self._build_clarification_result(
                            query=query,
                            clarification=clarification_request,
                            start_time=start_time,
                            pipeline_stages=pipeline_stages
                        )

                except Exception as e:
                    logger.warning(f"Query analysis failed: {e}, continuing without analysis")
                    pipeline_stages['query_analysis'] = {
                        'success': False,
                        'error': str(e)
                    }

            # Check if we have direct SQL from session context (e.g., "list them")
            if direct_sql:
                # FAST PATH: Direct SQL execution - skip entity extraction, table resolution,
                # context building, and SQL generation. This preserves all filter criteria
                # from the previous query exactly.
                logger.info("Using direct SQL from session context (list them shortcut)")

                # Create minimal extraction and table resolution for downstream code
                extraction = EntityExtractionResult(
                    entities=[],
                    query_with_resolved=clean_query,
                    unresolved_terms=[],
                    processing_time_ms=0
                )

                # Extract table from SQL for table resolution
                import re as re_module
                table_match = re_module.search(r'FROM\s+(\w+)', direct_sql, re_module.IGNORECASE)
                table_name = table_match.group(1) if table_match else 'ADAE'

                table_resolution = TableResolution(
                    selected_table=table_name,
                    table_type="ADaM" if table_name.startswith('AD') else "SDTM",
                    domain=QueryDomain.ADVERSE_EVENTS if table_name in ['ADAE', 'AE'] else QueryDomain.DEMOGRAPHICS,
                    selection_reason="Direct SQL from session context (list them follow-up)",
                    population=PopulationType.SAFETY,
                    population_filter="SAFFL = 'Y'",
                    population_name="Safety Population",
                    columns_resolved={},  # Not applicable for direct SQL
                    fallback_used=False,
                    available_tables=list(self.table_resolver.available_tables.keys()),
                    table_columns=self.table_resolver.available_tables.get(table_name, []),
                    assumptions=["Using cached SQL from previous query (list them)"]
                )

                pipeline_stages['entity_extraction'] = {'skipped': True, 'reason': 'direct_sql'}
                pipeline_stages['table_resolution'] = {'skipped': True, 'reason': 'direct_sql', 'table': table_name}
                pipeline_stages['context_building'] = {'skipped': True, 'reason': 'direct_sql'}

                # Validate and execute the direct SQL
                validation = self.sql_validator.validate(direct_sql)
                pipeline_stages['sql_validation'] = {
                    'success': validation.is_valid,
                    'time_ms': 0,
                    'direct_sql': True
                }

                if validation.is_valid:
                    execution = self.executor.execute(direct_sql)
                    pipeline_stages['execution'] = {
                        'success': execution.success,
                        'time_ms': execution.execution_time_ms,
                        'row_count': execution.row_count if execution.success else 0,
                        'direct_sql': True
                    }
                else:
                    execution = None

                final_sql = direct_sql
                correction_info = {'attempts': 0, 'corrections': [], 'direct_sql': True}

            else:
                # NORMAL PATH: Full pipeline with entity extraction, table resolution, etc.

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
                # Use the query with resolved entities so table detection recognizes
                # corrected terms (e.g., "headakes" -> "HEADACHE" triggers AE domain)
                logger.info("Step 3: Resolving table and columns")
                step_start = time.time()
                resolved_query = extraction.query_with_resolved if extraction.query_with_resolved else clean_query
                table_resolution = self.table_resolver.resolve(
                    query=resolved_query
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

                # Check if this is a refinement query that needs to preserve filters
                preserve_filters = (query_analysis and
                                   query_analysis.preserve_filters and
                                   accumulated_filters)

                context = self.context_builder.build(
                    query=clean_query,
                    table_resolution=table_resolution,
                    entities=extraction.entities,
                    accumulated_filters=accumulated_filters,
                    preserve_filters=preserve_filters,
                    conversation_context=conversation_context
                )
                pipeline_stages['context_building'] = {
                    'success': True,
                    'time_ms': (time.time() - step_start) * 1000,
                    'token_estimate': context.token_count_estimate,
                    'preserve_filters': preserve_filters,
                    'accumulated_filters': accumulated_filters if preserve_filters else None
                }

                # STEPS 5-7: SQL Generation with Self-Correction Loop
                # Generates SQL, validates, executes - if execution fails, feeds error
                # back to Claude for correction (up to MAX_CORRECTION_ATTEMPTS times)
                validation, execution, final_sql, correction_info = self._execute_with_self_correction(
                    query=clean_query,
                    context=context,
                    pipeline_stages=pipeline_stages,
                    accumulated_filters=accumulated_filters,
                    preserve_filters=preserve_filters
                )

            # Add correction info to metadata
            pipeline_stages['self_correction'] = correction_info

            # Check results
            if not final_sql:
                return self._build_error_result(
                    query=query,
                    error="SQL generation failed",
                    stage="sql_generation",
                    start_time=start_time
                )

            if not validation or not validation.is_valid:
                error_msg = "SQL validation failed"
                if validation and validation.errors:
                    error_msg = f"SQL validation failed: {', '.join(validation.errors)}"
                return self._build_error_result(
                    query=query,
                    error=error_msg,
                    stage="sql_validation",
                    start_time=start_time,
                    user_hint=f"Tried {correction_info['attempts']} times to generate valid SQL. Try rephrasing your question."
                )

            if not execution or not execution.success:
                error_msg = execution.error_message if execution else "Execution failed"
                return self._build_error_result(
                    query=query,
                    error=error_msg,
                    stage="execution",
                    start_time=start_time,
                    user_hint=f"Tried {correction_info['attempts']} times. Try a different way of asking your question."
                )

            # STEP 7.5: Answer Verification
            verification_result = None
            if self.answer_verifier and query_analysis:
                logger.info("Step 7.5: Verifying answer")
                step_start = time.time()
                try:
                    verification_result = self.answer_verifier.verify(
                        query=clean_query,
                        analysis=query_analysis,
                        sql=final_sql,
                        result=execution
                    )
                    pipeline_stages['answer_verification'] = {
                        'success': verification_result.passed,
                        'time_ms': (time.time() - step_start) * 1000,
                        'overall_score': verification_result.overall_score,
                        'checks_passed': sum(1 for c in verification_result.checks if c.passed),
                        'checks_total': len(verification_result.checks),
                        'issues': verification_result.issues,
                        'warnings': verification_result.warnings
                    }

                    # Add verification warnings to result
                    if verification_result.warnings:
                        logger.info(f"Verification warnings: {verification_result.warnings}")

                except Exception as e:
                    logger.warning(f"Answer verification failed: {e}")
                    pipeline_stages['answer_verification'] = {
                        'success': False,
                        'error': str(e)
                    }

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
                sql=final_sql
            )
            pipeline_stages['explanation'] = {
                'success': True,
                'time_ms': (time.time() - step_start) * 1000
            }

            # STEP 9.5: Enriched Explanation (Factory 4.5)
            enriched_explanation = None
            if hasattr(self, 'explanation_enricher') and self.explanation_enricher:
                step_start = time.time()
                try:
                    # Extract columns used from SQL
                    columns_used = validation.columns_verified if validation else []

                    # Build entity resolution info for explanation
                    entities_resolved = []
                    for e in extraction.entities:
                        entities_resolved.append({
                            'original': e.original_term,
                            'resolved': e.matched_term,
                            'match_type': e.match_type,
                            'confidence': e.confidence
                        })

                    enriched_explanation = self.explanation_enricher.explain(
                        columns_used=columns_used,
                        table_used=table_resolution.selected_table,
                        population=table_resolution.population_name,
                        population_filter=table_resolution.population_filter,
                        entities_resolved=entities_resolved,
                        assumptions=table_resolution.assumptions
                    )
                    pipeline_stages['explanation_enrichment'] = {
                        'success': True,
                        'columns_explained': len(enriched_explanation.columns),
                        'time_ms': (time.time() - step_start) * 1000
                    }
                except Exception as e:
                    logger.warning(f"Explanation enrichment failed: {e}")
                    pipeline_stages['explanation_enrichment'] = {
                        'success': False,
                        'error': str(e),
                        'time_ms': (time.time() - step_start) * 1000
                    }

            # Build final result
            total_time = (time.time() - start_time) * 1000

            # Merge verification warnings into response warnings
            all_warnings = response['warnings'].copy()
            if verification_result and verification_result.warnings:
                all_warnings.extend(verification_result.warnings)

            # Build enriched methodology with column explanations
            methodology = response['methodology']
            if enriched_explanation:
                # Add enriched details to methodology for Details tab
                if methodology is None:
                    methodology = {}
                methodology['enriched_details'] = enriched_explanation.to_dict()
                methodology['enriched_markdown'] = enriched_explanation.to_markdown()

            result = PipelineResult(
                success=True,
                query=query,
                answer=response['answer'],
                data=response['data'],
                row_count=response['row_count'],
                sql=final_sql,
                methodology=methodology,
                confidence=response['confidence'],
                warnings=all_warnings,
                pipeline_stages=pipeline_stages,
                total_time_ms=total_time,
                metadata={
                    'pipeline_used': True,
                    'cache_hit': False,
                    'correction_attempts': correction_info['attempts'],
                    'self_corrected': len(correction_info['corrections']) > 0,
                    'verification_score': verification_result.overall_score if verification_result else None,
                    'verification_passed': verification_result.passed if verification_result else None,
                    'query_intent': query_analysis.intent.value if query_analysis else None,
                    'understanding_confidence': query_analysis.understanding_confidence if query_analysis else None,
                    'session_id': self.session_id,
                    'has_enriched_explanation': enriched_explanation is not None
                }
            )

            # Cache successful results (session-scoped for proper isolation)
            if self.cache is not None:
                logger.info("Caching successful result")
                self.cache.set(query, result.to_dict(), session_id=self.session_id)

            # Update session memory with this turn
            if self.session:
                # Determine if this is a refinement query that should accumulate filters
                is_refinement = (query_analysis is not None and
                                query_analysis.preserve_filters and
                                query_analysis.intent == QueryIntent.REFINE_PREVIOUS)

                self.session.add_turn(
                    query=query,
                    response_type='answer',
                    answer=response['answer'],
                    data=response['data'],
                    entities=[e.matched_term for e in extraction.entities] if extraction.entities else None,
                    table=table_resolution.selected_table,
                    population=table_resolution.population_name,
                    sql=final_sql,  # Use final_sql to ensure SQL is always passed
                    is_refinement=is_refinement
                )
                logger.info(f"Session {self.session_id}: Turn recorded (is_refinement={is_refinement})")

            return result

        except LLMTimeoutError as e:
            logger.error(f"LLM timeout: {e}")
            return self._build_error_result(
                query=query,
                error=str(e),
                stage="sql_generation",
                start_time=start_time,
                user_hint="The AI is taking longer than usual. This might happen with complex queries. "
                         "Try a simpler question or try again later."
            )

        except LLMConnectionError as e:
            logger.error(f"LLM connection error: {e}")
            return self._build_error_result(
                query=query,
                error=str(e),
                stage="sql_generation",
                start_time=start_time,
                user_hint="The AI service is currently unavailable. Please try again in a few moments."
            )

        except LLMModelError as e:
            logger.error(f"LLM model error: {e}")
            return self._build_error_result(
                query=query,
                error=str(e),
                stage="sql_generation",
                start_time=start_time,
                user_hint="Try rephrasing your question to be more specific about what data you want."
            )

        except LLMError as e:
            logger.error(f"LLM error: {e}")
            return self._build_error_result(
                query=query,
                error=str(e),
                stage="sql_generation",
                start_time=start_time,
                user_hint="There was an issue with the AI. Please try again."
            )

        except Exception as e:
            logger.exception(f"Pipeline error: {e}")
            return self._build_error_result(
                query=query,
                error=str(e),
                stage="unknown",
                start_time=start_time,
                user_hint="An unexpected error occurred. Please try again or rephrase your question."
            )

    def _build_error_result(self,
                             query: str,
                             error: str,
                             stage: str,
                             start_time: float,
                             user_hint: str = None
                            ) -> PipelineResult:
        """
        Build error result with user-friendly messaging.

        Args:
            query: Original query
            error: Technical error message
            stage: Pipeline stage where error occurred
            start_time: When processing started
            user_hint: User-friendly hint about what to do
        """
        total_time = (time.time() - start_time) * 1000

        # Use error humanizer if available (Factory 4.5)
        if hasattr(self, 'error_humanizer') and self.error_humanizer:
            humanized = self.error_humanizer.humanize(
                error_type=stage,
                technical_error=error,
                context={'stage': stage}
            )
            answer = self.error_humanizer.format_for_chat(humanized)
            if user_hint:
                answer += f"\n\n**Additional tip:** {user_hint}"
        elif user_hint:
            answer = f"I couldn't complete your request.\n\n**What happened:** {error}\n\n**Suggestion:** {user_hint}"
        else:
            error_response = self.response_builder.build_error_response(
                query=query,
                error=error,
                stage=stage
            )
            answer = error_response['answer']

        return PipelineResult(
            success=False,
            query=query,
            answer=answer,
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
            total_time_ms=total_time,
            metadata={'error_type': stage, 'has_hint': bool(user_hint)}
        )

    def _build_clarification_result(
        self,
        query: str,
        clarification,
        start_time: float,
        pipeline_stages: Dict[str, Any]
    ) -> PipelineResult:
        """
        Build a clarification request result.

        When the query is ambiguous or we need more information,
        return a clarification request instead of attempting to answer.

        Args:
            query: Original query
            clarification: ClarificationRequest object
            start_time: When processing started
            pipeline_stages: Stages completed so far
        """
        total_time = (time.time() - start_time) * 1000

        # Build user-friendly clarification message
        answer_parts = [clarification.message]

        for i, question in enumerate(clarification.questions):
            answer_parts.append(f"\n**{question.question}**")
            for option in question.options:
                answer_parts.append(f"  {option.id}. {option.text}")

        if clarification.suggested_rephrasing:
            answer_parts.append(f"\n**Tip:** {clarification.suggested_rephrasing}")

        answer = "\n".join(answer_parts)

        # Record in session as clarification turn
        if self.session:
            self.session.add_turn(
                query=query,
                response_type='clarification',
                answer=answer
            )

        return PipelineResult(
            success=False,  # Not a successful answer - needs clarification
            query=query,
            answer=answer,
            data=None,
            row_count=0,
            sql=None,
            methodology=None,
            confidence={
                'score': 0,
                'level': 'unknown',
                'explanation': 'Query requires clarification before answering'
            },
            warnings=[],
            pipeline_stages=pipeline_stages,
            total_time_ms=total_time,
            metadata={
                'response_type': 'clarification',
                'clarification_needed': True,
                'questions_count': len(clarification.questions),
                'session_id': self.session_id
            }
        )

    def is_ready(self) -> Dict[str, bool]:
        """Check if all components are ready."""
        status = {
            'database': False,
            'claude': False,
            'metadata': False,
            'query_analyzer': False,
            'clarification_manager': False,
            'answer_verifier': False,
            'session_memory': False,
            'data_knowledge': False,
            # Factory 4.5 components
            'synonym_resolver': False,
            'explanation_enricher': False,
            'error_humanizer': False
        }

        # Check database
        if self.config.db_path:
            status['database'] = self.executor.validate_connection()

        # Check Claude LLM
        if not self.config.use_mock:
            status['claude'] = self.sql_generator.is_available()
        else:
            status['claude'] = True

        # Check metadata
        if self.config.metadata_path:
            status['metadata'] = Path(self.config.metadata_path).exists()

        # Check new accuracy components
        status['query_analyzer'] = self.query_analyzer is not None or not self.config.enable_query_analysis
        status['clarification_manager'] = self.clarification_manager is not None or not self.config.enable_clarification
        status['answer_verifier'] = self.answer_verifier is not None or not self.config.enable_verification
        status['session_memory'] = self.session is not None or not self.config.enable_session_memory
        status['data_knowledge'] = self.data_knowledge is not None or not self.config.enable_data_knowledge

        # Check Factory 4.5 LLM-enhanced components
        status['synonym_resolver'] = (
            hasattr(self, 'synonym_resolver') and self.synonym_resolver is not None
        ) or not self.config.enable_synonym_resolution
        status['explanation_enricher'] = (
            hasattr(self, 'explanation_enricher') and self.explanation_enricher is not None
        ) or not self.config.enable_explanation_enrichment
        status['error_humanizer'] = (
            hasattr(self, 'error_humanizer') and self.error_humanizer is not None
        ) or not self.config.enable_error_humanization

        return status

    def get_session_id(self) -> Optional[str]:
        """Get the current session ID."""
        return self.session_id

    def get_session_context(self) -> Optional[Dict[str, Any]]:
        """Get the current session context."""
        if self.session:
            return self.session.to_dict()
        return None

    def switch_session(self, session_id: str) -> None:
        """
        Switch to a different session for conversation context.

        This allows a shared pipeline instance to handle multiple conversations
        by switching sessions before processing each query.

        Args:
            session_id: Session ID to switch to (typically conversation_id from API)
        """
        if not self.config.enable_session_memory:
            logger.debug("Session memory not enabled, switch_session has no effect")
            return

        if session_id == self.session_id:
            logger.debug(f"Already using session {session_id}")
            return

        # Get or create session from session manager
        session_manager = get_session_manager()
        self.session = session_manager.get_session(session_id)
        self.session_id = self.session.session_id
        logger.info(f"Switched to session {self.session_id}")

    def process_with_session(self, query: str, session_id: str = None) -> PipelineResult:
        """
        Process a query with a specific session context.

        This is the recommended method for API endpoints that need to maintain
        conversation context across requests. It switches to the specified session
        before processing and maintains context for follow-up questions.

        Example:
            # First query
            result = pipeline.process_with_session("How many patients had headaches?", conv_id)
            # Follow-up - "they" resolves to "patients with headaches"
            result = pipeline.process_with_session("List them", conv_id)

        Args:
            query: User's natural language query
            session_id: Session ID for conversation context (e.g., conversation_id)

        Returns:
            PipelineResult with answer, data, and methodology
        """
        if session_id and self.config.enable_session_memory:
            self.switch_session(session_id)

        return self.process(query)


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
    use_mock: bool = False,
    fuzzy_matcher=None,
    meddra_lookup=None,
    fuzzy_index_path: str = None,
    auto_load_factory3: bool = True,
    available_tables: Dict[str, List[str]] = None,
    db_connection=None,
    session_id: str = None,
    enable_query_analysis: bool = True,
    enable_clarification: bool = True,
    enable_verification: bool = True,
    enable_session_memory: bool = True,
    enable_data_knowledge: bool = True,
    # Factory 4.5: LLM-Enhanced Features
    enable_synonym_resolution: bool = True,
    enable_explanation_enrichment: bool = True,
    enable_error_humanization: bool = True
) -> InferencePipeline:
    """
    Factory function to create a configured pipeline.

    Args:
        db_path: Path to DuckDB database
        metadata_path: Path to golden_metadata.json
        use_mock: Use mock components for testing
        fuzzy_matcher: Factory 3 fuzzy matcher (pre-loaded)
        meddra_lookup: Factory 3.5 MedDRA lookup (pre-loaded)
        fuzzy_index_path: Path to fuzzy_index.pkl (auto-loads if provided)
        auto_load_factory3: Automatically try to load Factory 3/3.5 components
        available_tables: Pre-loaded table schema dict to avoid DuckDB lock conflicts
        db_connection: Shared DuckDB connection to avoid lock conflicts
        session_id: Optional session ID for conversation memory
        enable_query_analysis: Enable structured query understanding
        enable_clarification: Enable clarification requests for ambiguous queries
        enable_verification: Enable answer verification
        enable_session_memory: Enable session conversation memory
        enable_data_knowledge: Enable data-driven knowledge
        enable_synonym_resolution: Enable LLM-suggested synonyms validated against data
        enable_explanation_enrichment: Enable metadata-based column explanations
        enable_error_humanization: Enable natural language error messages

    Returns:
        Configured InferencePipeline (uses Claude for SQL generation)
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
        use_mock=use_mock,
        available_tables=available_tables,
        db_connection=db_connection,
        enable_query_analysis=enable_query_analysis,
        enable_clarification=enable_clarification,
        enable_verification=enable_verification,
        enable_session_memory=enable_session_memory,
        enable_data_knowledge=enable_data_knowledge,
        # Factory 4.5: LLM-Enhanced Features
        enable_synonym_resolution=enable_synonym_resolution,
        enable_explanation_enrichment=enable_explanation_enrichment,
        enable_error_humanization=enable_error_humanization
    )

    return InferencePipeline(
        config=config,
        fuzzy_matcher=fuzzy_matcher,
        meddra_lookup=meddra_lookup,
        session_id=session_id
    )
