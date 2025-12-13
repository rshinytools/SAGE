# SAGE Engine Package
"""
Factory 4 - Inference Engine
============================
Query processing engine for SAGE.

9-Step Pipeline:
1. Input Sanitization - Security checks
2. Entity Extraction - Clinical term resolution
3. Table Resolution - Clinical Rules Engine
4. Context Building - LLM context preparation
5. SQL Generation - LLM-powered SQL generation
6. SQL Validation - Safety and correctness checks
7. Execution - DuckDB query execution
8. Confidence Scoring - Result reliability assessment
9. Explanation Generation - Human-readable response
"""

# Models
from .models import (
    SanitizationResult,
    EntityMatch,
    EntityExtractionResult,
    RouteDecision,
    LLMContext,
    GeneratedSQL,
    ValidationResult,
    ExecutionResult,
    ConfidenceScore,
    ConfidenceLevel,
    QueryMethodology,
    PipelineResult
)

# Clinical Configuration
from .clinical_config import (
    QueryDomain,
    PopulationType,
    TablePriority,
    ColumnPriority,
    ClinicalQueryConfig,
    DEFAULT_CLINICAL_CONFIG
)

# Pipeline Components
from .input_sanitizer import InputSanitizer, SanitizerConfig
from .entity_extractor import EntityExtractor, SimpleEntityExtractor
from .table_resolver import TableResolver, TableResolution
from .context_builder import ContextBuilder, SchemaInfo
from .sql_generator import SQLGenerator, OllamaConfig, MockSQLGenerator
from .sql_validator import SQLValidator, ValidatorConfig
from .executor import SQLExecutor, ExecutorConfig, MockExecutor
from .confidence_scorer import ConfidenceScorer, ScorerConfig, get_confidence_color
from .explanation_generator import ExplanationGenerator, ResponseBuilder

# Main Pipeline
from .pipeline import (
    InferencePipeline,
    PipelineConfig,
    create_pipeline
)

__all__ = [
    # Models
    'SanitizationResult',
    'EntityMatch',
    'EntityExtractionResult',
    'RouteDecision',
    'LLMContext',
    'GeneratedSQL',
    'ValidationResult',
    'ExecutionResult',
    'ConfidenceScore',
    'ConfidenceLevel',
    'QueryMethodology',
    'PipelineResult',

    # Clinical Config
    'QueryDomain',
    'PopulationType',
    'TablePriority',
    'ColumnPriority',
    'ClinicalQueryConfig',
    'DEFAULT_CLINICAL_CONFIG',

    # Components
    'InputSanitizer',
    'SanitizerConfig',
    'EntityExtractor',
    'SimpleEntityExtractor',
    'TableResolver',
    'TableResolution',
    'ContextBuilder',
    'SchemaInfo',
    'SQLGenerator',
    'OllamaConfig',
    'MockSQLGenerator',
    'SQLValidator',
    'ValidatorConfig',
    'SQLExecutor',
    'ExecutorConfig',
    'MockExecutor',
    'ConfidenceScorer',
    'ScorerConfig',
    'get_confidence_color',
    'ExplanationGenerator',
    'ResponseBuilder',

    # Pipeline
    'InferencePipeline',
    'PipelineConfig',
    'create_pipeline'
]
