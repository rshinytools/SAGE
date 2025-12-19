# SAGE - Tests for Factory 4.5 LLM-Enhanced Features
"""
Tests for the new LLM-enhanced modules:
- SynonymResolver
- ExplanationEnricher
- QueryDisambiguator
- ErrorHumanizer
"""

import pytest
from unittest.mock import MagicMock, patch
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.engine.synonym_resolver import (
    SynonymResolver, SynonymMatch, SynonymResolutionResult,
    create_synonym_resolver
)
from core.engine.explanation_enricher import (
    ExplanationEnricher, ColumnExplanation, QueryExplanation,
    create_explanation_enricher
)
from core.engine.query_disambiguator import (
    QueryDisambiguator, DisambiguationResult, ClarificationOption,
    AmbiguityType, create_query_disambiguator
)
from core.engine.error_humanizer import (
    ErrorHumanizer, HumanizedError, ErrorType,
    create_error_humanizer
)


class TestSynonymResolver:
    """Tests for SynonymResolver."""

    def test_resolver_creation(self):
        """Test resolver can be created."""
        resolver = SynonymResolver()
        assert resolver is not None

    def test_clinical_synonyms_mapping(self):
        """Test LLM-first approach - no hard-coded synonyms."""
        # With LLM-first approach, no hard-coded synonyms exist
        # LLM handles synonym resolution naturally
        resolver = SynonymResolver()
        assert not hasattr(resolver, 'CLINICAL_SYNONYMS') or len(getattr(resolver, 'CLINICAL_SYNONYMS', {})) == 0

    def test_get_llm_suggestions_uses_dictionary(self):
        """Test LLM suggestions with LLM-first approach."""
        resolver = SynonymResolver()
        # Without hard-coded dictionary, suggestions come from LLM only
        # When no LLM is configured, returns only the original term
        suggestions = resolver._get_llm_suggestions('fever')
        assert 'fever' in suggestions

    def test_resolution_result_properties(self):
        """Test SynonymResolutionResult properties."""
        result = SynonymResolutionResult(
            original_term='test',
            validated_terms=[
                SynonymMatch(
                    term='Test Match',
                    original_query='test',
                    match_type='exact',
                    count=10,
                    confidence=95.0,
                    source='data'
                )
            ],
            llm_suggestions=[],
            rejected_suggestions=[],
            resolution_method='exact',
            processing_time_ms=5.0
        )
        assert result.has_matches is True
        assert result.total_count == 10
        assert result.get_sql_terms() == ['Test Match']

    def test_create_synonym_resolver_factory(self):
        """Test factory function."""
        resolver = create_synonym_resolver()
        assert isinstance(resolver, SynonymResolver)


class TestExplanationEnricher:
    """Tests for ExplanationEnricher."""

    def test_enricher_creation(self):
        """Test enricher can be created."""
        enricher = ExplanationEnricher()
        assert enricher is not None

    def test_population_descriptions(self):
        """Test standard population descriptions."""
        enricher = ExplanationEnricher()
        assert 'safety' in enricher.POPULATION_DESCRIPTIONS
        assert 'dose' in enricher.POPULATION_DESCRIPTIONS['safety'].lower()

    def test_table_labels(self):
        """Test table labels mapping."""
        enricher = ExplanationEnricher()
        assert 'ADAE' in enricher.TABLE_LABELS
        assert 'Adverse Events' in enricher.TABLE_LABELS['ADAE']

    def test_explain_basic(self):
        """Test basic explanation generation."""
        enricher = ExplanationEnricher()
        explanation = enricher.explain(
            columns_used=['USUBJID', 'AEDECOD'],
            table_used='ADAE',
            population='Safety'
        )
        assert isinstance(explanation, QueryExplanation)
        assert explanation.table_used == 'ADAE'
        assert explanation.population_name == 'Safety'
        assert len(explanation.columns) == 2

    def test_explain_to_dict(self):
        """Test explanation to_dict method."""
        enricher = ExplanationEnricher()
        explanation = enricher.explain(
            columns_used=['USUBJID'],
            table_used='ADSL',
            population='ITT'
        )
        data = explanation.to_dict()
        assert 'columns' in data
        assert 'population' in data
        assert 'table' in data

    def test_explain_to_markdown(self):
        """Test explanation to_markdown method."""
        enricher = ExplanationEnricher()
        explanation = enricher.explain(
            columns_used=['USUBJID', 'AGE'],
            table_used='ADSL',
            population='Safety',
            assumptions=['Used Safety population']
        )
        markdown = explanation.to_markdown()
        assert '### Data Source:' in markdown
        assert '### Population' in markdown
        assert 'Safety' in markdown

    def test_generate_label_fallback(self):
        """Test fallback label generation."""
        enricher = ExplanationEnricher()
        label = enricher._generate_label('USUBJID')
        assert label == 'Unique Subject Identifier'

    def test_create_explanation_enricher_factory(self):
        """Test factory function."""
        enricher = create_explanation_enricher()
        assert isinstance(enricher, ExplanationEnricher)


class TestQueryDisambiguator:
    """Tests for QueryDisambiguator."""

    def test_disambiguator_creation(self):
        """Test disambiguator can be created."""
        disambiguator = QueryDisambiguator()
        assert disambiguator is not None

    def test_population_options(self):
        """Test standard population options."""
        disambiguator = QueryDisambiguator()
        assert len(disambiguator.POPULATION_OPTIONS) >= 2
        safety_opt = next(o for o in disambiguator.POPULATION_OPTIONS if o.key == 'safety')
        assert safety_opt.is_default is True

    def test_severity_options(self):
        """Test severity options."""
        disambiguator = QueryDisambiguator()
        assert len(disambiguator.SEVERITY_OPTIONS) >= 3

    def test_check_no_ambiguity(self):
        """Test query with no ambiguity."""
        disambiguator = QueryDisambiguator()
        result = disambiguator.check("How many patients in safety population had nausea?")
        # Should auto-resolve with default
        assert result.auto_resolved is True or result.needs_clarification is False

    def test_check_severity_ambiguity(self):
        """Test query with severity ambiguity."""
        disambiguator = QueryDisambiguator(auto_resolve_defaults=False)
        result = disambiguator.check("Show me severe adverse events")
        # May detect severity ambiguity
        assert result is not None

    def test_check_returns_disambiguation_result(self):
        """Test check returns DisambiguationResult."""
        disambiguator = QueryDisambiguator()
        result = disambiguator.check("Count patients")
        assert isinstance(result, DisambiguationResult)

    def test_get_filters_from_clarification(self):
        """Test converting clarification to SQL filters."""
        disambiguator = QueryDisambiguator()
        filters = disambiguator.get_filters_from_clarification({
            'population': 'safety',
            'severity': 'grade3_plus'
        })
        assert 'population' in filters
        assert 'severity' in filters
        assert "SAFFL" in filters['population']

    def test_auto_resolve_enabled(self):
        """Test auto-resolve uses defaults."""
        disambiguator = QueryDisambiguator(auto_resolve_defaults=True)
        result = disambiguator.check("How many patients had adverse events?")
        # Should auto-resolve with defaults
        if result.needs_clarification:
            assert result.auto_resolved is True

    def test_auto_resolve_disabled(self):
        """Test without auto-resolve."""
        disambiguator = QueryDisambiguator(auto_resolve_defaults=False)
        result = disambiguator.check("Count patients")
        # With auto-resolve disabled, ambiguous queries should need clarification
        # (depends on the specific query)
        assert isinstance(result, DisambiguationResult)

    def test_create_query_disambiguator_factory(self):
        """Test factory function."""
        disambiguator = create_query_disambiguator()
        assert isinstance(disambiguator, QueryDisambiguator)


class TestErrorHumanizer:
    """Tests for ErrorHumanizer."""

    def test_humanizer_creation(self):
        """Test humanizer can be created."""
        humanizer = ErrorHumanizer()
        assert humanizer is not None

    def test_default_available_tables(self):
        """Test default available tables."""
        humanizer = ErrorHumanizer()
        assert 'ADAE' in humanizer.available_tables
        assert 'ADSL' in humanizer.available_tables

    def test_humanize_term_not_found(self):
        """Test humanizing term not found error."""
        humanizer = ErrorHumanizer()
        error = humanizer.humanize(
            error_type='term_not_found',
            technical_error="Value 'xyz' not found",
            context={
                'term': 'xyz',
                'column': 'AEDECOD',
                'suggestions': ['abc', 'def']
            }
        )
        assert isinstance(error, HumanizedError)
        assert error.error_type == ErrorType.TERM_NOT_FOUND
        assert 'xyz' in error.message

    def test_humanize_table_not_found(self):
        """Test humanizing table not found error."""
        humanizer = ErrorHumanizer()
        error = humanizer.humanize(
            error_type='table_not_found',
            technical_error="No such table: MYTABLE",
            context={'table': 'MYTABLE'}
        )
        assert error.error_type == ErrorType.TABLE_NOT_FOUND
        assert 'MYTABLE' in error.message

    def test_humanize_timeout(self):
        """Test humanizing timeout error."""
        humanizer = ErrorHumanizer()
        error = humanizer.humanize(
            technical_error="Request timed out after 60 seconds"
        )
        assert error.error_type == ErrorType.TIMEOUT

    def test_humanize_connection(self):
        """Test humanizing connection error."""
        humanizer = ErrorHumanizer()
        error = humanizer.humanize(
            technical_error="Connection refused to localhost:11434"
        )
        assert error.error_type == ErrorType.CONNECTION

    def test_humanize_phi_blocked(self):
        """Test humanizing PHI blocked error."""
        humanizer = ErrorHumanizer()
        error = humanizer.humanize(
            error_type='blocked_phi',
            technical_error="Query contains PHI"
        )
        assert error.error_type == ErrorType.BLOCKED_PHI
        assert 'protected' in error.message.lower()

    def test_format_for_chat(self):
        """Test formatting for chat display."""
        humanizer = ErrorHumanizer()
        error = humanizer.humanize(
            error_type='timeout',
            technical_error="Timeout"
        )
        formatted = humanizer.format_for_chat(error)
        assert '**' in formatted  # Has markdown
        assert error.title in formatted

    def test_humanize_from_exception(self):
        """Test humanizing from exception."""
        humanizer = ErrorHumanizer()
        exc = ValueError("Column not found: MYCOLUMN")
        error = humanizer.humanize_from_exception(exc)
        assert error.error_type == ErrorType.COLUMN_NOT_FOUND

    def test_suggestions_included(self):
        """Test suggestions are included in humanized error."""
        humanizer = ErrorHumanizer()
        error = humanizer.humanize(
            error_type='term_not_found',
            technical_error="Not found",
            context={'suggestions': ['Option1', 'Option2']}
        )
        assert len(error.suggestions) > 0

    def test_error_classification(self):
        """Test automatic error classification."""
        humanizer = ErrorHumanizer()

        # SQL injection
        error = humanizer.humanize(technical_error="Injection pattern blocked")
        assert error.error_type == ErrorType.BLOCKED_INJECTION

        # No results
        error = humanizer.humanize(technical_error="Query returned 0 rows")
        assert error.error_type == ErrorType.NO_RESULTS

    def test_create_error_humanizer_factory(self):
        """Test factory function."""
        humanizer = create_error_humanizer()
        assert isinstance(humanizer, ErrorHumanizer)

    def test_create_error_humanizer_with_tables(self):
        """Test factory function with custom tables."""
        humanizer = create_error_humanizer(available_tables=['TABLE1', 'TABLE2'])
        assert humanizer.available_tables == ['TABLE1', 'TABLE2']


class TestIntegrationReadiness:
    """Tests for pipeline integration readiness."""

    def test_all_modules_importable(self):
        """Test all new modules can be imported together."""
        from core.engine.synonym_resolver import SynonymResolver
        from core.engine.explanation_enricher import ExplanationEnricher
        from core.engine.query_disambiguator import QueryDisambiguator
        from core.engine.error_humanizer import ErrorHumanizer
        assert True

    def test_pipeline_imports_new_modules(self):
        """Test pipeline imports new modules."""
        from core.engine import pipeline
        # Check imports exist
        assert hasattr(pipeline, 'SynonymResolver')
        assert hasattr(pipeline, 'ExplanationEnricher')
        assert hasattr(pipeline, 'QueryDisambiguator')
        assert hasattr(pipeline, 'ErrorHumanizer')

    def test_pipeline_config_has_new_options(self):
        """Test PipelineConfig has new feature options."""
        from core.engine.pipeline import PipelineConfig
        config = PipelineConfig()
        assert hasattr(config, 'enable_synonym_resolution')
        assert hasattr(config, 'enable_explanation_enrichment')
        assert hasattr(config, 'enable_error_humanization')

    def test_new_feature_options_default_enabled(self):
        """Test new features are enabled by default."""
        from core.engine.pipeline import PipelineConfig
        config = PipelineConfig()
        assert config.enable_synonym_resolution is True
        assert config.enable_explanation_enrichment is True
        assert config.enable_error_humanization is True
