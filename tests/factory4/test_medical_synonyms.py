# Test Medical Synonyms Module
"""
Tests for the medical_synonyms module and its integration
with entity_extractor and context_builder.

These tests verify that:
1. UK/US spelling variants are properly resolved
2. Colloquial terms are mapped to medical terms
3. Complex phrases are resolved correctly
4. Entity extractor uses synonyms correctly
5. Context builder generates proper IN clauses
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.medical_synonyms import (
    resolve_medical_term,
    get_spelling_variants,
    get_colloquial_mapping,
    get_complex_phrase_mapping,
    has_spelling_variants,
    build_in_clause,
    UK_US_VARIANTS,
    COLLOQUIAL_MAPPINGS,
    COMPLEX_PHRASE_MAPPINGS,
    SynonymMapping,
)


class TestUKUSSpellingVariants:
    """Test UK/US spelling variant handling."""

    def test_anaemia_variants(self):
        """Anaemia should have both UK and US spellings."""
        variants = get_spelling_variants("anaemia")
        assert variants is not None
        assert "ANAEMIA" in variants
        assert "ANEMIA" in variants

    def test_anemia_variants(self):
        """Anemia (US) should return same variants as anaemia (UK)."""
        variants = get_spelling_variants("anemia")
        assert variants is not None
        assert "ANAEMIA" in variants
        assert "ANEMIA" in variants

    def test_diarrhoea_variants(self):
        """Diarrhoea should have both spellings."""
        variants = get_spelling_variants("diarrhoea")
        assert variants is not None
        assert "DIARRHOEA" in variants
        assert "DIARRHEA" in variants

    def test_diarrhea_variants(self):
        """Diarrhea (US) should return same variants."""
        variants = get_spelling_variants("diarrhea")
        assert variants is not None
        assert "DIARRHOEA" in variants
        assert "DIARRHEA" in variants

    def test_oedema_variants(self):
        """Oedema/edema should have both spellings."""
        uk_variants = get_spelling_variants("oedema")
        us_variants = get_spelling_variants("edema")
        assert uk_variants == us_variants
        assert "OEDEMA" in uk_variants
        assert "EDEMA" in uk_variants

    def test_haemorrhage_variants(self):
        """Haemorrhage/hemorrhage variants."""
        variants = get_spelling_variants("haemorrhage")
        assert variants is not None
        assert "HAEMORRHAGE" in variants
        assert "HEMORRHAGE" in variants

    def test_case_insensitive(self):
        """Lookups should be case-insensitive."""
        assert get_spelling_variants("ANAEMIA") == get_spelling_variants("anaemia")
        assert get_spelling_variants("AnAeMiA") == get_spelling_variants("anaemia")

    def test_has_spelling_variants(self):
        """has_spelling_variants should identify terms with variants."""
        assert has_spelling_variants("anaemia")
        assert has_spelling_variants("anemia")
        assert has_spelling_variants("diarrhoea")
        assert not has_spelling_variants("headache")  # No UK/US variant
        assert not has_spelling_variants("nausea")


class TestColloquialMappings:
    """Test colloquial to medical term mappings."""

    def test_belly_pain_mapping(self):
        """Belly pain should map to ABDOMINAL PAIN."""
        mapping = get_colloquial_mapping("belly pain")
        assert mapping is not None
        assert mapping.canonical_term == "ABDOMINAL PAIN"

    def test_stomach_pain_mapping(self):
        """Stomach pain should also map to ABDOMINAL PAIN."""
        mapping = get_colloquial_mapping("stomach pain")
        assert mapping is not None
        assert mapping.canonical_term == "ABDOMINAL PAIN"

    def test_fever_mapping(self):
        """Fever should map to PYREXIA."""
        mapping = get_colloquial_mapping("fever")
        assert mapping is not None
        assert mapping.canonical_term == "PYREXIA"

    def test_high_temperature_mapping(self):
        """High temperature should map to PYREXIA."""
        mapping = get_colloquial_mapping("high temperature")
        assert mapping is not None
        assert mapping.canonical_term == "PYREXIA"

    def test_tiredness_mapping(self):
        """Tiredness should map to FATIGUE."""
        mapping = get_colloquial_mapping("tiredness")
        assert mapping is not None
        assert mapping.canonical_term == "FATIGUE"

    def test_shortness_of_breath_mapping(self):
        """Shortness of breath should map to DYSPNOEA with variants."""
        mapping = get_colloquial_mapping("shortness of breath")
        assert mapping is not None
        assert mapping.canonical_term == "DYSPNOEA"
        assert "DYSPNOEA" in mapping.all_variants
        assert "DYSPNEA" in mapping.all_variants

    def test_hives_mapping(self):
        """Hives should map to URTICARIA."""
        mapping = get_colloquial_mapping("hives")
        assert mapping is not None
        assert mapping.canonical_term == "URTICARIA"

    def test_itching_mapping(self):
        """Itching should map to PRURITUS."""
        mapping = get_colloquial_mapping("itching")
        assert mapping is not None
        assert mapping.canonical_term == "PRURITUS"


class TestComplexPhraseMappings:
    """Test complex phrase mappings."""

    def test_low_blood_cell_count_mapping(self):
        """Low blood cell count should map to WBC DECREASED."""
        mapping = get_complex_phrase_mapping("low blood cell count")
        assert mapping is not None
        assert mapping.canonical_term == "WHITE BLOOD CELL COUNT DECREASED"

    def test_low_white_blood_cell_mapping(self):
        """Low white blood cell variants."""
        mapping1 = get_complex_phrase_mapping("low white blood cell")
        mapping2 = get_complex_phrase_mapping("low white blood cells")
        assert mapping1 is not None
        assert mapping2 is not None
        assert mapping1.canonical_term == "WHITE BLOOD CELL COUNT DECREASED"
        assert mapping2.canonical_term == "WHITE BLOOD CELL COUNT DECREASED"

    def test_low_platelet_mapping(self):
        """Low platelet should map correctly."""
        mapping = get_complex_phrase_mapping("low platelets")
        assert mapping is not None
        assert mapping.canonical_term == "PLATELET COUNT DECREASED"

    def test_elevated_liver_enzymes_mapping(self):
        """Elevated liver enzymes mapping."""
        mapping = get_complex_phrase_mapping("elevated liver enzymes")
        assert mapping is not None
        assert "HEPATIC ENZYME INCREASED" in mapping.all_variants

    def test_uti_mapping(self):
        """UTI should map to URINARY TRACT INFECTION."""
        mapping = get_complex_phrase_mapping("uti")
        assert mapping is not None
        assert mapping.canonical_term == "URINARY TRACT INFECTION"


class TestResolveMedicalTerm:
    """Test the unified resolve_medical_term function."""

    def test_priority_complex_phrase(self):
        """Complex phrases should be resolved first."""
        # "low blood cell count" is a complex phrase
        mapping = resolve_medical_term("low blood cell count")
        assert mapping is not None
        assert mapping.canonical_term == "WHITE BLOOD CELL COUNT DECREASED"

    def test_priority_colloquial(self):
        """Colloquial terms should be resolved."""
        mapping = resolve_medical_term("fever")
        assert mapping is not None
        assert mapping.canonical_term == "PYREXIA"

    def test_priority_uk_us(self):
        """UK/US variants should be resolved."""
        mapping = resolve_medical_term("anaemia")
        assert mapping is not None
        assert len(mapping.all_variants) == 2
        assert "ANAEMIA" in mapping.all_variants
        assert "ANEMIA" in mapping.all_variants

    def test_unknown_term_returns_none(self):
        """Unknown terms should return None."""
        mapping = resolve_medical_term("xyzabc123")
        assert mapping is None

    def test_case_insensitive_resolution(self):
        """Resolution should be case-insensitive."""
        lower = resolve_medical_term("fever")
        upper = resolve_medical_term("FEVER")
        mixed = resolve_medical_term("FeVeR")

        assert lower is not None
        assert upper is not None
        assert mixed is not None
        assert lower.canonical_term == upper.canonical_term == mixed.canonical_term


class TestBuildInClause:
    """Test SQL IN clause generation."""

    def test_single_variant_equals(self):
        """Single variant should use = operator."""
        mapping = SynonymMapping("HEADACHE", ("HEADACHE",))
        clause = build_in_clause(mapping)
        assert clause == "= 'HEADACHE'"

    def test_multiple_variants_in(self):
        """Multiple variants should use IN operator."""
        mapping = SynonymMapping("ANAEMIA", ("ANAEMIA", "ANEMIA"))
        clause = build_in_clause(mapping)
        assert "IN" in clause
        assert "'ANAEMIA'" in clause
        assert "'ANEMIA'" in clause


class TestEntityExtractorIntegration:
    """Test integration with entity extractor."""

    @pytest.fixture
    def extractor(self):
        """Create a simple entity extractor."""
        from core.engine.entity_extractor import EntityExtractor
        return EntityExtractor(min_confidence=70.0)

    def test_extract_belly_pain(self, extractor):
        """Entity extractor should resolve 'belly pain'."""
        result = extractor.extract("How many patients had belly pain?")

        # Should find the belly pain entity
        belly_pain_entities = [
            e for e in result.entities
            if e.original_term == "belly pain"
        ]
        assert len(belly_pain_entities) == 1
        assert belly_pain_entities[0].matched_term == "ABDOMINAL PAIN"
        assert belly_pain_entities[0].match_type == "medical_synonym"

    def test_extract_fever(self, extractor):
        """Entity extractor should resolve 'fever' to PYREXIA."""
        result = extractor.extract("How many subjects had fever?")

        fever_entities = [
            e for e in result.entities
            if e.original_term == "fever"
        ]
        assert len(fever_entities) == 1
        assert fever_entities[0].matched_term == "PYREXIA"

    def test_extract_anemia_with_variants(self, extractor):
        """Entity extractor should resolve 'anemia' with both variants."""
        result = extractor.extract("Count cases of anemia")

        anemia_entities = [
            e for e in result.entities
            if e.original_term == "anemia"
        ]
        assert len(anemia_entities) == 1
        entity = anemia_entities[0]
        assert entity.match_type == "medical_synonym"
        assert entity.metadata is not None
        assert "all_variants" in entity.metadata
        variants = entity.metadata["all_variants"]
        assert "ANAEMIA" in variants
        assert "ANEMIA" in variants

    def test_extract_low_blood_cell_count(self, extractor):
        """Entity extractor should resolve complex phrase."""
        result = extractor.extract("Count occurrences of low blood cell count")

        # Should find the complex phrase
        wbc_entities = [
            e for e in result.entities
            if "blood" in e.original_term.lower()
        ]
        assert len(wbc_entities) >= 1
        assert any(
            e.matched_term == "WHITE BLOOD CELL COUNT DECREASED"
            for e in wbc_entities
        )


class TestContextBuilderIntegration:
    """Test integration with context builder."""

    @pytest.fixture
    def context_builder(self):
        """Create a context builder."""
        from core.engine.context_builder import ContextBuilder
        return ContextBuilder()

    @pytest.fixture
    def mock_table_resolution(self):
        """Create mock table resolution."""
        from core.engine.table_resolver import TableResolution
        from core.engine.clinical_config import QueryDomain, PopulationType

        return TableResolution(
            selected_table="ADAE",
            table_type="ADaM",
            domain=QueryDomain.ADVERSE_EVENTS,
            selection_reason="Test",
            population=PopulationType.SAFETY,
            population_filter="SAFFL = 'Y'",
            population_name="Safety",
            columns_resolved={},
            fallback_used=False,
            available_tables=["ADAE", "ADSL"],
            table_columns=["USUBJID", "AEDECOD", "SAFFL"]
        )

    def test_entity_context_with_variants(self, context_builder):
        """Entity context should show IN clause for variants."""
        from core.engine.models import EntityMatch

        entities = [
            EntityMatch(
                original_term="anemia",
                matched_term="ANAEMIA",
                match_type="medical_synonym",
                confidence=95.0,
                table="ADAE",
                column="AEDECOD",
                metadata={"all_variants": ("ANAEMIA", "ANEMIA")}
            )
        ]

        context = context_builder._build_entity_context(entities)

        assert "IN" in context
        assert "'ANAEMIA'" in context
        assert "'ANEMIA'" in context
        assert "AEDECOD" in context

    def test_entity_context_single_variant(self, context_builder):
        """Single variant should use = operator."""
        from core.engine.models import EntityMatch

        entities = [
            EntityMatch(
                original_term="fever",
                matched_term="PYREXIA",
                match_type="medical_synonym",
                confidence=95.0,
                table="ADAE",
                column="AEDECOD",
                metadata={"all_variants": ("PYREXIA",)}
            )
        ]

        context = context_builder._build_entity_context(entities)

        assert "= 'PYREXIA'" in context
        assert "AEDECOD" in context

    def test_system_prompt_includes_synonym_hints(self, context_builder, mock_table_resolution):
        """System prompt should include critical synonym hints."""
        system_prompt = context_builder._build_system_prompt(mock_table_resolution)

        # Should include the synonym guidance
        assert "SYNONYM" in system_prompt.upper() or "anemia" in system_prompt.lower()


class TestGoldenSuiteFailingQueries:
    """Test the specific failing queries from the golden suite."""

    @pytest.fixture
    def extractor(self):
        """Create entity extractor."""
        from core.engine.entity_extractor import EntityExtractor
        return EntityExtractor(min_confidence=70.0)

    def test_q70_anemia_extraction(self, extractor):
        """Q70: 'Count cases of anemia' should extract with both variants."""
        result = extractor.extract("Count cases of anemia")

        # Find anemia entity
        anemia_entity = next(
            (e for e in result.entities if "anemia" in e.original_term.lower()),
            None
        )

        assert anemia_entity is not None, "Should find 'anemia' entity"
        assert anemia_entity.metadata is not None, "Should have metadata"
        variants = anemia_entity.metadata.get("all_variants", ())
        assert "ANAEMIA" in variants, "Should include UK spelling"
        assert "ANEMIA" in variants, "Should include US spelling"

    def test_q72_belly_pain_extraction(self, extractor):
        """Q72: 'How many participants had belly pain?' should map to ABDOMINAL PAIN."""
        result = extractor.extract("How many participants had belly pain?")

        belly_entity = next(
            (e for e in result.entities if "belly pain" in e.original_term.lower()),
            None
        )

        assert belly_entity is not None, "Should find 'belly pain' entity"
        assert belly_entity.matched_term == "ABDOMINAL PAIN", "Should map to ABDOMINAL PAIN"

    def test_q73_low_blood_cell_count_extraction(self, extractor):
        """Q73: 'Count occurrences of low blood cell count' should map to WBC DECREASED."""
        result = extractor.extract("Count occurrences of low blood cell count")

        wbc_entity = next(
            (e for e in result.entities if "low blood cell count" in e.original_term.lower()),
            None
        )

        assert wbc_entity is not None, "Should find 'low blood cell count' entity"
        assert wbc_entity.matched_term == "WHITE BLOOD CELL COUNT DECREASED", \
            "Should map to WHITE BLOOD CELL COUNT DECREASED"

    def test_q75_fever_extraction(self, extractor):
        """Q75: 'How many subjects had fever?' should map to PYREXIA."""
        result = extractor.extract("How many subjects had fever?")

        fever_entity = next(
            (e for e in result.entities if "fever" in e.original_term.lower()),
            None
        )

        assert fever_entity is not None, "Should find 'fever' entity"
        assert fever_entity.matched_term == "PYREXIA", "Should map to PYREXIA"
