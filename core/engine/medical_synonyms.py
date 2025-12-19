# SAGE - Medical Synonyms Dictionary
# ===================================
"""
Medical Synonyms Dictionary
===========================
Comprehensive mapping of:
1. UK/US spelling variants (anaemia/anemia)
2. Colloquial to medical terms (belly pain -> abdominal pain)
3. Complex phrase mappings (low blood cell count -> WBC decreased)

This module provides a single source of truth for term normalization
in clinical data queries.

Used by: entity_extractor.py, context_builder.py
"""

from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass


@dataclass
class SynonymMapping:
    """A mapping from a user term to standardized medical term(s)."""
    canonical_term: str  # Primary term to use (e.g., 'ANAEMIA')
    all_variants: Tuple[str, ...]  # All acceptable variants for IN clause
    table: str = 'ADAE'
    column: str = 'AEDECOD'


# =============================================================================
# UK/US SPELLING VARIANTS
# =============================================================================
# These terms need IN clauses because data may contain either spelling

UK_US_VARIANTS: Dict[str, Tuple[str, ...]] = {
    # Anaemia variants
    'anaemia': ('ANAEMIA', 'ANEMIA'),
    'anemia': ('ANAEMIA', 'ANEMIA'),

    # Diarrhoea variants
    'diarrhoea': ('DIARRHOEA', 'DIARRHEA'),
    'diarrhea': ('DIARRHOEA', 'DIARRHEA'),

    # Oedema variants
    'oedema': ('OEDEMA', 'EDEMA'),
    'edema': ('OEDEMA', 'EDEMA'),

    # Haemorrhage variants
    'haemorrhage': ('HAEMORRHAGE', 'HEMORRHAGE'),
    'hemorrhage': ('HAEMORRHAGE', 'HEMORRHAGE'),

    # Haematoma variants
    'haematoma': ('HAEMATOMA', 'HEMATOMA'),
    'hematoma': ('HAEMATOMA', 'HEMATOMA'),

    # Leukaemia variants
    'leukaemia': ('LEUKAEMIA', 'LEUKEMIA'),
    'leukemia': ('LEUKAEMIA', 'LEUKEMIA'),

    # Tumour variants
    'tumour': ('TUMOUR', 'TUMOR'),
    'tumor': ('TUMOUR', 'TUMOR'),

    # Behaviour variants (less common in AE but might appear)
    'behaviour': ('BEHAVIOUR', 'BEHAVIOR'),
    'behavior': ('BEHAVIOUR', 'BEHAVIOR'),

    # Colour variants
    'colour': ('COLOUR', 'COLOR'),
    'color': ('COLOUR', 'COLOR'),

    # Haemoglobin variants
    'haemoglobin': ('HAEMOGLOBIN', 'HEMOGLOBIN'),
    'hemoglobin': ('HAEMOGLOBIN', 'HEMOGLOBIN'),

    # Faeces variants
    'faeces': ('FAECES', 'FECES'),
    'feces': ('FAECES', 'FECES'),

    # Oesophageal variants
    'oesophageal': ('OESOPHAGEAL', 'ESOPHAGEAL'),
    'esophageal': ('OESOPHAGEAL', 'ESOPHAGEAL'),

    # Foetal variants
    'foetal': ('FOETAL', 'FETAL'),
    'fetal': ('FOETAL', 'FETAL'),
}


# =============================================================================
# COLLOQUIAL TO MEDICAL TERM MAPPINGS
# =============================================================================
# Maps everyday language to standardized MedDRA terms

COLLOQUIAL_MAPPINGS: Dict[str, SynonymMapping] = {
    # Body pain terms
    'belly pain': SynonymMapping('ABDOMINAL PAIN', ('ABDOMINAL PAIN',)),
    'stomach pain': SynonymMapping('ABDOMINAL PAIN', ('ABDOMINAL PAIN',)),
    'tummy pain': SynonymMapping('ABDOMINAL PAIN', ('ABDOMINAL PAIN',)),
    'stomach ache': SynonymMapping('ABDOMINAL PAIN', ('ABDOMINAL PAIN',)),
    'bellyache': SynonymMapping('ABDOMINAL PAIN', ('ABDOMINAL PAIN',)),

    # Fever terms
    'fever': SynonymMapping('PYREXIA', ('PYREXIA',)),
    'high temperature': SynonymMapping('PYREXIA', ('PYREXIA',)),
    'febrile': SynonymMapping('PYREXIA', ('PYREXIA',)),

    # Headache variants
    'headache': SynonymMapping('HEADACHE', ('HEADACHE',)),
    'head pain': SynonymMapping('HEADACHE', ('HEADACHE',)),
    'head ache': SynonymMapping('HEADACHE', ('HEADACHE',)),

    # Nausea/vomiting
    'sick': SynonymMapping('NAUSEA', ('NAUSEA',)),  # Context dependent
    'feeling sick': SynonymMapping('NAUSEA', ('NAUSEA',)),
    'nauseous': SynonymMapping('NAUSEA', ('NAUSEA',)),
    'throwing up': SynonymMapping('VOMITING', ('VOMITING',)),
    'vomit': SynonymMapping('VOMITING', ('VOMITING',)),

    # Tiredness
    'tiredness': SynonymMapping('FATIGUE', ('FATIGUE',)),
    'tired': SynonymMapping('FATIGUE', ('FATIGUE',)),
    'exhaustion': SynonymMapping('FATIGUE', ('FATIGUE',)),
    'exhausted': SynonymMapping('FATIGUE', ('FATIGUE',)),

    # Skin conditions
    'skin rash': SynonymMapping('RASH', ('RASH',)),
    'hives': SynonymMapping('URTICARIA', ('URTICARIA',)),
    'itchy skin': SynonymMapping('PRURITUS', ('PRURITUS',)),
    'itching': SynonymMapping('PRURITUS', ('PRURITUS',)),
    'itch': SynonymMapping('PRURITUS', ('PRURITUS',)),

    # Respiratory
    'shortness of breath': SynonymMapping('DYSPNOEA', ('DYSPNOEA', 'DYSPNEA')),
    'breathlessness': SynonymMapping('DYSPNOEA', ('DYSPNOEA', 'DYSPNEA')),
    'difficulty breathing': SynonymMapping('DYSPNOEA', ('DYSPNOEA', 'DYSPNEA')),
    'runny nose': SynonymMapping('RHINORRHOEA', ('RHINORRHOEA', 'RHINORRHEA')),

    # Dizziness
    'dizzy': SynonymMapping('DIZZINESS', ('DIZZINESS',)),
    'lightheaded': SynonymMapping('DIZZINESS', ('DIZZINESS',)),
    'light headed': SynonymMapping('DIZZINESS', ('DIZZINESS',)),
    'vertigo': SynonymMapping('VERTIGO', ('VERTIGO',)),

    # Sleep
    'sleeplessness': SynonymMapping('INSOMNIA', ('INSOMNIA',)),
    'cant sleep': SynonymMapping('INSOMNIA', ('INSOMNIA',)),
    "can't sleep": SynonymMapping('INSOMNIA', ('INSOMNIA',)),
    'trouble sleeping': SynonymMapping('INSOMNIA', ('INSOMNIA',)),

    # Appetite
    'loss of appetite': SynonymMapping('DECREASED APPETITE', ('DECREASED APPETITE',)),
    'no appetite': SynonymMapping('DECREASED APPETITE', ('DECREASED APPETITE',)),

    # Joint/muscle
    'joint pain': SynonymMapping('ARTHRALGIA', ('ARTHRALGIA',)),
    'muscle pain': SynonymMapping('MYALGIA', ('MYALGIA',)),
    'muscle ache': SynonymMapping('MYALGIA', ('MYALGIA',)),

    # Blood pressure
    'high blood pressure': SynonymMapping('HYPERTENSION', ('HYPERTENSION',)),
    'low blood pressure': SynonymMapping('HYPOTENSION', ('HYPOTENSION',)),

    # Heart
    'fast heartbeat': SynonymMapping('TACHYCARDIA', ('TACHYCARDIA',)),
    'rapid heartbeat': SynonymMapping('TACHYCARDIA', ('TACHYCARDIA',)),
    'slow heartbeat': SynonymMapping('BRADYCARDIA', ('BRADYCARDIA',)),
    'irregular heartbeat': SynonymMapping('ARRHYTHMIA', ('ARRHYTHMIA',)),
    'heart palpitations': SynonymMapping('PALPITATIONS', ('PALPITATIONS',)),

    # Swelling
    'swelling': SynonymMapping('OEDEMA', ('OEDEMA', 'EDEMA')),
    'swollen': SynonymMapping('OEDEMA', ('OEDEMA', 'EDEMA')),

    # Weight
    'weight gain': SynonymMapping('WEIGHT INCREASED', ('WEIGHT INCREASED',)),
    'weight loss': SynonymMapping('WEIGHT DECREASED', ('WEIGHT DECREASED',)),

    # Bleeding
    'bleeding': SynonymMapping('HAEMORRHAGE', ('HAEMORRHAGE', 'HEMORRHAGE')),
    'nose bleed': SynonymMapping('EPISTAXIS', ('EPISTAXIS',)),
    'nosebleed': SynonymMapping('EPISTAXIS', ('EPISTAXIS',)),
}


# =============================================================================
# COMPLEX PHRASE MAPPINGS
# =============================================================================
# Maps multi-word descriptions to specific MedDRA terms

COMPLEX_PHRASE_MAPPINGS: Dict[str, SynonymMapping] = {
    # Blood cell terms
    'low blood cell count': SynonymMapping(
        'WHITE BLOOD CELL COUNT DECREASED',
        ('WHITE BLOOD CELL COUNT DECREASED',)
    ),
    'low white blood cell': SynonymMapping(
        'WHITE BLOOD CELL COUNT DECREASED',
        ('WHITE BLOOD CELL COUNT DECREASED',)
    ),
    'low white blood cells': SynonymMapping(
        'WHITE BLOOD CELL COUNT DECREASED',
        ('WHITE BLOOD CELL COUNT DECREASED',)
    ),
    'decreased white blood cell': SynonymMapping(
        'WHITE BLOOD CELL COUNT DECREASED',
        ('WHITE BLOOD CELL COUNT DECREASED',)
    ),
    'wbc decreased': SynonymMapping(
        'WHITE BLOOD CELL COUNT DECREASED',
        ('WHITE BLOOD CELL COUNT DECREASED',)
    ),
    'low wbc': SynonymMapping(
        'WHITE BLOOD CELL COUNT DECREASED',
        ('WHITE BLOOD CELL COUNT DECREASED',)
    ),

    # Red blood cell terms
    'low red blood cell': SynonymMapping(
        'RED BLOOD CELL COUNT DECREASED',
        ('RED BLOOD CELL COUNT DECREASED',)
    ),
    'decreased red blood cell': SynonymMapping(
        'RED BLOOD CELL COUNT DECREASED',
        ('RED BLOOD CELL COUNT DECREASED',)
    ),

    # Platelet terms
    'low platelet': SynonymMapping(
        'PLATELET COUNT DECREASED',
        ('PLATELET COUNT DECREASED',)
    ),
    'low platelets': SynonymMapping(
        'PLATELET COUNT DECREASED',
        ('PLATELET COUNT DECREASED',)
    ),
    'decreased platelets': SynonymMapping(
        'PLATELET COUNT DECREASED',
        ('PLATELET COUNT DECREASED',)
    ),

    # Neutrophil terms
    'low neutrophil': SynonymMapping(
        'NEUTROPHIL COUNT DECREASED',
        ('NEUTROPHIL COUNT DECREASED',)
    ),
    'low neutrophils': SynonymMapping(
        'NEUTROPHIL COUNT DECREASED',
        ('NEUTROPHIL COUNT DECREASED',)
    ),

    # Liver function
    'elevated liver enzymes': SynonymMapping(
        'HEPATIC ENZYME INCREASED',
        ('HEPATIC ENZYME INCREASED', 'LIVER FUNCTION TEST ABNORMAL')
    ),
    'high liver enzymes': SynonymMapping(
        'HEPATIC ENZYME INCREASED',
        ('HEPATIC ENZYME INCREASED', 'LIVER FUNCTION TEST ABNORMAL')
    ),
    'liver damage': SynonymMapping(
        'HEPATOTOXICITY',
        ('HEPATOTOXICITY', 'LIVER INJURY')
    ),

    # Kidney function
    'kidney failure': SynonymMapping(
        'RENAL FAILURE',
        ('RENAL FAILURE', 'ACUTE KIDNEY INJURY')
    ),
    'kidney injury': SynonymMapping(
        'ACUTE KIDNEY INJURY',
        ('ACUTE KIDNEY INJURY', 'RENAL IMPAIRMENT')
    ),

    # Infections
    'urinary tract infection': SynonymMapping(
        'URINARY TRACT INFECTION',
        ('URINARY TRACT INFECTION',)
    ),
    'uti': SynonymMapping(
        'URINARY TRACT INFECTION',
        ('URINARY TRACT INFECTION',)
    ),
    'upper respiratory infection': SynonymMapping(
        'UPPER RESPIRATORY TRACT INFECTION',
        ('UPPER RESPIRATORY TRACT INFECTION',)
    ),

    # Other common terms
    'blood in urine': SynonymMapping(
        'HAEMATURIA',
        ('HAEMATURIA', 'HEMATURIA')
    ),
    'blood in stool': SynonymMapping(
        'HAEMATOCHEZIA',
        ('HAEMATOCHEZIA', 'HEMATOCHEZIA')
    ),
    'allergic reaction': SynonymMapping(
        'HYPERSENSITIVITY',
        ('HYPERSENSITIVITY', 'ALLERGIC REACTION')
    ),
    'drug reaction': SynonymMapping(
        'DRUG HYPERSENSITIVITY',
        ('DRUG HYPERSENSITIVITY',)
    ),
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_spelling_variants(term: str) -> Optional[Tuple[str, ...]]:
    """
    Get UK/US spelling variants for a term.

    Args:
        term: The term to check (case-insensitive)

    Returns:
        Tuple of all spelling variants, or None if no variants exist
    """
    term_lower = term.lower().strip()
    return UK_US_VARIANTS.get(term_lower)


def get_colloquial_mapping(term: str) -> Optional[SynonymMapping]:
    """
    Get medical term mapping for a colloquial term.

    Args:
        term: The colloquial term (case-insensitive)

    Returns:
        SynonymMapping with canonical term and variants, or None
    """
    term_lower = term.lower().strip()
    return COLLOQUIAL_MAPPINGS.get(term_lower)


def get_complex_phrase_mapping(phrase: str) -> Optional[SynonymMapping]:
    """
    Get medical term mapping for a complex phrase.

    Args:
        phrase: The phrase to check (case-insensitive)

    Returns:
        SynonymMapping with canonical term and variants, or None
    """
    phrase_lower = phrase.lower().strip()
    return COMPLEX_PHRASE_MAPPINGS.get(phrase_lower)


def resolve_medical_term(term: str) -> Optional[SynonymMapping]:
    """
    Comprehensive term resolution - checks all dictionaries.

    Priority:
    1. Complex phrase mappings (most specific)
    2. Colloquial mappings
    3. UK/US spelling variants

    Args:
        term: The term or phrase to resolve

    Returns:
        SynonymMapping with resolved term(s), or None if no mapping found
    """
    term_lower = term.lower().strip()

    # 1. Check complex phrases first (most specific)
    if term_lower in COMPLEX_PHRASE_MAPPINGS:
        return COMPLEX_PHRASE_MAPPINGS[term_lower]

    # 2. Check colloquial mappings
    if term_lower in COLLOQUIAL_MAPPINGS:
        return COLLOQUIAL_MAPPINGS[term_lower]

    # 3. Check UK/US variants
    if term_lower in UK_US_VARIANTS:
        variants = UK_US_VARIANTS[term_lower]
        return SynonymMapping(
            canonical_term=variants[0],  # First variant is canonical
            all_variants=variants
        )

    return None


def has_spelling_variants(term: str) -> bool:
    """Check if a term has UK/US spelling variants."""
    return term.lower().strip() in UK_US_VARIANTS


def get_all_synonym_keys() -> Set[str]:
    """Get all terms that have synonym mappings."""
    keys = set()
    keys.update(UK_US_VARIANTS.keys())
    keys.update(COLLOQUIAL_MAPPINGS.keys())
    keys.update(COMPLEX_PHRASE_MAPPINGS.keys())
    return keys


def build_in_clause(mapping: SynonymMapping) -> str:
    """
    Build SQL IN clause for a synonym mapping.

    Args:
        mapping: SynonymMapping with variants

    Returns:
        SQL snippet like "IN ('TERM1', 'TERM2')" or "= 'TERM'" if single variant
    """
    variants = mapping.all_variants
    if len(variants) == 1:
        return f"= '{variants[0]}'"
    else:
        quoted = ", ".join(f"'{v}'" for v in variants)
        return f"IN ({quoted})"


# =============================================================================
# SYNONYM HINTS FOR LLM PROMPTS
# =============================================================================

# Compact hint for system prompt - details in TERMS section
CRITICAL_SYNONYM_HINTS = """
UK/US SPELLINGS: anaemia/anemia, diarrhoea/diarrhea, oedema/edema - use IN clause with both.
COLLOQUIAL: fever=PYREXIA, belly pain=ABDOMINAL PAIN. Use UPPER(AEDECOD)."""
