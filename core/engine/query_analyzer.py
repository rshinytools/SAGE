# SAGE - Query Analyzer Module
# ============================
"""
Query Analyzer
==============
Analyzes natural language queries to understand intent and extract conditions
BEFORE generating SQL. This prevents semantic mismatches.

Key Principle: Understand Before Acting
- Parse query structure
- Map terms to CDISC concepts
- Detect ambiguities
- Request clarification when uncertain
"""

import json
import logging
import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)


# =============================================================================
# QUERY INTENT TYPES
# =============================================================================

class QueryIntent(Enum):
    """Types of questions the system can answer."""

    # Counting questions
    COUNT_SUBJECTS = "count_subjects"    # How many patients...
    COUNT_EVENTS = "count_events"        # How many adverse events...
    COUNT_RECORDS = "count_records"      # How many lab values...

    # Listing questions
    LIST_VALUES = "list_values"          # What are the adverse events...
    LIST_TOP_N = "list_top_n"            # Top 5 most common...
    LIST_RECORDS = "list_records"        # Show all patients with...

    # Follow-up questions (referencing previous results)
    DETAIL_PREVIOUS = "detail_previous"  # List them, show the subjects, who are they
    REFINE_PREVIOUS = "refine_previous"  # Of those, how many... (sub-filter)

    # Distribution questions
    DISTRIBUTION = "distribution"        # Age distribution, gender breakdown

    # Comparison questions
    COMPARE_GROUPS = "compare_groups"    # Compare treatment vs placebo

    # Trend questions
    TREND_OVER_TIME = "trend"            # How did X change over time

    # Existence questions
    EXISTS = "exists"                    # Are there any patients with...

    # Aggregation
    AVERAGE = "average"                  # Average age, mean value
    SUM = "sum"                          # Total count
    MIN_MAX = "min_max"                  # Minimum/maximum values

    # Unknown/unclear
    UNCLEAR = "unclear"                  # Need clarification


class QuerySubject(Enum):
    """What is being counted or listed."""

    SUBJECTS = "subjects"      # Unique patients/subjects
    EVENTS = "events"          # Adverse events, occurrences
    RECORDS = "records"        # Individual records/rows
    SITES = "sites"            # Study sites
    PARAMETERS = "parameters"  # Lab parameters, vital signs


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class AlternativeMapping:
    """An alternative interpretation of a query condition."""
    interpretation: str
    column: str
    operator: str
    values: List[str]
    confidence: float


@dataclass
class QueryCondition:
    """A single condition in the query."""

    # Original text
    original_text: str  # "sick from the drug"

    # Mapped CDISC concept
    cdisc_concept: str  # "Treatment-related Adverse Event"

    # Column mapping
    column: str  # "AEREL"
    operator: str  # "IN", "=", ">", "<", "LIKE", "BETWEEN"
    values: List[str]  # ["DRUG", "POSSIBLE", "PROBABLE"]

    # Mapping confidence
    mapping_confidence: float = 1.0

    # Alternative interpretations
    alternatives: List[AlternativeMapping] = field(default_factory=list)


@dataclass
class Ambiguity:
    """An ambiguous part of the query."""
    text: str  # The ambiguous text
    interpretations: List[str]  # Possible interpretations
    clarification_needed: bool = False
    suggested_question: Optional[str] = None


@dataclass
class TimeFrame:
    """Time constraints for the query."""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    relative: Optional[str] = None  # "last 30 days", "during treatment"


@dataclass
class QueryAnalysis:
    """Structured understanding of user's question."""

    # Original query
    original_query: str

    # What type of question
    intent: QueryIntent

    # What are we counting/listing
    subject: QuerySubject

    # Conditions/filters
    conditions: List[QueryCondition] = field(default_factory=list)

    # Time constraints
    time_frame: Optional[TimeFrame] = None

    # Grouping
    group_by: Optional[List[str]] = None

    # Sorting/limiting
    order_by: Optional[str] = None
    limit: Optional[int] = None

    # Ambiguities detected
    ambiguities: List[Ambiguity] = field(default_factory=list)

    # Confidence in understanding
    understanding_confidence: float = 0.0

    # Does this need clarification?
    needs_clarification: bool = False
    clarification_question: Optional[str] = None

    # Suggested table based on analysis
    suggested_table: Optional[str] = None

    # Suggested population
    suggested_population: Optional[str] = None

    # Follow-up context
    references_previous: bool = False  # Does this query reference previous results?
    preserve_filters: bool = False  # Should previous filters be preserved?
    new_condition_text: Optional[str] = None  # The new condition being added (for REFINE)

    # Analysis timestamp
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'original_query': self.original_query,
            'intent': self.intent.value,
            'subject': self.subject.value,
            'conditions': [
                {
                    'original_text': c.original_text,
                    'cdisc_concept': c.cdisc_concept,
                    'column': c.column,
                    'operator': c.operator,
                    'values': c.values,
                    'mapping_confidence': c.mapping_confidence
                }
                for c in self.conditions
            ],
            'group_by': self.group_by,
            'limit': self.limit,
            'ambiguities': [
                {
                    'text': a.text,
                    'interpretations': a.interpretations,
                    'clarification_needed': a.clarification_needed
                }
                for a in self.ambiguities
            ],
            'understanding_confidence': self.understanding_confidence,
            'needs_clarification': self.needs_clarification,
            'clarification_question': self.clarification_question,
            'suggested_table': self.suggested_table,
            'suggested_population': self.suggested_population,
            'references_previous': self.references_previous,
            'preserve_filters': self.preserve_filters,
            'new_condition_text': self.new_condition_text,
            'timestamp': self.timestamp
        }


# =============================================================================
# QUERY ANALYZER
# =============================================================================

QUERY_ANALYSIS_PROMPT = '''Analyze this clinical data question and extract structured information.

Question: {query}

Available data:
- ADSL: Subject demographics (USUBJID, AGE, SEX, RACE, SAFFL, ITTFL, TRTSDT, TRTEDT, TRT01A)
- ADAE: Adverse events (USUBJID, AEDECOD, AEBODSYS, AESEV, AESER, AEREL, ATOXGR, AESTDTC, TRTEMFL, SEX)
- ADLB: Lab results (USUBJID, PARAMCD, PARAM, AVAL, BASE, CHG, ANRLO, ANRHI, ABLFL)
- ADVS: Vital signs (USUBJID, PARAMCD, PARAM, AVAL, BASE, CHG, ABLFL)
- DM: Demographics (SDTM) - same as ADSL for basic demographics
- AE: Adverse Events (SDTM) - raw AE data

Common CDISC columns:
- SAFFL: Safety population flag ('Y'/'N')
- ITTFL: Intent-to-treat flag ('Y'/'N')
- AESER: Serious adverse event ('Y'/'N')
- AEREL: Drug relationship ('RELATED', 'POSSIBLY RELATED', 'NOT RELATED', etc.)
- ATOXGR/AETOXGR: Toxicity grade (1-5)
- AESEV: Severity ('MILD', 'MODERATE', 'SEVERE')
- TRTEMFL: Treatment-emergent flag ('Y'/'N')

IMPORTANT - NO CLARIFICATION NEEDED FOR:
- Standard medical terms (nausea, headache, anaemia, hypertension, etc.)
- UK/US spelling variants (anaemia/anemia, diarrhoea/diarrhea, oedema/edema)
- Common clinical queries - you understand these naturally
- Set ambiguities to empty array [] for straightforward clinical questions

ONLY ask for clarification when the query is genuinely unclear or could mean completely different things.

Respond with JSON only (no markdown, no explanation):
{{
    "intent": "COUNT_SUBJECTS|COUNT_EVENTS|LIST_VALUES|LIST_TOP_N|LIST_RECORDS|DISTRIBUTION|COMPARE_GROUPS|AVERAGE|EXISTS|UNCLEAR",
    "subject": "SUBJECTS|EVENTS|RECORDS|SITES|PARAMETERS",
    "conditions": [
        {{
            "original_text": "the part of the question this refers to",
            "cdisc_concept": "the CDISC standard concept this maps to",
            "column": "the column name (e.g., AEDECOD, AEREL, ATOXGR)",
            "operator": "=|IN|>|<|>=|<=|LIKE|BETWEEN|IS NOT NULL",
            "values": ["value1", "value2"],
            "confidence": 0.0-1.0
        }}
    ],
    "group_by": ["column1"] or null,
    "limit": number or null,
    "ambiguities": [],
    "understanding_confidence": 0.9,
    "suggested_table": "ADAE|ADSL|ADLB|ADVS|DM|AE",
    "suggested_population": "Safety|ITT|All Subjects",
    "suggested_clarification": null
}}'''


# Context-aware prompt for follow-up queries
CONTEXT_AWARE_ANALYSIS_PROMPT = '''Analyze this clinical data question IN THE CONTEXT of the previous conversation.

CONVERSATION HISTORY:
{conversation_context}

ACCUMULATED FILTERS FROM PREVIOUS QUERIES:
{accumulated_filters}

CURRENT QUESTION: {query}

CRITICAL INSTRUCTIONS FOR FOLLOW-UP QUERIES:
When the user says "of these", "of those", "out of these", "among them", etc., they are asking
about a SUBSET of the previous results. You MUST preserve ALL previous filter conditions.

DETERMINE THE INTENT:
1. DETAIL_PREVIOUS: User wants to LIST/SHOW details of the previous results
   - Examples: "list them", "show them", "can you list them", "show the subjects",
     "who are they", "what are their IDs", "which patients", "who were they"
   - Key phrases: "list", "show", "who", "which" + referencing previous results
   - Action: Transform COUNT to SELECT, keep same filters
   - IMPORTANT: Return DETAIL_PREVIOUS even if the phrase seems short or informal

2. REFINE_PREVIOUS: User wants to FILTER/NARROW the previous results (add more conditions)
   - Examples: "of those, how many are male", "out of these, how many had serious AE"
   - Action: Keep ALL previous filters AND add new condition
   - IMPORTANT: "out of" does NOT mean a specific adverse event - it's a reference word!

3. NEW_QUERY: A completely new question unrelated to previous context
   - Example: "What's the average age of all patients?"
   - Action: Start fresh, don't carry over filters

IMPORTANT: Short follow-up queries like "list them", "can you list them", "show them" are ALWAYS DETAIL_PREVIOUS when there is previous conversation context!

REFERENCE WORDS TO DETECT (these indicate REFINE_PREVIOUS, not adverse events!):
- "of those", "of these", "out of these", "out of those"
- "among them", "among these", "from those"
- "within that group", "in that population"

Available data:
- ADSL: Subject demographics (USUBJID, AGE, SEX, RACE, SAFFL, ITTFL, TRTSDT, TRTEDT, TRT01A)
- ADAE: Adverse events (USUBJID, AEDECOD, AEBODSYS, AESEV, AESER, AEREL, ATOXGR, AESTDTC, TRTEMFL)
- ADLB: Lab results (USUBJID, PARAMCD, PARAM, AVAL, BASE, CHG, ANRLO, ANRHI, ABLFL)
- ADVS: Vital signs (USUBJID, PARAMCD, PARAM, AVAL, BASE, CHG, ABLFL)

Respond with JSON only (no markdown, no explanation):
{{
    "intent": "DETAIL_PREVIOUS|REFINE_PREVIOUS|COUNT_SUBJECTS|COUNT_EVENTS|LIST_VALUES|LIST_TOP_N|LIST_RECORDS|DISTRIBUTION|COMPARE_GROUPS|AVERAGE|EXISTS|UNCLEAR",
    "references_previous": true or false,
    "preserve_filters": true or false,
    "new_condition_text": "the NEW condition being added (if REFINE_PREVIOUS)" or null,
    "subject": "SUBJECTS|EVENTS|RECORDS|SITES|PARAMETERS",
    "conditions": [
        {{
            "original_text": "the part of the question this refers to",
            "cdisc_concept": "the CDISC standard concept this maps to",
            "column": "the column name (e.g., AEDECOD, AEREL, ATOXGR, AESER)",
            "operator": "=|IN|>|<|>=|<=|LIKE|BETWEEN|IS NOT NULL",
            "values": ["value1", "value2"],
            "confidence": 0.0-1.0
        }}
    ],
    "group_by": ["column1"] or null,
    "limit": number or null,
    "ambiguities": [],
    "understanding_confidence": 0.0-1.0,
    "suggested_table": "ADAE|ADSL|ADLB|ADVS|DM|AE",
    "suggested_population": "Safety|ITT|All Subjects",
    "suggested_clarification": null
}}'''


class QueryAnalyzer:
    """
    Analyzes natural language queries to understand intent and extract conditions.
    Uses Claude for semantic understanding, but structures the output.
    """

    CLARIFICATION_THRESHOLD = 0.7  # Below this, ask for clarification

    def __init__(self, llm_provider=None):
        """
        Initialize query analyzer.

        Args:
            llm_provider: LLM provider for semantic analysis
        """
        self._llm_provider = llm_provider

    def _get_llm_provider(self):
        """Get or create LLM provider."""
        if self._llm_provider is None:
            from .llm_providers import create_llm_provider, LLMConfig
            config = LLMConfig.from_env()
            self._llm_provider = create_llm_provider(config)
        return self._llm_provider

    def analyze(self,
                query: str,
                previous_query: Optional[str] = None,
                previous_sql: Optional[str] = None,
                previous_result: Optional[str] = None,
                conversation_context: Optional[str] = None,
                accumulated_filters: Optional[str] = None
               ) -> QueryAnalysis:
        """
        Analyze query and return structured understanding.

        Args:
            query: Natural language query
            previous_query: Previous question in conversation (for context)
            previous_sql: SQL from previous query (for context)
            previous_result: Result summary from previous query (for context)
            conversation_context: Full conversation context for LLM
            accumulated_filters: SQL filters that must be preserved

        Returns:
            QueryAnalysis with structured understanding
        """
        has_context = bool(conversation_context) or (previous_query and previous_sql)
        logger.info(f"Analyzing query: {query[:100]}... (has_context={has_context})")

        try:
            # Call LLM for semantic analysis
            provider = self._get_llm_provider()

            # Choose prompt based on whether we have conversation context
            if has_context:
                # Build conversation context if not provided
                if not conversation_context:
                    conversation_context = f"Previous question: {previous_query}\nPrevious result: {previous_result}"

                prompt = CONTEXT_AWARE_ANALYSIS_PROMPT.format(
                    query=query,
                    conversation_context=conversation_context or "No previous context",
                    accumulated_filters=accumulated_filters or "No accumulated filters"
                )
            else:
                prompt = QUERY_ANALYSIS_PROMPT.format(query=query)

            from .llm_providers import LLMRequest
            request = LLMRequest(
                prompt=prompt,
                max_tokens=1500,
                temperature=0.0  # Deterministic for analysis
            )

            response = provider.generate(request)

            # Parse JSON response
            analysis_data = self._parse_json_response(response.content)

            # Convert to structured object
            analysis = self._build_analysis(query, analysis_data)

            # Check if clarification needed
            if analysis.understanding_confidence < self.CLARIFICATION_THRESHOLD:
                analysis.needs_clarification = True

            if any(a.clarification_needed for a in analysis.ambiguities):
                analysis.needs_clarification = True

            logger.info(f"Query analysis complete: intent={analysis.intent.value}, "
                       f"references_previous={analysis.references_previous}, "
                       f"preserve_filters={analysis.preserve_filters}, "
                       f"confidence={analysis.understanding_confidence:.2f}")

            return analysis

        except Exception as e:
            logger.error(f"Query analysis failed: {e}")
            # Return a default analysis with low confidence
            return QueryAnalysis(
                original_query=query,
                intent=QueryIntent.UNCLEAR,
                subject=QuerySubject.SUBJECTS,
                understanding_confidence=0.0,
                needs_clarification=True,
                clarification_question="Could you please rephrase your question?"
            )

    def _parse_json_response(self, content: str) -> Dict[str, Any]:
        """Parse JSON from LLM response, handling markdown code blocks."""
        # Remove markdown code blocks if present
        content = content.strip()
        if content.startswith('```'):
            lines = content.split('\n')
            # Remove first and last lines (```json and ```)
            lines = [l for l in lines if not l.strip().startswith('```')]
            content = '\n'.join(lines)

        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            logger.error(f"Content: {content[:500]}")
            # Return default structure
            return {
                "intent": "UNCLEAR",
                "subject": "SUBJECTS",
                "conditions": [],
                "ambiguities": [],
                "understanding_confidence": 0.0
            }

    def _build_analysis(self, query: str, data: Dict[str, Any]) -> QueryAnalysis:
        """Build QueryAnalysis from parsed data."""

        # Parse intent
        intent_str = data.get("intent", "UNCLEAR").upper()
        try:
            intent = QueryIntent(intent_str.lower())
        except ValueError:
            intent = QueryIntent.UNCLEAR

        # Parse subject
        subject_str = data.get("subject", "SUBJECTS").upper()
        try:
            subject = QuerySubject(subject_str.lower())
        except ValueError:
            subject = QuerySubject.SUBJECTS

        # Parse conditions
        conditions = []
        for c in data.get("conditions", []):
            conditions.append(QueryCondition(
                original_text=c.get("original_text", ""),
                cdisc_concept=c.get("cdisc_concept", ""),
                column=c.get("column", ""),
                operator=c.get("operator", "="),
                values=c.get("values", []),
                mapping_confidence=c.get("confidence", 1.0)
            ))

        # Parse ambiguities
        ambiguities = []
        for a in data.get("ambiguities", []):
            ambiguities.append(Ambiguity(
                text=a.get("text", ""),
                interpretations=a.get("interpretations", []),
                clarification_needed=a.get("clarification_needed", False)
            ))

        # Check for follow-up reference
        references_previous = data.get("references_previous", False)
        preserve_filters = data.get("preserve_filters", False)
        new_condition_text = data.get("new_condition_text")

        # Also set references_previous=True if intent is DETAIL_PREVIOUS or REFINE_PREVIOUS
        if intent in [QueryIntent.DETAIL_PREVIOUS, QueryIntent.REFINE_PREVIOUS]:
            references_previous = True
            # REFINE_PREVIOUS should always preserve filters
            if intent == QueryIntent.REFINE_PREVIOUS:
                preserve_filters = True

        return QueryAnalysis(
            original_query=query,
            intent=intent,
            subject=subject,
            conditions=conditions,
            group_by=data.get("group_by"),
            limit=data.get("limit"),
            ambiguities=ambiguities,
            understanding_confidence=data.get("understanding_confidence", 0.5),
            suggested_table=data.get("suggested_table"),
            suggested_population=data.get("suggested_population"),
            clarification_question=data.get("suggested_clarification"),
            references_previous=references_previous,
            preserve_filters=preserve_filters,
            new_condition_text=new_condition_text
        )

    def quick_analyze(self, query: str) -> QueryAnalysis:
        """
        Quick analysis without LLM for simple patterns.
        Falls back to full analysis if pattern not recognized.
        """
        query_lower = query.lower().strip()

        # Count patterns
        count_patterns = [
            (r'how many (patients?|subjects?|people)', QueryIntent.COUNT_SUBJECTS),
            (r'count( of)? (patients?|subjects?)', QueryIntent.COUNT_SUBJECTS),
            (r'number of (patients?|subjects?)', QueryIntent.COUNT_SUBJECTS),
            (r'how many adverse events?', QueryIntent.COUNT_EVENTS),
            (r'how many (aes?|events?)', QueryIntent.COUNT_EVENTS),
        ]

        for pattern, intent in count_patterns:
            if re.search(pattern, query_lower):
                return QueryAnalysis(
                    original_query=query,
                    intent=intent,
                    subject=QuerySubject.SUBJECTS if 'SUBJECTS' in intent.value else QuerySubject.EVENTS,
                    understanding_confidence=0.8,
                    suggested_table=self._suggest_table_from_query(query_lower)
                )

        # List patterns
        list_patterns = [
            (r'^(list|show|display|what are)', QueryIntent.LIST_VALUES),
            (r'top (\d+)', QueryIntent.LIST_TOP_N),
        ]

        for pattern, intent in list_patterns:
            match = re.search(pattern, query_lower)
            if match:
                limit = None
                if intent == QueryIntent.LIST_TOP_N and match.groups():
                    try:
                        limit = int(match.group(1))
                    except (ValueError, IndexError):
                        limit = 10

                return QueryAnalysis(
                    original_query=query,
                    intent=intent,
                    subject=QuerySubject.RECORDS,
                    limit=limit,
                    understanding_confidence=0.75,
                    suggested_table=self._suggest_table_from_query(query_lower)
                )

        # Fall back to full analysis
        return self.analyze(query)

    def _suggest_table_from_query(self, query_lower: str) -> Optional[str]:
        """Suggest table based on query keywords."""
        if any(word in query_lower for word in ['adverse', 'event', 'ae', 'nausea',
                                                  'headache', 'toxicity', 'grade']):
            return 'ADAE'
        if any(word in query_lower for word in ['demographic', 'age', 'sex', 'gender',
                                                  'population', 'treatment']):
            return 'ADSL'
        if any(word in query_lower for word in ['lab', 'hemoglobin', 'creatinine',
                                                  'glucose', 'parameter']):
            return 'ADLB'
        if any(word in query_lower for word in ['vital', 'blood pressure', 'heart rate',
                                                  'temperature', 'weight']):
            return 'ADVS'
        return None


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_intent_description(intent: QueryIntent) -> str:
    """Get human-readable description of intent."""
    descriptions = {
        QueryIntent.COUNT_SUBJECTS: "Count unique patients/subjects",
        QueryIntent.COUNT_EVENTS: "Count adverse events or occurrences",
        QueryIntent.COUNT_RECORDS: "Count individual records",
        QueryIntent.LIST_VALUES: "List distinct values",
        QueryIntent.LIST_TOP_N: "Show top N by frequency",
        QueryIntent.LIST_RECORDS: "List individual records",
        QueryIntent.DETAIL_PREVIOUS: "List details of previous results",
        QueryIntent.REFINE_PREVIOUS: "Filter/narrow previous results",
        QueryIntent.DISTRIBUTION: "Show distribution/breakdown",
        QueryIntent.COMPARE_GROUPS: "Compare between groups",
        QueryIntent.TREND_OVER_TIME: "Show trend over time",
        QueryIntent.EXISTS: "Check if records exist",
        QueryIntent.AVERAGE: "Calculate average/mean",
        QueryIntent.SUM: "Calculate sum/total",
        QueryIntent.MIN_MAX: "Find minimum/maximum",
        QueryIntent.UNCLEAR: "Intent unclear - needs clarification"
    }
    return descriptions.get(intent, "Unknown intent")
