# Factory 4: Accuracy & Consistency Architecture

## The Real Problems

### 1. Answer Accuracy Issues
- SQL might be syntactically correct but **semantically wrong**
- No verification that the generated SQL actually answers the question
- Entity resolution might match wrong terms (e.g., "headache" → "sinus headache")
- No feedback loop to learn from mistakes

### 2. Query Coverage Limitations
- System only handles queries it was "designed" for
- No graceful handling of questions it can't answer
- No clarification requests when ambiguous
- No understanding of query variations

### 3. Response Inconsistency
- Confidence shown as: 95%, 0.95, "high", "5/5 stars" - all over the place
- Different response structures from different code paths
- No unified response contract

### 4. No Memory/Learning
- Same question asked differently = different answers
- No learning from corrections
- No session context (multi-turn conversations)

---

## Architecture: Query Understanding Layer

### Core Principle: Understand Before Acting

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    QUERY UNDERSTANDING PIPELINE                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  User Query: "How many patients got sick from the drug?"                │
│       │                                                                  │
│       ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ 1. QUERY ANALYSIS                                                │    │
│  │    - Intent: COUNT query                                         │    │
│  │    - Subject: patients/subjects                                  │    │
│  │    - Condition: "sick from drug" → adverse events, drug-related │    │
│  │    - Ambiguity: "sick" is vague - need clarification?           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│       │                                                                  │
│       ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ 2. SEMANTIC MAPPING                                              │    │
│  │    - "sick from drug" → CDISC concept: Treatment-related AE     │    │
│  │    - Maps to: ADAE where AEREL contains 'DRUG' or 'POSSIBLE'    │    │
│  │    - Population: Safety (has exposure to drug)                   │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│       │                                                                  │
│       ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ 3. QUERY PLAN GENERATION                                         │    │
│  │    - Target: COUNT(DISTINCT USUBJID)                             │    │
│  │    - From: ADAE                                                   │    │
│  │    - Where: SAFFL='Y' AND AEREL IN ('DRUG','POSSIBLE','PROBABLE')│    │
│  │    - Verified against schema: ✓                                  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│       │                                                                  │
│       ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ 4. ANSWER VERIFICATION                                           │    │
│  │    - SQL executes successfully: ✓                                │    │
│  │    - Result is plausible (< total subjects): ✓                   │    │
│  │    - Cross-check: Subjects in result exist in ADSL: ✓           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Query Understanding Module

### 1.1 Structured Query Analysis

Instead of going directly from text to SQL, decompose the query first:

```python
@dataclass
class QueryAnalysis:
    """Structured understanding of user's question."""

    # What type of question
    intent: QueryIntent  # COUNT, LIST, COMPARE, TREND, DISTRIBUTION, etc.

    # What are we counting/listing
    subject: QuerySubject  # SUBJECTS, EVENTS, RECORDS, SITES, etc.

    # Conditions/filters
    conditions: List[QueryCondition]

    # Time constraints
    time_frame: Optional[TimeFrame]

    # Grouping
    group_by: Optional[List[str]]

    # Sorting/limiting
    order_by: Optional[str]
    limit: Optional[int]

    # Ambiguities detected
    ambiguities: List[Ambiguity]

    # Confidence in understanding
    understanding_confidence: float  # 0-1


@dataclass
class QueryCondition:
    """A single condition in the query."""

    # Original text
    original_text: str  # "sick from the drug"

    # Mapped CDISC concept
    cdisc_concept: str  # "Treatment-related Adverse Event"

    # Column mapping
    column: str  # "AEREL"
    operator: str  # "IN"
    values: List[str]  # ["DRUG", "POSSIBLE", "PROBABLE"]

    # Mapping confidence
    mapping_confidence: float

    # Alternative interpretations
    alternatives: List[AlternativeMapping]


class QueryIntent(Enum):
    """Types of questions the system can answer."""

    # Counting questions
    COUNT_SUBJECTS = "count_subjects"  # How many patients...
    COUNT_EVENTS = "count_events"      # How many adverse events...
    COUNT_RECORDS = "count_records"    # How many lab values...

    # Listing questions
    LIST_VALUES = "list_values"        # What are the adverse events...
    LIST_TOP_N = "list_top_n"          # Top 5 most common...

    # Distribution questions
    DISTRIBUTION = "distribution"      # Age distribution, gender breakdown

    # Comparison questions
    COMPARE_GROUPS = "compare_groups"  # Compare treatment vs placebo

    # Trend questions
    TREND_OVER_TIME = "trend"          # How did X change over time

    # Existence questions
    EXISTS = "exists"                  # Are there any patients with...

    # Unknown/unclear
    UNCLEAR = "unclear"                # Need clarification
```

### 1.2 Query Analyzer Implementation

```python
class QueryAnalyzer:
    """
    Analyzes natural language queries to understand intent and extract conditions.
    Uses Claude for semantic understanding, but structures the output.
    """

    ANALYSIS_PROMPT = '''Analyze this clinical data question and extract structured information.

Question: {query}

Available data:
- ADSL: Subject demographics (AGE, SEX, RACE, SAFFL, ITTFL, etc.)
- ADAE: Adverse events (AEDECOD, AESEV, AESER, AEREL, ATOXGR, etc.)
- ADLB: Lab results (PARAMCD, AVAL, ANRLO, ANRHI, etc.)
- ADVS: Vital signs (PARAMCD, AVAL, etc.)

Respond with JSON only:
{{
    "intent": "COUNT_SUBJECTS|COUNT_EVENTS|LIST_VALUES|LIST_TOP_N|DISTRIBUTION|COMPARE_GROUPS|UNCLEAR",
    "subject": "SUBJECTS|EVENTS|RECORDS|SITES",
    "conditions": [
        {{
            "original_text": "the part of the question this refers to",
            "cdisc_concept": "the CDISC standard concept this maps to",
            "column": "the column name",
            "operator": "=|IN|>|<|>=|<=|LIKE|BETWEEN",
            "values": ["value1", "value2"],
            "confidence": 0.0-1.0
        }}
    ],
    "group_by": ["column1", "column2"] or null,
    "ambiguities": [
        {{
            "text": "ambiguous part",
            "interpretations": ["interpretation1", "interpretation2"],
            "clarification_needed": true/false
        }}
    ],
    "understanding_confidence": 0.0-1.0,
    "suggested_clarification": "question to ask user if unclear" or null
}}'''

    def analyze(self, query: str, context: RequestContext) -> QueryAnalysis:
        """
        Analyze query and return structured understanding.
        """
        # Call Claude for semantic analysis
        response = self.llm.generate(
            prompt=self.ANALYSIS_PROMPT.format(query=query),
            max_tokens=1000,
            temperature=0.0  # Deterministic
        )

        # Parse JSON response
        analysis_data = json.loads(response.content)

        # Convert to structured object
        analysis = QueryAnalysis(
            intent=QueryIntent(analysis_data["intent"]),
            subject=QuerySubject(analysis_data["subject"]),
            conditions=[
                QueryCondition(**c) for c in analysis_data["conditions"]
            ],
            ambiguities=[
                Ambiguity(**a) for a in analysis_data.get("ambiguities", [])
            ],
            understanding_confidence=analysis_data["understanding_confidence"]
        )

        # Check if clarification needed
        if analysis.understanding_confidence < 0.7 or analysis.ambiguities:
            analysis.needs_clarification = True
            analysis.clarification_question = analysis_data.get("suggested_clarification")

        return analysis
```

---

## 2. Clarification System

### When understanding is low, ASK - don't guess

```python
class ClarificationManager:
    """
    Manages clarification dialogs when queries are ambiguous.
    """

    CLARIFICATION_THRESHOLD = 0.7  # Below this, ask for clarification

    def needs_clarification(self, analysis: QueryAnalysis) -> bool:
        """Check if clarification is needed."""
        return (
            analysis.understanding_confidence < self.CLARIFICATION_THRESHOLD or
            any(a.clarification_needed for a in analysis.ambiguities)
        )

    def generate_clarification_request(
        self,
        analysis: QueryAnalysis
    ) -> ClarificationRequest:
        """
        Generate a clarification request for the user.
        """
        options = []

        # If ambiguous conditions, offer choices
        for ambiguity in analysis.ambiguities:
            if ambiguity.clarification_needed:
                options.append(ClarificationOption(
                    question=f"When you say '{ambiguity.text}', do you mean:",
                    choices=ambiguity.interpretations
                ))

        # If intent unclear, offer common interpretations
        if analysis.intent == QueryIntent.UNCLEAR:
            options.append(ClarificationOption(
                question="I want to make sure I understand. Are you asking:",
                choices=[
                    "How many patients experienced this?",
                    "Which patients experienced this?",
                    "A list of all occurrences?",
                    "Something else (please specify)"
                ]
            ))

        return ClarificationRequest(
            original_query=analysis.original_query,
            options=options,
            suggested_rephrasing=self._suggest_rephrasing(analysis)
        )


# Response when clarification needed
{
    "type": "clarification_needed",
    "message": "I want to make sure I give you the right answer.",
    "options": [
        {
            "question": "When you say 'sick from the drug', do you mean:",
            "choices": [
                {"id": 1, "text": "Any adverse event (whether drug-related or not)"},
                {"id": 2, "text": "Only drug-related adverse events"},
                {"id": 3, "text": "Serious adverse events only"}
            ]
        }
    ],
    "suggested_rephrasing": "Try: 'How many patients had drug-related adverse events?'"
}
```

---

## 3. Answer Verification Layer

### Don't just execute SQL - verify the answer makes sense

```python
class AnswerVerifier:
    """
    Verifies that generated answers are accurate and sensible.
    """

    def verify(
        self,
        query: str,
        analysis: QueryAnalysis,
        sql: str,
        result: ExecutionResult,
        context: RequestContext
    ) -> VerificationResult:
        """
        Multi-layer verification of the answer.
        """
        checks = []

        # 1. SQL matches intent
        checks.append(self._verify_sql_matches_intent(analysis, sql))

        # 2. Result is plausible
        checks.append(self._verify_result_plausible(analysis, result))

        # 3. Cross-reference check
        checks.append(self._verify_cross_reference(analysis, result))

        # 4. Sanity bounds
        checks.append(self._verify_sanity_bounds(analysis, result))

        # Calculate overall verification score
        verification_score = sum(c.score * c.weight for c in checks)

        return VerificationResult(
            checks=checks,
            overall_score=verification_score,
            passed=verification_score >= 0.8,
            issues=[c.issue for c in checks if not c.passed]
        )

    def _verify_sql_matches_intent(
        self,
        analysis: QueryAnalysis,
        sql: str
    ) -> VerificationCheck:
        """
        Verify SQL structure matches query intent.
        """
        sql_upper = sql.upper()

        # COUNT intent should have COUNT() in SQL
        if analysis.intent in [QueryIntent.COUNT_SUBJECTS, QueryIntent.COUNT_EVENTS]:
            if 'COUNT(' not in sql_upper:
                return VerificationCheck(
                    name="intent_match",
                    passed=False,
                    score=0.0,
                    weight=0.3,
                    issue="Query asks for count but SQL doesn't use COUNT()"
                )

            # COUNT_SUBJECTS should use COUNT(DISTINCT USUBJID)
            if analysis.intent == QueryIntent.COUNT_SUBJECTS:
                if 'DISTINCT' not in sql_upper or 'USUBJID' not in sql_upper:
                    return VerificationCheck(
                        name="intent_match",
                        passed=False,
                        score=0.5,
                        weight=0.3,
                        issue="Counting subjects should use COUNT(DISTINCT USUBJID)"
                    )

        # LIST intent should not have COUNT
        if analysis.intent in [QueryIntent.LIST_VALUES, QueryIntent.LIST_TOP_N]:
            if 'COUNT(' in sql_upper and 'SELECT' in sql_upper:
                # Make sure COUNT isn't the main select
                pass  # Could be GROUP BY with COUNT

        return VerificationCheck(
            name="intent_match",
            passed=True,
            score=1.0,
            weight=0.3
        )

    def _verify_result_plausible(
        self,
        analysis: QueryAnalysis,
        result: ExecutionResult
    ) -> VerificationCheck:
        """
        Verify result values are plausible.
        """
        if analysis.intent == QueryIntent.COUNT_SUBJECTS:
            # Count should not exceed total subjects
            count = self._extract_count(result)
            total_subjects = self._get_total_subjects()

            if count > total_subjects:
                return VerificationCheck(
                    name="plausibility",
                    passed=False,
                    score=0.0,
                    weight=0.25,
                    issue=f"Result ({count}) exceeds total subjects ({total_subjects})"
                )

            # Count should not be negative
            if count < 0:
                return VerificationCheck(
                    name="plausibility",
                    passed=False,
                    score=0.0,
                    weight=0.25,
                    issue=f"Negative count ({count}) is impossible"
                )

        return VerificationCheck(
            name="plausibility",
            passed=True,
            score=1.0,
            weight=0.25
        )

    def _verify_cross_reference(
        self,
        analysis: QueryAnalysis,
        result: ExecutionResult
    ) -> VerificationCheck:
        """
        Cross-reference check: verify subjects exist in reference tables.
        """
        if analysis.intent == QueryIntent.COUNT_SUBJECTS:
            # Get subjects from result
            # Verify they exist in ADSL
            pass

        return VerificationCheck(
            name="cross_reference",
            passed=True,
            score=1.0,
            weight=0.2
        )
```

---

## 4. Unified Response Format

### Single response structure - always consistent

```python
@dataclass
class SAGEResponse:
    """
    Unified response format for ALL SAGE responses.
    This is the ONLY response format used anywhere in the system.
    """

    # === Always Present ===

    # Unique response ID
    response_id: str

    # Original query
    query: str

    # Response type
    type: ResponseType  # ANSWER, CLARIFICATION, ERROR, GREETING, HELP

    # Timestamp
    timestamp: str

    # === Answer Fields (when type=ANSWER) ===

    # The answer in natural language
    answer: Optional[str] = None

    # Structured data (if applicable)
    data: Optional[List[Dict]] = None

    # === Confidence (always present for ANSWER type) ===

    # Single confidence score: 0-100 integer
    confidence_score: int = 0  # ALWAYS 0-100, no decimals, no stars

    # Confidence level: high/medium/low/very_low
    confidence_level: str = "unknown"

    # Confidence explanation
    confidence_explanation: str = ""

    # === Methodology (for transparency) ===

    methodology: Optional[MethodologyInfo] = None

    # === Clarification (when type=CLARIFICATION) ===

    clarification: Optional[ClarificationRequest] = None

    # === Error (when type=ERROR) ===

    error: Optional[ErrorInfo] = None

    # === Metadata ===

    execution_time_ms: float = 0
    cached: bool = False


class ResponseType(Enum):
    ANSWER = "answer"              # Successful data query response
    CLARIFICATION = "clarification"  # Need more info from user
    ERROR = "error"                # Something went wrong
    GREETING = "greeting"          # Hi, hello responses
    HELP = "help"                  # Help/capabilities responses
    CONVERSATION = "conversation"  # General conversational response


@dataclass
class MethodologyInfo:
    """How the answer was derived - for transparency."""

    # Data source used
    data_source: str  # "Adverse Events Analysis Dataset"

    # Population
    population: str  # "Safety Population"

    # Key filters applied (in plain English)
    filters_applied: List[str]  # ["Only drug-related events", "Grade 2 or higher"]

    # SQL (hidden by default, available on request)
    sql: str

    # Entities resolved
    entities: List[EntityResolution]


# Response example - ALWAYS this format
{
    "response_id": "resp_abc123",
    "query": "How many patients had drug-related adverse events?",
    "type": "answer",
    "timestamp": "2025-12-15T20:00:00Z",

    "answer": "**127 subjects** had drug-related adverse events in the Safety Population.",

    "data": [{"count": 127}],

    "confidence_score": 94,  # ALWAYS integer 0-100
    "confidence_level": "high",
    "confidence_explanation": "High confidence - exact match on drug-related adverse events, verified against ADAE.",

    "methodology": {
        "data_source": "Adverse Events Analysis Dataset",
        "population": "Safety Population (232 subjects)",
        "filters_applied": [
            "Only subjects in Safety Population",
            "Only events related to study drug"
        ],
        "sql": "SELECT COUNT(DISTINCT USUBJID) FROM ADAE WHERE SAFFL='Y' AND AEREL IN ('DRUG','PROBABLE','POSSIBLE')"
    },

    "execution_time_ms": 3456,
    "cached": false
}
```

---

## 5. Confidence Score Standardization

### One format. Always. Everywhere.

```python
class ConfidenceCalculator:
    """
    Calculates confidence score in a SINGLE, CONSISTENT format.

    Output: Integer 0-100 (NEVER decimals, NEVER stars, NEVER ratios)
    """

    def calculate(
        self,
        analysis: QueryAnalysis,
        verification: VerificationResult,
        execution: ExecutionResult
    ) -> ConfidenceScore:
        """
        Calculate unified confidence score.

        Components:
        1. Understanding confidence (30%) - How well we understood the query
        2. Mapping confidence (25%) - How well terms mapped to data
        3. Verification score (25%) - Did answer pass verification
        4. Execution quality (20%) - Did SQL execute cleanly
        """

        # 1. Understanding (30%)
        understanding = analysis.understanding_confidence * 100

        # 2. Mapping (25%)
        if analysis.conditions:
            mapping = sum(c.mapping_confidence for c in analysis.conditions) / len(analysis.conditions) * 100
        else:
            mapping = 100  # No conditions to map

        # 3. Verification (25%)
        verification_score = verification.overall_score * 100

        # 4. Execution (20%)
        execution_score = 100 if execution.success else 0
        if execution.success and execution.execution_time_ms > 10000:
            execution_score -= 10  # Slow query penalty

        # Weighted total
        total = (
            understanding * 0.30 +
            mapping * 0.25 +
            verification_score * 0.25 +
            execution_score * 0.20
        )

        # Round to integer
        score = int(round(total))

        # Determine level
        if score >= 90:
            level = "high"
        elif score >= 70:
            level = "medium"
        elif score >= 50:
            level = "low"
        else:
            level = "very_low"

        # Generate explanation
        explanation = self._generate_explanation(
            score, level, understanding, mapping, verification_score, execution_score
        )

        return ConfidenceScore(
            score=score,  # INTEGER 0-100
            level=level,
            explanation=explanation,
            components={
                "understanding": int(understanding),
                "mapping": int(mapping),
                "verification": int(verification_score),
                "execution": int(execution_score)
            }
        )
```

---

## 6. Data-Driven Knowledge

### Learn from the data, don't hardcode

```python
class DataDrivenKnowledge:
    """
    Build knowledge from actual data, not hardcoded rules.
    """

    def learn_from_data(self, db_path: str) -> DataKnowledge:
        """
        Analyze database to learn:
        - What columns exist and their distributions
        - What values are common
        - Relationships between tables
        """
        knowledge = DataKnowledge()

        # Learn column value distributions
        for table in self._get_tables():
            for column in self._get_columns(table):
                # Get distinct values and frequencies
                values = self._get_value_distribution(table, column)
                knowledge.add_column_knowledge(table, column, values)

        # Learn common patterns
        # e.g., "Grade 3" appears in ATOXGR column
        # e.g., "Nausea" appears in AEDECOD column
        knowledge.learn_patterns()

        return knowledge

    def suggest_corrections(self, term: str) -> List[Suggestion]:
        """
        Suggest corrections based on actual data.

        If user says "headach" -> suggest "Headache" from AEDECOD values
        If user says "grade 4" -> suggest "Grade 4" or "4" based on ATOXGR values
        """
        suggestions = []

        for table, column, values in self.knowledge.get_similar_values(term):
            for value, similarity in values:
                if similarity > 0.8:
                    suggestions.append(Suggestion(
                        original=term,
                        suggested=value,
                        source_column=f"{table}.{column}",
                        confidence=similarity
                    ))

        return sorted(suggestions, key=lambda s: s.confidence, reverse=True)
```

---

## 7. Query Templates (Data-Driven)

### Templates generated from data structure, not hardcoded

```python
class QueryTemplateGenerator:
    """
    Generate SQL templates based on actual data structure.
    """

    def generate_templates(self, knowledge: DataKnowledge) -> Dict[str, QueryTemplate]:
        """
        Generate templates for common query patterns.
        """
        templates = {}

        # Count subjects with condition
        templates["count_subjects_with_condition"] = QueryTemplate(
            name="Count subjects with condition",
            description="Count unique subjects matching a condition",
            sql_template="""
                SELECT COUNT(DISTINCT USUBJID) as subject_count
                FROM {table}
                WHERE {population_filter}
                AND {condition}
            """,
            parameters={
                "table": "Table to query (ADAE, ADSL, etc.)",
                "population_filter": "Population filter (SAFFL='Y', etc.)",
                "condition": "The condition to filter on"
            },
            example_queries=[
                "How many patients had nausea?",
                "How many subjects experienced headaches?",
                "Count of patients with serious adverse events"
            ]
        )

        # Top N by frequency
        templates["top_n_by_frequency"] = QueryTemplate(
            name="Top N by frequency",
            sql_template="""
                SELECT {group_column}, COUNT(DISTINCT USUBJID) as subject_count
                FROM {table}
                WHERE {population_filter}
                GROUP BY {group_column}
                ORDER BY subject_count DESC
                LIMIT {limit}
            """,
            parameters={
                "table": "Table to query",
                "group_column": "Column to group by",
                "population_filter": "Population filter",
                "limit": "Number of results"
            }
        )

        return templates

    def match_query_to_template(
        self,
        analysis: QueryAnalysis
    ) -> Tuple[QueryTemplate, Dict[str, str]]:
        """
        Match analyzed query to a template and extract parameters.
        """
        if analysis.intent == QueryIntent.COUNT_SUBJECTS:
            template = self.templates["count_subjects_with_condition"]
            params = {
                "table": self._determine_table(analysis),
                "population_filter": self._get_population_filter(analysis),
                "condition": self._build_condition(analysis.conditions)
            }
            return template, params

        # ... more template matching
```

---

## 8. Session Memory

### Remember context across conversation

```python
class SessionMemory:
    """
    Maintains conversation context for better understanding.
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.history: List[ConversationTurn] = []
        self.context: Dict[str, Any] = {}
        self.corrections: List[Correction] = []

    def add_turn(self, query: str, response: SAGEResponse):
        """Add a conversation turn."""
        self.history.append(ConversationTurn(
            query=query,
            response=response,
            timestamp=datetime.now()
        ))

        # Extract context from this turn
        self._update_context(query, response)

    def _update_context(self, query: str, response: SAGEResponse):
        """Extract and store context from conversation."""
        if response.methodology:
            # Remember which table was used
            self.context["last_table"] = response.methodology.data_source

            # Remember which population
            self.context["last_population"] = response.methodology.population

            # Remember entities mentioned
            if response.methodology.entities:
                self.context["mentioned_entities"] = [
                    e.resolved for e in response.methodology.entities
                ]

    def resolve_references(self, query: str) -> str:
        """
        Resolve references to previous context.

        "How many of those were serious?"
        -> "How many of the subjects with nausea had serious adverse events?"
        """
        # "those" / "them" / "these" -> last subject set
        if any(ref in query.lower() for ref in ["those", "them", "these", "they"]):
            if "mentioned_entities" in self.context:
                # Expand the reference
                entities = self.context["mentioned_entities"]
                query = query.replace("those", f"subjects with {entities[0]}")

        return query

    def learn_correction(self, original: str, corrected: str):
        """
        Learn from user corrections.

        If user says "no, I meant serious events not all events",
        store this preference.
        """
        self.corrections.append(Correction(
            original=original,
            corrected=corrected,
            timestamp=datetime.now()
        ))

        # Update context with learned preference
        # Could be used for future queries in this session
```

---

## 9. Implementation Priority

### Phase 1: Foundation (Week 1-2)
1. **Unified Response Format** - Single response structure everywhere
2. **Confidence Standardization** - Integer 0-100, no variations
3. **Query Analysis Structure** - Decompose queries before SQL

### Phase 2: Accuracy (Week 3-4)
4. **Answer Verification** - Verify before returning
5. **Clarification System** - Ask when uncertain
6. **Data-Driven Knowledge** - Learn from actual data

### Phase 3: Intelligence (Week 5-6)
7. **Session Memory** - Remember conversation context
8. **Template Generation** - Data-driven query templates
9. **Learning from Corrections** - Improve over time

---

## 10. Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Answer Accuracy | Unknown (~70%?) | > 95% |
| Query Coverage | Limited patterns | Any clinical question |
| Response Consistency | Multiple formats | 100% unified |
| Clarification Rate | 0% (guesses) | 10-15% (asks when unsure) |
| False Confidence | Common | Rare (<5%) |

---

## Key Principles

1. **Understand before acting** - Parse query structure before generating SQL
2. **Ask when unsure** - Clarification > wrong answer
3. **Verify before responding** - Cross-check results
4. **One format always** - Consistent response structure
5. **Learn from data** - Don't hardcode, discover
6. **Remember context** - Use conversation history
7. **Be transparent** - Show how answer was derived
