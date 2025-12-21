# Factory 4: Inference Engine

Factory 4 is the runtime query processing engine that translates natural language into SQL results.

---

## Purpose

The Inference Engine processes user queries through a 9-step pipeline:

1. Sanitize input for security
2. Extract clinical entities
3. Resolve tables and populations
4. Build LLM context
5. Generate SQL
6. Validate SQL safety
7. Execute against DuckDB
8. Score confidence
9. Generate human explanation

```mermaid
graph LR
    A[Query] --> B[Sanitize]
    B --> C[Extract]
    C --> D[Resolve]
    D --> E[Context]
    E --> F[Generate]
    F --> G[Validate]
    G --> H[Execute]
    H --> I[Score]
    I --> J[Explain]
    J --> K[Result]
```

---

## Components

| Step | Component | File | Purpose |
|------|-----------|------|---------|
| 1 | InputSanitizer | `input_sanitizer.py` | Security filtering |
| 2 | EntityExtractor | `entity_extractor.py` | Term extraction |
| 3 | TableResolver | `table_resolver.py` | Table/population selection |
| 4 | ContextBuilder | `context_builder.py` | LLM context preparation |
| 5 | SQLGenerator | `sql_generator.py` | SQL generation |
| 6 | SQLValidator | `sql_validator.py` | Safety validation |
| 7 | Executor | `executor.py` | DuckDB execution |
| 8 | ConfidenceScorer | `confidence_scorer.py` | Reliability scoring |
| 9 | ExplanationGenerator | `explanation_generator.py` | Response building |

### Support Components

| Component | File | Purpose |
|-----------|------|---------|
| SessionMemory | `session_memory.py` | Conversation context |
| MedicalSynonyms | `medical_synonyms.py` | Term normalization |
| Pipeline | `pipeline.py` | Orchestration |

---

## The 9-Step Pipeline

### Step 1: Input Sanitization

```python
from core.engine.input_sanitizer import InputSanitizer

sanitizer = InputSanitizer()
result = sanitizer.sanitize("How many subjects had headache?")

if not result.is_safe:
    return f"Blocked: {result.blocked_reason}"
```

**Checks:**
- PHI/PII patterns (SSN, names, emails)
- SQL injection attempts
- Prompt injection attempts

### Step 2: Entity Extraction

```python
from core.engine.entity_extractor import EntityExtractor

extractor = EntityExtractor()
result = extractor.extract("How many subjects had headache?")

# Entities found:
# - "headache" → HEADACHE (AEDECOD, 95% confidence)
```

**Uses:**
- Fuzzy matching (Factory 3)
- MedDRA lookup (Factory 3.5)
- Medical synonyms

### Step 3: Table Resolution

```python
from core.engine.table_resolver import TableResolver

resolver = TableResolver()
resolution = resolver.resolve(entities, query)

# Resolution:
# - Table: ADAE
# - Population: Safety (SAFFL='Y')
# - Grade column: ATOXGR
```

**Clinical Rules:**
- ADaM preferred over SDTM
- Default populations by query type
- Column preference rules

### Step 4: Context Building

```python
from core.engine.context_builder import ContextBuilder

builder = ContextBuilder()
context = builder.build(query, resolution, entities)

# Context includes:
# - System prompt with SQL rules
# - Schema context
# - Entity mappings
# - Clinical rules
```

### Step 5: SQL Generation

```python
from core.engine.sql_generator import SQLGenerator

generator = SQLGenerator()
sql_result = generator.generate(context)

# Generated SQL:
# SELECT COUNT(DISTINCT USUBJID)
# FROM ADAE
# WHERE SAFFL='Y' AND UPPER(AEDECOD)='HEADACHE'
```

**Uses Claude API for generation**

### Step 6: SQL Validation

```python
from core.engine.sql_validator import SQLValidator

validator = SQLValidator()
validation = validator.validate(sql, schema)

if not validation.is_valid:
    return f"Invalid SQL: {validation.errors}"
```

**Checks:**
- SQL syntax
- Table/column existence
- Dangerous operations

### Step 7: Execution

```python
from core.engine.executor import Executor

executor = Executor(db_path)
result = executor.execute(validated_sql)

# Result:
# - success: True
# - data: [{"count": 45}]
# - execution_time_ms: 50
```

### Step 8: Confidence Scoring

```python
from core.engine.confidence_scorer import ConfidenceScorer

scorer = ConfidenceScorer()
confidence = scorer.score(
    entities=entities,
    validation=validation,
    execution=result
)

# Score: 95% (HIGH)
```

**Components:**
- Entity resolution quality (40%)
- Metadata coverage (30%)
- Execution success (20%)
- Result sanity (10%)

### Step 9: Explanation Generation

```python
from core.engine.explanation_generator import ExplanationGenerator

generator = ExplanationGenerator()
answer = generator.generate(query, result, confidence, resolution)

# "45 subjects experienced headache in the safety population."
```

---

## Pipeline Orchestration

```python
from core.engine.pipeline import InferencePipeline

pipeline = InferencePipeline()
result = pipeline.process("How many subjects had headache?")

print(result.answer)      # "45 subjects..."
print(result.confidence)  # {"score": 95, "level": "high"}
print(result.sql)         # "SELECT COUNT..."
```

---

## Configuration

### Environment Variables

```env
# LLM Settings
ANTHROPIC_API_KEY=sk-ant-...
CLAUDE_MODEL=claude-sonnet-4-20250514
CLAUDE_TEMPERATURE=0

# Execution
QUERY_TIMEOUT_SECONDS=30
MAX_RESULT_ROWS=10000

# Confidence
CONFIDENCE_HIGH=90
CONFIDENCE_MEDIUM=70
```

---

## Error Handling

Each step can fail gracefully:

```python
result = pipeline.process("invalid query")

if not result.success:
    print(f"Error at {result.error_stage}: {result.error}")
```

---

## Session Context

Multi-turn conversations maintain context:

```python
pipeline = InferencePipeline()

# First query
r1 = pipeline.process("How many subjects had headache?", session_id="conv1")
# 45 subjects

# Follow-up
r2 = pipeline.process("Of those, how many were Grade 3+?", session_id="conv1")
# 3 subjects (uses headache filter from previous query)
```

---

## Next Steps

- [Pipeline Steps Detail](pipeline-steps.md)
- [Input Sanitizer](input-sanitizer.md)
- [Entity Extractor](entity-extractor.md)
- [SQL Generator](sql-generator.md)
- [Confidence Scorer](confidence-scorer.md)
