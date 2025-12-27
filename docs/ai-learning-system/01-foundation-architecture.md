# AI Chat Learning System - Phase 1: Foundation & Architecture

## Executive Summary

This document outlines a comprehensive learning system to improve SAGE's AI chat accuracy from 60-70% to 85-95%. The system uses semantic similarity, few-shot learning, multi-layer validation, and continuous feedback to create a self-improving clinical AI platform.

### Key Goals

| Goal | Target | Approach |
|------|--------|----------|
| Accuracy Improvement | 85-95% | Few-shot learning + semantic matching |
| Handling Variations | 1000s of phrasings | Vector embeddings (store ~70 examples) |
| Continuous Learning | Self-improving | Feedback loop + correction database |
| Clinical Safety | Confidence-based | Multi-layer validation + action thresholds |
| Regulatory Compliance | 21 CFR Part 11 | Full audit trail |

---

## Current Architecture Overview

### Existing 9-Step RAG Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CURRENT PIPELINE (pipeline.py)                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. INPUT SANITIZATION                                                   │
│     └── Block PHI/PII, SQL injection, prompt injection                   │
│                                                                          │
│  2. ENTITY EXTRACTION                                                    │
│     └── Extract tables, columns, values from query                       │
│                                                                          │
│  3. QUERY ANALYSIS                                                       │
│     └── Classify intent: DATA, DOCUMENT, HYBRID, CONVERSATIONAL          │
│                                                                          │
│  4. FUZZY MATCHING                                                       │
│     └── Correct typos (Tyleonl → TYLENOL), resolve synonyms              │
│                                                                          │
│  5. METADATA RETRIEVAL                                                   │
│     └── Load Golden Metadata for referenced variables                    │
│                                                                          │
│  6. CONTEXT BUILDING                                                     │
│     └── Build LLM prompt with schema, metadata, examples                 │
│                                                                          │
│  7. SQL GENERATION                                                       │
│     └── LLM generates DuckDB SQL                                         │
│                                                                          │
│  8. CODE VALIDATION                                                      │
│     └── Parse SQL, verify columns, block dangerous operations            │
│                                                                          │
│  9. EXECUTION & RESPONSE                                                 │
│     └── Execute SQL, score confidence, generate explanation              │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Current Files & Their Roles

| File | Purpose | Lines |
|------|---------|-------|
| `core/engine/pipeline.py` | Main orchestrator, SQL generation | ~800 |
| `core/engine/query_analyzer.py` | Intent classification | ~200 |
| `core/engine/confidence_scorer.py` | 4-component weighted scoring | ~150 |
| `core/engine/session_memory.py` | Conversation context | ~100 |
| `core/engine/answer_verifier.py` | Result verification | ~250 |
| `core/engine/context_builder.py` | LLM prompt preparation | ~300 |
| `docker/api/routers/chat.py` | Chat API endpoints | ~200 |

### Current Confidence Scoring

```python
# Current 4-component scoring (confidence_scorer.py)
components = {
    "dictionary_match": 0.40,   # Fuzzy match quality
    "metadata_coverage": 0.30,  # Golden metadata found
    "execution_success": 0.20,  # Query ran without error
    "result_sanity": 0.10       # Basic result checks
}
```

### Current Limitations

| Issue | Impact | Root Cause |
|-------|--------|------------|
| No learning from corrections | Same mistakes repeat | No feedback storage |
| Limited examples for LLM | SQL hallucinations | Static prompts |
| Weak semantic validation | Wrong SQL accepted | No intent matching |
| No query complexity assessment | Same confidence for all | Missing complexity scoring |
| No historical comparison | Anomalies not detected | No result history |

---

## Enhanced Architecture

### New 11-Step Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ENHANCED PIPELINE                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. INPUT SANITIZATION (existing)                                        │
│     └── Block PHI/PII, SQL injection, prompt injection                   │
│                                                                          │
│  2. ENTITY EXTRACTION (existing)                                         │
│     └── Extract tables, columns, values from query                       │
│                                                                          │
│  3. QUERY ANALYSIS (existing)                                            │
│     └── Classify intent: DATA, DOCUMENT, HYBRID, CONVERSATIONAL          │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ 4. COMPLEXITY ASSESSMENT (NEW)                                    │   │
│  │    └── Score: SIMPLE, MODERATE, COMPLEX, VERY_COMPLEX             │   │
│  │    └── Adjust confidence thresholds based on complexity           │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ 5. EXAMPLE RETRIEVAL (NEW)                                        │   │
│  │    └── Find semantically similar past queries from learning store │   │
│  │    └── Retrieve verified SQL examples for few-shot learning       │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  6. FUZZY MATCHING (existing)                                            │
│     └── Correct typos, resolve synonyms                                  │
│                                                                          │
│  7. METADATA RETRIEVAL (existing)                                        │
│     └── Load Golden Metadata for referenced variables                    │
│                                                                          │
│  8. CONTEXT BUILDING (ENHANCED)                                          │
│     └── Include retrieved examples in prompt                             │
│     └── Add complexity-aware instructions                                │
│                                                                          │
│  9. SQL GENERATION (existing)                                            │
│     └── LLM generates DuckDB SQL with few-shot examples                  │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ 10. SEMANTIC VALIDATION (NEW)                                     │   │
│  │     └── Verify SQL matches understood intent                      │   │
│  │     └── Check required columns, grouping, filters                 │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  11. CODE VALIDATION (existing)                                          │
│      └── Parse SQL, verify columns, block dangerous operations           │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ 12. RESULT VALIDATION (NEW)                                       │   │
│  │     └── Compare against historical results                        │   │
│  │     └── Check bounds, percentages, null ratios                    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ 13. CONFIDENCE CALCULATION (ENHANCED)                             │   │
│  │     └── 8-component scoring with learning signals                 │   │
│  │     └── Determine response action based on confidence             │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  14. EXECUTION & RESPONSE (existing)                                     │
│      └── Execute SQL, generate explanation                               │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ 15. FEEDBACK COLLECTION (NEW)                                     │   │
│  │     └── User provides feedback: Correct, Incorrect, Corrected     │   │
│  │     └── Store corrections for future learning                     │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## New Components Overview

### Directory Structure

```
core/engine/learning/
├── __init__.py
├── example_store.py      # Vector store for query-SQL pairs
├── example_retriever.py  # Semantic similarity search
├── complexity_scorer.py  # Query complexity assessment
├── semantic_validator.py # SQL-intent alignment check
├── result_validator.py   # Historical comparison & sanity checks
├── confidence_manager.py # Enhanced confidence calculation
├── feedback_handler.py   # Process user feedback
└── training_manager.py   # Admin training interface
```

### Component Responsibilities

| Component | Input | Output | Purpose |
|-----------|-------|--------|---------|
| `ExampleStore` | Query + SQL | Stored vector | ChromaDB integration |
| `ExampleRetriever` | User query | Similar examples | Few-shot retrieval |
| `ComplexityScorer` | Query text | Complexity level | Adjust thresholds |
| `SemanticValidator` | SQL + Intent | Validation result | Intent alignment |
| `ResultValidator` | Query result | Anomaly flags | Historical comparison |
| `ConfidenceManager` | All signals | Final score + action | Response decision |
| `FeedbackHandler` | User feedback | Updated store | Learning loop |
| `TrainingManager` | Admin input | Training session | Pre-release training |

---

## Database Schema

### New Tables (in `data/learning.db`)

```sql
-- Core learning examples table
CREATE TABLE learning_examples (
    id TEXT PRIMARY KEY,
    question TEXT NOT NULL,
    normalized_question TEXT,
    sql TEXT NOT NULL,
    intent TEXT,
    tables_used TEXT,      -- JSON array
    columns_used TEXT,     -- JSON array
    complexity TEXT,       -- SIMPLE, MODERATE, COMPLEX, VERY_COMPLEX
    category TEXT,         -- patient_count, adverse_events, demographics, etc.
    source TEXT NOT NULL,  -- manual, feedback, training
    verified BOOLEAN DEFAULT FALSE,
    created_by TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usage_count INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    last_used_at TIMESTAMP,
    status TEXT DEFAULT 'active'
);

-- Vector embeddings (managed by ChromaDB, reference here)
CREATE TABLE example_embeddings (
    example_id TEXT PRIMARY KEY REFERENCES learning_examples(id),
    collection_name TEXT DEFAULT 'query_examples',
    embedding_model TEXT DEFAULT 'nomic-embed-text',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User feedback on responses
CREATE TABLE query_feedback (
    id TEXT PRIMARY KEY,
    query_id TEXT NOT NULL,
    question TEXT NOT NULL,
    generated_sql TEXT,
    feedback_type TEXT NOT NULL,  -- CORRECT, INCORRECT, CORRECTED
    corrected_sql TEXT,
    user_id TEXT NOT NULL,
    session_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP
);

-- Training sessions
CREATE TABLE training_sessions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_by TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status TEXT DEFAULT 'active',  -- active, completed, cancelled
    examples_added INTEGER DEFAULT 0,
    coverage_before REAL,
    coverage_after REAL
);

-- Training session examples
CREATE TABLE training_session_examples (
    id TEXT PRIMARY KEY,
    session_id TEXT REFERENCES training_sessions(id),
    example_id TEXT REFERENCES learning_examples(id),
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Historical results for comparison
CREATE TABLE historical_results (
    id TEXT PRIMARY KEY,
    query_hash TEXT NOT NULL,
    question TEXT NOT NULL,
    sql TEXT NOT NULL,
    result_hash TEXT,
    result_summary TEXT,  -- JSON: row_count, columns, sample_values
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_version TEXT     -- Track when underlying data changed
);

-- Review queue for uncertain queries
CREATE TABLE review_queue (
    id TEXT PRIMARY KEY,
    query_id TEXT NOT NULL,
    question TEXT NOT NULL,
    generated_sql TEXT,
    confidence_score REAL,
    reason TEXT,
    status TEXT DEFAULT 'pending',  -- pending, approved, rejected, corrected
    reviewed_by TEXT,
    reviewed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Indexes

```sql
CREATE INDEX idx_learning_examples_status ON learning_examples(status);
CREATE INDEX idx_learning_examples_category ON learning_examples(category);
CREATE INDEX idx_learning_examples_verified ON learning_examples(verified);
CREATE INDEX idx_query_feedback_processed ON query_feedback(processed);
CREATE INDEX idx_historical_results_hash ON historical_results(query_hash);
CREATE INDEX idx_review_queue_status ON review_queue(status);
```

---

## API Endpoints

### Feedback Endpoints (`/api/v1/feedback`)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/v1/feedback` | Submit feedback for a query |
| GET | `/api/v1/feedback/pending` | Get pending feedback to process |
| POST | `/api/v1/feedback/{id}/process` | Process feedback into learning |

### Training Endpoints (`/api/v1/training`)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/v1/training/sessions` | Start new training session |
| GET | `/api/v1/training/sessions` | List training sessions |
| GET | `/api/v1/training/sessions/{id}` | Get session details |
| POST | `/api/v1/training/sessions/{id}/examples` | Add example to session |
| POST | `/api/v1/training/sessions/{id}/complete` | Complete session |
| GET | `/api/v1/training/coverage` | Get coverage report |
| GET | `/api/v1/training/suggested` | Get suggested training queries |

### Learning Endpoints (`/api/v1/learning`)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/v1/learning/examples` | List learning examples |
| POST | `/api/v1/learning/examples` | Add manual example |
| GET | `/api/v1/learning/examples/{id}` | Get example details |
| PUT | `/api/v1/learning/examples/{id}` | Update example |
| DELETE | `/api/v1/learning/examples/{id}` | Deactivate example |
| GET | `/api/v1/learning/stats` | Get learning statistics |

---

## Confidence Scoring Enhancement

### Current vs Enhanced Scoring

**Current (4 components):**
```python
{
    "dictionary_match": 0.40,
    "metadata_coverage": 0.30,
    "execution_success": 0.20,
    "result_sanity": 0.10
}
```

**Enhanced (8 components):**
```python
{
    "example_similarity": 0.20,    # NEW: Similar verified examples found
    "dictionary_match": 0.15,      # Reduced weight
    "metadata_coverage": 0.15,     # Reduced weight
    "semantic_alignment": 0.15,    # NEW: SQL matches intent
    "complexity_match": 0.10,      # NEW: Appropriate for complexity
    "execution_success": 0.10,     # Reduced weight
    "result_validation": 0.10,     # NEW: Historical comparison
    "result_sanity": 0.05          # Reduced weight
}
```

### Response Actions Based on Confidence

| Confidence | Action | User Experience |
|------------|--------|-----------------|
| 90-100% | RETURN_NORMAL | Direct answer, green indicator |
| 75-89% | RETURN_WITH_WARNING | Answer + "verify assumptions" note |
| 60-74% | RETURN_WITH_VERIFICATION | Answer + detailed explanation + verify prompt |
| 40-59% | ASK_CLARIFICATION | Ask user to clarify before answering |
| <40% | REFUSE | "Cannot provide reliable answer" + suggestions |

---

## How Semantic Similarity Works

### The Problem
User asks: "How many subjects are in the safety population?"

But they might also ask:
- "Count of patients in safety analysis set"
- "Total safety population size"
- "Number of subjects with SAFFL = Y"
- "Safety pop N"
- "give me the safety population count"

### The Solution: Vector Embeddings

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SEMANTIC MATCHING                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. STORE CANONICAL EXAMPLE                                              │
│     Question: "How many subjects are in the safety population?"          │
│     SQL: SELECT COUNT(*) FROM adsl WHERE SAFFL = 'Y'                     │
│     → Embedding: [0.23, -0.45, 0.12, ..., 0.89] (768 dimensions)         │
│                                                                          │
│  2. USER ASKS VARIATION                                                  │
│     "Safety pop N?"                                                      │
│     → Embedding: [0.21, -0.43, 0.14, ..., 0.87] (768 dimensions)         │
│                                                                          │
│  3. VECTOR SIMILARITY SEARCH                                             │
│     Cosine similarity: 0.94 (94% match!)                                 │
│     → Retrieve canonical SQL as example                                  │
│                                                                          │
│  4. FEW-SHOT INJECTION                                                   │
│     LLM prompt includes:                                                 │
│     "Similar query: How many subjects are in the safety population?      │
│      SQL: SELECT COUNT(*) FROM adsl WHERE SAFFL = 'Y'"                   │
│                                                                          │
│  5. LLM GENERATES CORRECT SQL                                            │
│     → High confidence, consistent results                                │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Coverage Estimate

| Canonical Examples | Variations Covered | Coverage |
|-------------------|-------------------|----------|
| 20 | ~500 | 60% of common queries |
| 50 | ~2000 | 80% of common queries |
| 70 | ~5000 | 90% of common queries |
| 100 | ~10000+ | 95% of common queries |

---

## Where This System Will Work Well

### High Accuracy Scenarios (90%+)

| Scenario | Example | Why It Works |
|----------|---------|--------------|
| Standard counts | "How many patients..." | Many training examples |
| CDISC variables | "List subjects where SAFFL=Y" | Clear domain-specific terms |
| Common aggregations | "Average age by treatment" | Pattern-based SQL |
| Trained queries | Any query with feedback history | Direct example match |
| Simple filters | "Show AEs for subject 001" | Straightforward SQL |

### Moderate Accuracy Scenarios (70-89%)

| Scenario | Example | Handling |
|----------|---------|----------|
| Complex joins | "AEs with concomitant meds" | Return with verification |
| Derived calculations | "BMI changes from baseline" | Ask clarification if needed |
| Temporal queries | "Events in first 30 days" | Multiple validation layers |

---

## Where This System Will Fail

### Known Limitations

| Failure Scenario | Example | Mitigation |
|------------------|---------|------------|
| Novel query types | "Predict patient outcome" | REFUSE action + explanation |
| Complex reasoning | "Is treatment A better than B?" | Clarification + expert review |
| Ambiguous terms | "Recent events" (how recent?) | ASK_CLARIFICATION action |
| Clinical judgment | "Significant AE?" | Cannot answer, suggest criteria |
| Data quality issues | Query returns unexpected nulls | Result validation flags |
| Cross-domain queries | "Compare to literature values" | Out of scope detection |

### Realistic Accuracy Expectations

| Query Type | Expected Accuracy | Notes |
|------------|------------------|-------|
| Trained queries | 95-100% | Exact or near-exact match |
| Similar to trained | 85-95% | Semantic similarity works |
| Common patterns | 80-90% | SQL patterns generalize |
| Novel but simple | 70-80% | May need verification |
| Complex novel | 50-70% | Ask clarification or refuse |
| Truly novel | 40-60% | May refuse to answer |

**Overall Target: 85-95% accuracy** (up from 60-70%)

---

## Next Document

See **[Phase 2: Component Implementation](./02-component-implementation.md)** for detailed code specifications.
