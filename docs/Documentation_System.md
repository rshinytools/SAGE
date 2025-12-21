# SAGE Documentation System Implementation Plan

> **This is the central implementation document for the SAGE Documentation System.**
> All documentation work should reference this file for guidance and structure.

---

## Goal

Transform SAGE into a **self-documenting Enterprise Platform** that:

1. Satisfies auditors/validators with complete code traceability
2. Empowers users with a searchable knowledge base
3. Provides an "Ask the System" chat feature for discovering platform internals

## User Decisions

- **Ask the System:** Option A - Static Knowledge (JSON index, keyword matching)
- **Priority:** Static documentation first, then Ask feature
- **Compliance:** Full GAMP 5 coverage (security, data integrity, accuracy, performance, usability)

---

## Platform Analysis Summary

Based on comprehensive codebase exploration:

| Component | Files | Key Classes | Purpose |
|-----------|-------|-------------|---------|
| Factory 1 (Data) | 6 modules | SASReader, DuckDBLoader, SchemaTracker | SAS→DuckDB pipeline |
| Factory 2 (Metadata) | 7 modules | ExcelParser, MetadataStore, CDISCLibrary | Excel specs→Golden Metadata |
| Factory 3 (Dictionary) | 4 modules | FuzzyMatcher, ValueScanner, SchemaMapper | Fuzzy matching indexes |
| Factory 3.5 (MedDRA) | 2 modules | MedDRALoader, MedDRALookup | Medical terminology |
| Factory 4 (Engine) | 20+ modules | InferencePipeline (9-step), All components | Query processing |
| Admin | 2 modules | AuthProvider, AuditTracker | Authentication, audit |
| Docker/API | 9 routers | FastAPI endpoints | REST API |
| React UI | 13 pages | Admin dashboard, Chat | User interface |

**Total: 60+ Python modules, 13+ React pages, 9 API routers**

---

## Implementation Plan

### Phase 1: Documentation Infrastructure (MkDocs Enhancement)

**Location:** `docs/` (source) → `docker/docs/` (server)

**New Directory Structure:**

```
docs/
├── index.md                          # Landing page with quick links
├── getting-started/
│   ├── installation.md               # Docker setup, prerequisites
│   ├── configuration.md              # .env variables, ports
│   └── first-query.md                # "Hello World" tutorial
├── architecture/
│   ├── overview.md                   # Four Factories high-level
│   ├── data-flow.md                  # Pipeline stages 1-9 diagram
│   ├── security-model.md             # Auth, PHI blocking, SQL validation
│   └── technology-stack.md           # DuckDB, Claude, React, FastAPI
├── factories/
│   ├── factory1-data/
│   │   ├── overview.md               # Data ingestion pipeline
│   │   ├── sas-reader.md             # SAS file handling
│   │   ├── duckdb-loader.md          # Database loading
│   │   ├── schema-tracker.md         # Version control
│   │   └── api-reference.md          # Class/method docs
│   ├── factory2-metadata/
│   │   ├── overview.md               # Metadata refinery
│   │   ├── excel-parser.md           # Spec parsing
│   │   ├── approval-workflow.md      # Human-in-loop
│   │   ├── cdisc-integration.md      # CDISC library
│   │   └── api-reference.md
│   ├── factory3-dictionary/
│   │   ├── overview.md               # Dictionary plant
│   │   ├── fuzzy-matching.md         # RapidFuzz index
│   │   ├── value-scanning.md         # Data profiling
│   │   └── api-reference.md
│   ├── factory35-meddra/
│   │   ├── overview.md               # MedDRA integration
│   │   ├── hierarchy.md              # SOC→PT→LLT
│   │   └── api-reference.md
│   └── factory4-engine/
│       ├── overview.md               # Inference engine
│       ├── pipeline-steps.md         # 9-step breakdown
│       ├── input-sanitizer.md        # Security layer
│       ├── entity-extractor.md       # Term resolution
│       ├── table-resolver.md         # Clinical rules
│       ├── context-builder.md        # LLM context
│       ├── sql-generator.md          # Claude integration
│       ├── sql-validator.md          # Safety checks
│       ├── executor.md               # DuckDB execution
│       ├── confidence-scorer.md      # 4-component scoring
│       ├── explanation-generator.md  # Response building
│       ├── session-memory.md         # Conversation context
│       ├── medical-synonyms.md       # UK/US, colloquial
│       └── api-reference.md
├── api-reference/
│   ├── overview.md                   # API architecture
│   ├── authentication.md             # JWT tokens
│   ├── chat-endpoints.md             # /chat/* routes
│   ├── data-endpoints.md             # /data/* routes
│   ├── metadata-endpoints.md         # /metadata/* routes
│   ├── dictionary-endpoints.md       # /dictionary/* routes
│   ├── system-endpoints.md           # /system/* routes
│   └── response-models.md            # Pydantic models
├── admin-guide/
│   ├── user-management.md            # User CRUD, roles
│   ├── data-loading.md               # Factory 1 operation
│   ├── metadata-approval.md          # Factory 2 workflow
│   ├── dictionary-rebuild.md         # Factory 3 operation
│   ├── monitoring.md                 # Prometheus/Grafana
│   └── troubleshooting.md            # Common issues
├── user-guide/
│   ├── chat-interface.md             # How to use chat
│   ├── asking-questions.md           # Best practices
│   ├── understanding-results.md      # Interpreting answers
│   ├── confidence-scores.md          # GREEN/YELLOW/RED
│   └── example-queries.md            # 20+ examples
├── compliance/
│   ├── validation-summary.md         # GAMP 5 Category 4
│   ├── audit-trail.md                # Query logging
│   ├── data-integrity.md             # Deterministic processing
│   ├── access-controls.md            # RBAC documentation
│   └── traceability-matrix.md        # Req→Code→Test mapping
└── code-reference/
    ├── index.md                      # Auto-generated index
    ├── core-data.md                  # core/data/* modules
    ├── core-metadata.md              # core/metadata/* modules
    ├── core-dictionary.md            # core/dictionary/* modules
    ├── core-meddra.md                # core/meddra/* modules
    ├── core-engine.md                # core/engine/* modules
    ├── core-admin.md                 # core/admin/* modules
    └── docker-api.md                 # docker/api/* routers
```

**Files to Create/Modify:**

- `docs/mkdocs.yml` - Update navigation with new structure
- 50+ new Markdown files with comprehensive content

---

### Phase 2: Code Analysis Script

**Purpose:** Auto-generate documentation from code docstrings and structure

**New Script:** `scripts/generate_code_docs.py`

**Functionality:**

1. Scan all Python files in `core/` and `docker/api/`
2. Extract:
   - Module docstrings
   - Class definitions with docstrings
   - Method signatures with docstrings
   - Type hints
   - Dependencies (imports)
3. Generate Markdown files in `docs/code-reference/`
4. Create cross-reference index

**Output Format (per module):**

```markdown
# core.engine.pipeline

## Overview
Main orchestrator for the 9-step query processing pipeline.

## Classes

### InferencePipeline
**File:** `core/engine/pipeline.py:45`

Main pipeline class that orchestrates query processing.

**Constructor:**
def __init__(self, config: PipelineConfig)

**Key Methods:**
| Method | Description | Returns |
|--------|-------------|---------|
| `process(query)` | Main entry point | `PipelineResult` |
| `process_with_session(query, session_id)` | With conversation context | `PipelineResult` |
| `is_ready()` | Check component health | `bool` |

**Dependencies:**
- `InputSanitizer` - Step 1
- `EntityExtractor` - Step 2
- `TableResolver` - Step 3
```

---

### Phase 3: Knowledge Base for "Ask the System"

**Purpose:** Enable users to query about platform internals via chat

**Implementation Approach:**

#### Implementation: Static Knowledge Ingestion

1. Generate `knowledge/system_docs.json` from:
   - All Markdown documentation
   - Code docstrings (via generate_code_docs.py)
   - CLAUDE.md content
   - Configuration files

2. Create `SystemKnowledge` class (similar to existing `DataKnowledge`)
   - Stores documentation chunks with keywords
   - Provides keyword/phrase-based search
   - Returns relevant documentation sections

3. Add "meta-query" detection in pipeline:
   - Detect questions like "How does SAGE handle...?"
   - Route to SystemKnowledge instead of clinical data
   - Return documentation-based answer

**New Files:**

- `core/engine/system_knowledge.py` - System documentation store
- `knowledge/system_docs.json` - Indexed documentation
- `scripts/index_system_docs.py` - Documentation indexer

**Pipeline Integration:**

```python
# In pipeline.py, after intent classification
if query_analysis.is_meta_query:
    return self._answer_from_system_knowledge(query)
```

**Example Meta-Queries:**

- "How does SAGE handle SQL injection?"
- "What columns are used for serious adverse events?"
- "How does the confidence scoring work?"
- "What is the difference between AESER and AESEV?"

---

### Phase 4: Traceability Matrix

**Purpose:** Map Requirements → Code → Tests for compliance

**New File:** `docs/compliance/traceability-matrix.md`

**GAMP 5 Requirement Categories:**

| Category | Prefix | Example Requirements |
|----------|--------|---------------------|
| Security | SEC-* | PHI blocking, SQL injection, auth, audit |
| Data Integrity | DAT-* | Deterministic processing, schema validation, data lineage |
| Accuracy | ACC-* | Confidence scoring, entity resolution, SQL validation |
| Performance | PRF-* | Response time, caching, timeout handling |
| Usability | USE-* | Error messages, help system, documentation |

**Sample Traceability Matrix:**

| Req ID | Requirement | Implementation | Test Coverage |
|--------|-------------|----------------|---------------|
| SEC-001 | Block PHI/PII in queries | `input_sanitizer.py:PHI_PATTERNS` | `test_input_sanitizer.py::test_phi_blocking` |
| SEC-002 | Block SQL injection | `sql_validator.py:DANGEROUS_PATTERNS` | `test_sql_validator.py::test_sql_injection` |
| SEC-003 | JWT authentication | `auth.py:LocalAuthProvider` | `test_auth.py::test_jwt_token` |
| SEC-004 | Query audit logging | `tracker_db.py:AuditTracker` | `test_audit.py::test_query_logging` |
| DAT-001 | SAS file ingestion | `sas_reader.py:SASReader` | `test_universal_reader.py` |
| DAT-002 | Schema version tracking | `schema_tracker.py` | `test_schema_tracker.py` |
| DAT-003 | Deterministic SQL execution | `executor.py:read_only=True` | `test_executor.py::test_read_only` |
| ACC-001 | 4-component confidence scoring | `confidence_scorer.py` | `test_accuracy_components.py` |
| ACC-002 | Entity fuzzy matching | `fuzzy_matcher.py` | `test_fuzzy_matcher.py` |
| ACC-003 | Medical synonym resolution | `medical_synonyms.py` | `test_medical_synonyms.py` |
| PRF-001 | Query caching | `cache.py:QueryCache` | `test_cache.py` |
| PRF-002 | Instant responses (<100ms) | `pipeline.py:INSTANT_PATTERNS` | `test_instant_responses.py` |
| USE-001 | Human-readable errors | `error_humanizer.py` | `test_error_handling.py` |
| USE-002 | Confidence explanations | `explanation_generator.py` | `test_e2e_pipeline.py` |

**Auto-Generation Script:** `scripts/generate_traceability.py`

- Scans test files for test functions
- Maps to source files via imports
- Generates markdown table

---

### Phase 5: Interactive Documentation Features

**Enhancements to MkDocs:**

1. **Search Enhancement**
   - Full-text search across all docs
   - Code snippet search
   - API endpoint search

2. **Mermaid Diagrams**
   - Pipeline flow diagram
   - Authentication flow
   - Data flow between factories
   - Class hierarchy diagrams

3. **Code Tabs**
   - Show Python implementation alongside documentation
   - Syntax highlighting

4. **Admonitions**
   - Warning boxes for security-critical sections
   - Info boxes for best practices
   - Note boxes for auditor-relevant information

---

## Implementation Steps (Priority Order)

### PART A: Static Documentation (Priority 1)

#### Step 1: Create Documentation Structure

- [ ] Create all directory folders under `docs/`
- [ ] Create placeholder `.md` files for each section
- [ ] Update `docs/mkdocs.yml` with full navigation

#### Step 2: Write Architecture Documentation

- [ ] `architecture/overview.md` - Four Factories model
- [ ] `architecture/data-flow.md` - 9-step pipeline with Mermaid diagram
- [ ] `architecture/security-model.md` - Complete security documentation
- [ ] `architecture/technology-stack.md` - All technologies used

#### Step 3: Document Each Factory

- [ ] Factory 1: 5 documentation files
- [ ] Factory 2: 5 documentation files
- [ ] Factory 3: 4 documentation files
- [ ] Factory 3.5: 3 documentation files
- [ ] Factory 4: 13 documentation files

#### Step 4: Create Code Analysis Script

- [ ] `scripts/generate_code_docs.py`
- [ ] Run to generate `docs/code-reference/*.md`

#### Step 5: Create API Reference

- [ ] Document all 9 FastAPI routers
- [ ] Include request/response models
- [ ] Add example curl commands

#### Step 6: Create User & Admin Guides

- [ ] User guide: 5 files (chat, queries, results, confidence, examples)
- [ ] Admin guide: 6 files (users, data, metadata, dictionary, monitoring, troubleshooting)

#### Step 7: Create Compliance Documentation (Full GAMP 5)

- [ ] `compliance/validation-summary.md` - GAMP 5 Category 4 overview
- [ ] `compliance/audit-trail.md` - Query logging documentation
- [ ] `compliance/data-integrity.md` - Deterministic processing
- [ ] `compliance/access-controls.md` - RBAC documentation
- [ ] `compliance/traceability-matrix.md` - Full requirement mapping
- [ ] `scripts/generate_traceability.py` - Auto-generate from code/tests

#### Step 8: Test Documentation

- [ ] Build and serve documentation locally (`mkdocs serve`)
- [ ] Test all navigation links
- [ ] Review with auditor checklist
- [ ] Verify all 60+ modules are documented

---

### PART B: "Ask the System" Feature (Priority 2)

#### Step 9: Build System Knowledge Base

- [ ] `scripts/index_system_docs.py` - Index all documentation
- [ ] `knowledge/system_docs.json` - Structured doc index
- [ ] Keyword extraction and categorization

#### Step 10: Create SystemKnowledge Class

- [ ] `core/engine/system_knowledge.py`
- [ ] Load from `system_docs.json`
- [ ] Implement keyword-based search
- [ ] Return relevant documentation sections

#### Step 11: Add Meta-Query Detection to Pipeline

- [ ] Detect meta-queries: "How does SAGE...", "What is...", "Explain..."
- [ ] Add `is_meta_query` flag to QueryAnalysis
- [ ] Route to SystemKnowledge instead of clinical data
- [ ] Format documentation-based response

#### Step 12: Test "Ask the System"

- [ ] Test: "How does SAGE handle SQL injection?"
- [ ] Test: "What is the confidence scoring formula?"
- [ ] Test: "Explain the difference between AESER and AESEV"
- [ ] Test: "How does fuzzy matching work?"
- [ ] Verify responses are accurate and helpful

---

## Key Files Summary

### Utility Scripts (Optional - Can Be Removed After Use)

| File | Purpose | Removable? |
|------|---------|------------|
| `scripts/generate_code_docs.py` | Auto-generate API reference from docstrings | Yes - run once, delete after |
| `scripts/index_system_docs.py` | Index docs for "Ask the System" feature | Yes - run once, delete after |
| `scripts/generate_traceability.py` | Generate compliance traceability matrix | Yes - run once, delete after |

**Note:** These are one-time utility scripts for documentation generation. They:

- Do NOT modify any core platform code
- Can be run once and deleted
- Are NOT required for SAGE to function
- Only regenerate static Markdown/JSON files

### New Core Modules

| File | Purpose |
|------|---------|
| `core/engine/system_knowledge.py` | System documentation store and retrieval |

### New Knowledge Files

| File | Purpose |
|------|---------|
| `knowledge/system_docs.json` | Indexed documentation for meta-queries |

### Documentation Files

| Location | Count | Purpose |
|----------|-------|---------|
| `docs/architecture/` | 4 files | High-level system design |
| `docs/factories/` | 30 files | Detailed factory documentation |
| `docs/api-reference/` | 8 files | REST API documentation |
| `docs/admin-guide/` | 6 files | Administrator operations |
| `docs/user-guide/` | 5 files | End-user documentation |
| `docs/compliance/` | 5 files | Audit/validation docs |
| `docs/code-reference/` | 7 files | Auto-generated code docs |

**Total: ~65 new documentation files**

---

## Success Criteria

| Criterion | Validation Method |
|-----------|-------------------|
| Auditor can find any code explanation | Navigate docs → find module → see implementation details |
| User can understand confidence scores | User guide explains GREEN/YELLOW/RED with examples |
| "Ask the System" works | Query "How does SAGE block SQL injection?" returns accurate answer |
| Traceability complete | Every security requirement maps to code and tests |
| All APIs documented | Every endpoint has request/response examples |
| Factory documentation complete | Each of 60+ modules has documentation page |

---

## Dependencies

- MkDocs Material theme (already configured in docker/docs/)
- Mermaid diagrams (already in mkdocs.yml)
- Python AST module (for code analysis - built into Python)
- Existing golden_metadata.json structure

## Docker Changes Required

**None** - The existing MkDocs container:

- Already has all required plugins installed
- Mounts `docs/` directory as volume
- Automatically serves new files when added
- No Dockerfile or docker-compose changes needed

We only edit:

- `docs/*.md` files (content)
- `docs/mkdocs.yml` (navigation structure)

---

## Notes

- **No code changes to core platform** - documentation only
- **Read-only analysis** - scripts analyze but don't modify
- **Incremental build** - can deploy documentation as sections complete
- **Version controlled** - all docs in git with the codebase
