# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SAGE (Study Analytics Generative Engine) is an on-premise clinical data AI platform that enables natural language queries against SDTM and ADaM clinical trial datasets. The system uses a RAG architecture with local LLMs to translate natural language to SQL/Python code, which is then executed deterministically against DuckDB.

**Core Principle**: The AI acts as a TRANSLATOR, not an EXPERT. Mathematical calculations are performed by generated code, not by probabilistic AI inference.

## Architecture

### The Four Factories Model

1. **Factory 1 - Data Foundry**: Transforms SAS7BDAT files to Parquet/DuckDB
   - Input: Raw .sas7bdat files (SDTM: DM, AE, CM, LB, VS; ADaM: ADSL, ADAE, ADLB)
   - Output: `clinical_data.duckdb`, Parquet files
   - Script: `scripts/factory1_data.py`

2. **Factory 2 - Metadata Refinery**: Transforms Excel specs into Golden Metadata JSON
   - Input: SDTM/ADaM specification Excel files
   - Output: `knowledge/golden_metadata.json`
   - Script: `scripts/factory2_metadata.py`
   - Requires human approval for each variable via Admin UI

3. **Factory 3 - Dictionary Plant**: Creates fuzzy matching indexes
   - Input: Live data from DuckDB
   - Output: `knowledge/chroma/` (vector store), `fuzzy_index.pkl`, `schema_map.json`
   - Script: `scripts/factory3_dictionary.py`

4. **Factory 4 - Inference Engine**: Runtime query processing
   - Sanitize input → Fuzzy match → Route → Retrieve metadata → Generate SQL → Validate → Execute → Score confidence → Explain

### Technology Stack

| Component | Technology |
|-----------|------------|
| LLM Runtime | Ollama (LLaMA 3.1 70B primary, 8B fallback) |
| Embeddings | nomic-embed-text |
| SQL Database | DuckDB |
| Vector Database | ChromaDB |
| Orchestration | LlamaIndex |
| Chat UI | Chainlit (port 8000) |
| Admin UI | Streamlit (port 8501) |
| Documentation | MkDocs Material (port 8080) |
| Monitoring | Prometheus + Grafana (ports 9090, 3000) |
| Containerization | Docker Compose |
| Fuzzy Matching | RapidFuzz + vector similarity |

## Directory Structure

```
clinical-ai-platform/
├── docker-compose.yml
├── .env
├── docker/
│   ├── ollama/, chat-ui/, admin-ui/, api/, docs/, monitoring/
├── data/
│   ├── raw/           # Drop SAS7BDAT files here
│   ├── processed/     # Parquet output
│   └── database/      # DuckDB files
├── specs/
│   └── raw/           # Drop Excel specs here
├── knowledge/
│   ├── golden_metadata.json
│   ├── schema_map.json
│   └── chroma/        # Vector store
├── logs/audit/
├── scripts/
│   ├── factory1_data.py
│   ├── factory2_metadata.py
│   └── factory3_dictionary.py
└── docs/              # MkDocs source
```

## Commands

```bash
# Start all services
./start.sh
# or
docker compose up -d

# Check service health
curl http://localhost:11434/api/tags  # Ollama
curl http://localhost:8000/health     # Chat UI

# Pull LLM models (if not present)
docker exec clinical-ollama ollama pull llama3.1:70b-instruct-q4_K_M
docker exec clinical-ollama ollama pull nomic-embed-text

# View logs
docker logs clinical-chat
docker logs clinical-ollama
```

## Query Processing Pipeline

1. **Input Sanitization**: Block PHI/PII, SQL injection, prompt injection
2. **Entity Extraction & Fuzzy Matching**: Correct typos (Tyleonl → TYLENOL), resolve synonyms
3. **Intent Classification**: Route to DATA (SQL), DOCUMENT (RAG), or HYBRID pipeline
4. **Metadata Retrieval**: Load Golden Metadata for referenced variables
5. **Code Generation**: Generate DuckDB SQL or Python/Pandas code
6. **Code Validation**: Parse SQL, verify columns against schema, block dangerous operations
7. **Execution**: Sandboxed execution with timeout limits
8. **Confidence Scoring**: Dictionary match (40%) + Metadata coverage (30%) + Execution success (20%) + Result sanity (10%)
9. **Explanation Generation**: Template-based plain-English explanation

## Confidence Score Thresholds

- **90-100% (GREEN)**: High confidence, reliable result
- **70-89% (YELLOW)**: Medium confidence, verify assumptions
- **50-69% (ORANGE)**: Low confidence, review methodology
- **<50% (RED)**: Cannot provide reliable answer

## Security Requirements

- Air-gapped deployment (no outbound internet)
- All LLM inference runs 100% locally
- Block dangerous SQL operations (DELETE, UPDATE, DROP)
- Audit logging for every query with full traceability
- GAMP 5 Category 4 validation approach

## Key Environment Variables

```env
OLLAMA_HOST=http://ollama:11434
PRIMARY_MODEL=deepseek-r1:8b
FALLBACK_MODEL=deepseek-r1:8b
DUCKDB_PATH=/app/data/clinical.duckdb
METADATA_PATH=/app/knowledge/golden_metadata.json
CONFIDENCE_HIGH=90
CONFIDENCE_MEDIUM=70
QUERY_TIMEOUT_SECONDS=30
```

## GPU Requirements

- Minimum: NVIDIA RTX 3090 (24GB VRAM)
- Recommended: NVIDIA A100 (40/80GB) or H100
- System RAM: 64GB minimum, 128GB recommended
