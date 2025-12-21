# Configuration Guide

This guide explains all configuration options available in SAGE.

---

## Environment Variables

All configuration is managed through environment variables in the `.env` file.

### Core Settings

```env
# API Configuration
ANTHROPIC_API_KEY=sk-ant-...      # Required: Claude API key

# Database Paths
DUCKDB_PATH=/app/data/clinical.duckdb
METADATA_PATH=/app/knowledge/golden_metadata.json
SCHEMA_MAP_PATH=/app/knowledge/schema_map.json

# Application Mode
DEBUG=false
LOG_LEVEL=INFO
```

### Confidence Thresholds

Control how answers are scored:

```env
# Confidence Score Thresholds (0-100)
CONFIDENCE_HIGH=90      # GREEN badge threshold
CONFIDENCE_MEDIUM=70    # YELLOW badge threshold
# Below MEDIUM = ORANGE, Below 50 = RED
```

| Score Range | Badge | Meaning |
|-------------|-------|---------|
| 90-100% | GREEN | High confidence - reliable result |
| 70-89% | YELLOW | Medium confidence - verify assumptions |
| 50-69% | ORANGE | Low confidence - review methodology |
| <50% | RED | Cannot provide reliable answer |

### Query Settings

```env
# Query Execution
QUERY_TIMEOUT_SECONDS=30     # Max query execution time
MAX_RESULT_ROWS=10000        # Maximum rows returned
ENABLE_CACHING=true          # Cache query results

# SQL Safety
ALLOW_WRITE_OPERATIONS=false # Always false in production
```

### Security Settings

```env
# Authentication
JWT_SECRET_KEY=your-secret-key-here
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_HOURS=24

# PHI/PII Protection
ENABLE_PHI_BLOCKING=true     # Block personal identifiers
ENABLE_SQL_INJECTION_CHECK=true
```

---

## Port Configuration

Default ports used by SAGE services:

| Service | Default Port | Environment Variable |
|---------|-------------|---------------------|
| Main UI | 80 | `UI_PORT` |
| API | 8002 | `API_PORT` |
| Documentation | 8080 | `DOCS_PORT` |
| Grafana | 3000 | `GRAFANA_PORT` |
| Prometheus | 9090 | `PROMETHEUS_PORT` |

To change ports, modify `docker-compose.yml`:

```yaml
services:
  api:
    ports:
      - "${API_PORT:-8002}:8000"
```

---

## Data Directories

SAGE uses specific directories for data management:

```
data/
├── raw/           # Drop SAS7BDAT files here
├── processed/     # Parquet files (auto-generated)
└── database/      # DuckDB files

specs/
└── raw/           # Drop Excel specs here

knowledge/
├── golden_metadata.json  # Approved metadata
├── schema_map.json       # Column mappings
└── chroma/               # Vector store (optional)

logs/
└── audit/         # Query audit logs
```

---

## LLM Configuration

SAGE uses Claude API for natural language processing:

```env
# Primary Model
CLAUDE_MODEL=claude-sonnet-4-20250514

# Request Settings
CLAUDE_MAX_TOKENS=4096
CLAUDE_TEMPERATURE=0
```

!!! warning "Temperature Setting"
    Always use `CLAUDE_TEMPERATURE=0` for deterministic SQL generation.

---

## Monitoring Configuration

Prometheus and Grafana are pre-configured. Custom dashboards are in:

```
docker/monitoring/grafana/dashboards/
```

Prometheus scrape config:
```
docker/monitoring/prometheus/prometheus.yml
```

---

## Development vs Production

### Development Settings

```env
DEBUG=true
LOG_LEVEL=DEBUG
ENABLE_CACHING=false
```

### Production Settings

```env
DEBUG=false
LOG_LEVEL=WARNING
ENABLE_CACHING=true
JWT_SECRET_KEY=<strong-random-key>
```

---

## Validation and Testing

Verify configuration:

```bash
# Check API configuration
curl http://localhost:8002/api/v1/system/config

# Run health check
curl http://localhost:8002/health
```

---

## Next Steps

- [Run your first query](first-query.md)
- [Load clinical data](../admin-guide/data-loading.md)
- [Configure users](../admin-guide/user-management.md)
