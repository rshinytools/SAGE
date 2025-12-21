# Installation Guide

This guide walks you through installing SAGE on your local or enterprise environment.

---

## Prerequisites

### Hardware Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 4 cores | 8+ cores |
| RAM | 16 GB | 32+ GB |
| Storage | 50 GB SSD | 200+ GB SSD |
| GPU | Not required | Not required |

!!! note "GPU Not Required"
    SAGE uses Claude API for language processing. No local GPU is needed.

### Software Requirements

| Software | Version | Notes |
|----------|---------|-------|
| Docker | 24.0+ | With Docker Compose v2 |
| Python | 3.11+ | For running factory scripts |
| Git | 2.30+ | For version control |

---

## Installation Steps

### Step 1: Clone the Repository

```bash
git clone https://github.com/your-org/sage-platform.git
cd sage-platform
```

### Step 2: Configure Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` with your settings:

```env
# Claude API Configuration
ANTHROPIC_API_KEY=your-api-key-here

# Database Settings
DUCKDB_PATH=/app/data/clinical.duckdb
METADATA_PATH=/app/knowledge/golden_metadata.json

# Confidence Thresholds
CONFIDENCE_HIGH=90
CONFIDENCE_MEDIUM=70

# Query Settings
QUERY_TIMEOUT_SECONDS=30
MAX_RESULT_ROWS=10000
```

### Step 3: Start Services

**Windows (PowerShell):**
```powershell
.\start.ps1
```

**Linux/Mac:**
```bash
./start.sh
```

Or using Docker Compose directly:
```bash
docker compose up -d
```

### Step 4: Verify Installation

Check service status:
```bash
docker compose ps
```

Expected output:
```
NAME                STATUS              PORTS
sage-api            running             0.0.0.0:8002->8000/tcp
sage-ui             running             0.0.0.0:80->80/tcp
sage-docs           running             0.0.0.0:8080->8080/tcp
sage-grafana        running             0.0.0.0:3000->3000/tcp
```

---

## Access Points

After successful installation, access SAGE at:

| Service | URL | Purpose |
|---------|-----|---------|
| **Main UI** | http://localhost | Chat interface |
| **API** | http://localhost:8002 | REST API endpoints |
| **Documentation** | http://localhost:8080 | This documentation |
| **Grafana** | http://localhost:3000 | Monitoring dashboards |

---

## Initial Setup

### Load Clinical Data

Before querying, you need to load clinical trial data:

1. Place SAS7BDAT files in `data/raw/`
2. Run Factory 1 (Data Foundry):
   ```bash
   python scripts/factory1_data.py
   ```

### Load Metadata Specifications

1. Place Excel specification files in `specs/raw/`
2. Run Factory 2 (Metadata Refinery):
   ```bash
   python scripts/factory2_metadata.py
   ```
3. Approve metadata mappings in Admin UI

### Build Dictionary Indexes

After data and metadata are loaded:
```bash
python scripts/factory3_dictionary.py
```

---

## Troubleshooting

### Common Issues

**Docker containers won't start:**
```bash
# Check logs
docker compose logs

# Restart services
docker compose restart
```

**API not responding:**
```bash
# Check API health
curl http://localhost:8002/health
```

**Database not found:**
- Ensure Factory 1 has been run
- Check `data/database/` for DuckDB files

---

## Next Steps

- [Configure environment settings](configuration.md)
- [Run your first query](first-query.md)
- [Load your clinical data](../admin-guide/data-loading.md)
