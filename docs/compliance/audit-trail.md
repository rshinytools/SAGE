# Audit Trail

SAGE maintains comprehensive audit logs for regulatory compliance with 21 CFR Part 11 and EU Annex 11 requirements.

---

## Overview

Every user action is logged with full traceability:

- **Who** performed the action (authenticated user)
- **When** it occurred (ISO 8601 timestamp)
- **What** was requested (full request details)
- **What** was returned (response/results)
- **How long** it took (duration in milliseconds)
- **Integrity checksum** (SHA-256 for tamper detection)

---

## Architecture

### Database Schema

SAGE uses SQLite for persistent, immutable audit storage:

```
data/
└── audit.db              # SQLite database
    ├── audit_logs        # Main audit events
    ├── query_audit_details   # LLM/query specifics
    └── electronic_signatures # 21 CFR Part 11 signatures
```

### Core Tables

#### audit_logs (Main Events)

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| timestamp | TEXT | ISO 8601 timestamp |
| user_id | TEXT | User identifier |
| username | TEXT | Display username |
| action | TEXT | Event type (LOGIN, QUERY, UPLOAD, etc.) |
| resource_type | TEXT | Resource category (chat, data, system) |
| resource_id | TEXT | Specific resource identifier |
| status | TEXT | success, failure, or error |
| ip_address | TEXT | Client IP address |
| user_agent | TEXT | Client browser/application |
| request_method | TEXT | HTTP method |
| request_path | TEXT | API endpoint path |
| request_body | TEXT | Request payload (JSON, sanitized) |
| response_status | INTEGER | HTTP status code |
| response_body | TEXT | Response payload (truncated) |
| duration_ms | INTEGER | Processing time |
| error_message | TEXT | Error details if applicable |
| checksum | TEXT | SHA-256 integrity hash |
| created_at | TEXT | Record creation time |

#### query_audit_details (LLM Interactions)

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| audit_log_id | INTEGER | Reference to audit_logs |
| original_question | TEXT | User's original question |
| sanitized_question | TEXT | After input sanitization |
| intent_classification | TEXT | DATA, DOCUMENT, or HYBRID |
| matched_entities | TEXT | JSON array of fuzzy matches |
| generated_sql | TEXT | SQL code generated |
| llm_prompt | TEXT | Full prompt sent to LLM |
| llm_response | TEXT | Raw LLM response |
| llm_model | TEXT | Model used (e.g., llama3.1:70b) |
| llm_tokens_used | INTEGER | Token count |
| confidence_score | REAL | 0-100 confidence |
| execution_time_ms | INTEGER | Query execution time |
| result_row_count | INTEGER | Rows returned |

#### electronic_signatures (21 CFR Part 11)

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| audit_log_id | INTEGER | Reference to audit_logs |
| signer_user_id | TEXT | Signer's user ID |
| signer_username | TEXT | Signer's username |
| signature_meaning | TEXT | Reviewed, Approved, Submitted |
| signature_timestamp | TEXT | When signed |
| signature_hash | TEXT | HMAC of audit record |

---

## Logged Events

### Authentication Events

| Action | When Logged | Details Captured |
|--------|-------------|------------------|
| LOGIN | Successful login | User, IP, user agent, method |
| LOGIN_FAILED | Failed login attempt | Username tried, failure reason, IP |
| LOGOUT | User logout | Session duration |
| TOKEN_REFRESH | Token renewed | Old/new expiry |
| PASSWORD_CHANGE | Password updated | User, success/failure |

### Query Events

| Action | When Logged | Details Captured |
|--------|-------------|------------------|
| QUERY | Natural language query | Full LLM interaction details |
| QUERY_FAILED | Query processing error | Error message, stage failed |

**Query Details Include:**
- Original user question
- Sanitized/cleaned question
- Intent classification (DATA/DOCUMENT/HYBRID)
- Fuzzy-matched entities with confidence
- Generated SQL code
- Full LLM prompt
- Raw LLM response
- Model used and tokens consumed
- Confidence score breakdown
- Execution time and row count

### Data Events

| Action | When Logged | Details Captured |
|--------|-------------|------------------|
| DATA_UPLOAD | File uploaded | Filename, size, row count, columns |
| DATA_TRANSFORM | ETL processing | Source, destination, transformations |
| DATA_EXPORT | Data exported | Format, row count, destination |

### API Request Events

| Action | When Logged | Details Captured |
|--------|-------------|------------------|
| API_REQUEST | Every API call | Method, path, status, duration |

**Excluded Paths** (not logged):
- `/health` - Health checks
- `/docs` - Swagger documentation
- `/openapi.json` - OpenAPI spec
- `/redoc` - ReDoc documentation

### System Events

| Action | When Logged | Details Captured |
|--------|-------------|------------------|
| STARTUP | Service start | Version, configuration |
| SHUTDOWN | Service stop | Reason, uptime |
| CONFIG_CHANGE | Configuration modified | What changed, by whom |

---

## Log Examples

### Query Log Example

```json
{
  "id": 1234,
  "timestamp": "2024-01-15T10:30:00.000Z",
  "user_id": "analyst_001",
  "username": "Dr. Smith",
  "action": "QUERY",
  "resource_type": "chat",
  "resource_id": "conv_abc123",
  "status": "success",
  "ip_address": "192.168.1.100",
  "duration_ms": 1500,
  "checksum": "sha256:abc123...",
  "query_details": {
    "original_question": "How many subjects had headache?",
    "sanitized_question": "How many subjects had headache?",
    "intent_classification": "DATA",
    "matched_entities": [
      {"original": "headache", "matched": "HEADACHE", "confidence": 100}
    ],
    "generated_sql": "SELECT COUNT(DISTINCT USUBJID) FROM ADAE WHERE SAFFL='Y' AND UPPER(AEDECOD)='HEADACHE'",
    "llm_model": "llama3.1:70b-instruct-q4_K_M",
    "llm_tokens_used": 450,
    "confidence_score": 95.0,
    "execution_time_ms": 50,
    "result_row_count": 1
  }
}
```

### Authentication Log Example

```json
{
  "id": 5678,
  "timestamp": "2024-01-15T08:00:00.000Z",
  "user_id": "analyst_001",
  "username": "Dr. Smith",
  "action": "LOGIN",
  "status": "success",
  "ip_address": "192.168.1.100",
  "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
  "checksum": "sha256:def456..."
}
```

### Failed Login Example

```json
{
  "id": 5679,
  "timestamp": "2024-01-15T08:05:00.000Z",
  "user_id": "unknown_user",
  "action": "LOGIN_FAILED",
  "status": "failure",
  "ip_address": "192.168.1.200",
  "error_message": "Invalid credentials",
  "checksum": "sha256:ghi789..."
}
```

---

## API Endpoints

### Search Audit Logs

```bash
# Get all logs with filters
GET /api/v1/audit/logs?user_id=analyst_001&action=QUERY&status=success&start_date=2024-01-01&end_date=2024-01-31&page=1&page_size=50

# Response
{
  "logs": [...],
  "total": 234,
  "page": 1,
  "page_size": 50,
  "total_pages": 5
}
```

### Get Log Details

```bash
# Get single log with query details
GET /api/v1/audit/logs/1234

# Response includes full query_details if action=QUERY
```

### Export Logs

```bash
# Export to Excel
GET /api/v1/audit/export/excel?start_date=2024-01-01&end_date=2024-01-31
# Returns: audit_logs_2024-01-01_to_2024-01-31.xlsx

# Export to PDF
GET /api/v1/audit/export/pdf?start_date=2024-01-01&end_date=2024-01-31
# Returns: audit_report_2024-01-01_to_2024-01-31.pdf

# Export to CSV (existing)
GET /api/v1/audit/export/csv?start_date=2024-01-01&end_date=2024-01-31
# Returns: audit_logs.csv
```

### Statistics

```bash
# Get audit statistics
GET /api/v1/audit/statistics?start_date=2024-01-01&end_date=2024-01-31

# Response
{
  "total_events": 5000,
  "by_action": {
    "QUERY": 3500,
    "LOGIN": 450,
    "API_REQUEST": 1000,
    ...
  },
  "by_status": {
    "success": 4800,
    "failure": 150,
    "error": 50
  },
  "by_user": {...},
  "average_query_confidence": 87.5
}
```

### Integrity Verification

```bash
# Verify record integrity
GET /api/v1/audit/logs/1234/verify

# Response
{
  "log_id": 1234,
  "integrity_valid": true,
  "checksum": "sha256:abc123...",
  "verified_at": "2024-01-15T12:00:00Z"
}
```

### Electronic Signatures

```bash
# Add signature to audit record
POST /api/v1/audit/logs/1234/signature
{
  "meaning": "Reviewed"
}

# Response
{
  "signature_id": 1,
  "log_id": 1234,
  "signer": "Dr. Smith",
  "meaning": "Reviewed",
  "timestamp": "2024-01-15T12:00:00Z"
}
```

---

## Admin UI Features

### Audit Logs Page

Access via: **Admin UI > Audit Logs**

**Features:**
- **Statistics Dashboard**: Event counts, success rates, user activity
- **Filter Panel**: Filter by user, action, resource, status, date range
- **Data Table**: Paginated logs with sortable columns
- **Log Details Modal**: View full event details, query information
- **Export Options**: Download as Excel, PDF, CSV, or JSON
- **Integrity Verification**: Checkmark indicates verified records
- **Electronic Signatures**: Sign records for compliance

---

## 21 CFR Part 11 Compliance

| Requirement | Implementation |
|-------------|----------------|
| 11.10(b) - Accurate copies | Export to Excel/PDF/CSV with full fidelity |
| 11.10(c) - Record protection | Immutable SQLite (no UPDATE/DELETE), backups |
| 11.10(e) - Audit trail | Comprehensive logging of all actions |
| 11.10(g) - Authority checks | RBAC enforcement, authenticated access |
| 11.10(k) - Device checks | Session management, token validation |
| 11.50 - Signature manifestations | Signature meaning captured (Reviewed/Approved) |
| 11.70 - Signature linking | HMAC links signature to record content |

### Integrity Features

1. **SHA-256 Checksums**: Every record includes hash of its content
2. **Immutable Storage**: No UPDATE/DELETE operations on audit tables
3. **Tamper Detection**: Verify endpoint checks record integrity
4. **Electronic Signatures**: Cryptographically signed approvals
5. **User Attribution**: All events tied to authenticated user
6. **Complete Trail**: Full context captured for every action

---

## EU Annex 11 Compliance

| Requirement | Implementation |
|-------------|----------------|
| 9 - Audit trail | Comprehensive query/auth logging |
| 12.1 - Security | JWT authentication, RBAC |
| 12.4 - Audit trail | Immutable logs, no modification possible |
| 16 - Business continuity | Regular backups, SQLite durability |

---

## Configuration

### Environment Variables

```env
# Audit database location
AUDIT_DB_PATH=/app/data/audit.db

# Retention (no automatic deletion - permanent storage)
AUDIT_RETENTION_POLICY=permanent

# What to log
AUDIT_LOG_QUERIES=true
AUDIT_LOG_AUTH=true
AUDIT_LOG_API_REQUESTS=true
AUDIT_LOG_SYSTEM=true

# Response body truncation (bytes)
AUDIT_MAX_RESPONSE_SIZE=10000

# Excluded API paths (comma-separated)
AUDIT_EXCLUDED_PATHS=/health,/docs,/openapi.json,/redoc
```

---

## Best Practices

1. **Regular Review**: Check audit logs weekly for anomalies
2. **Backup Strategy**: Include audit.db in regular backups
3. **Access Control**: Limit who can view audit logs (admin only)
4. **Integrity Verification**: Run periodic integrity checks
5. **Retention Compliance**: Maintain logs per regulatory requirements (7+ years)
6. **Export Archives**: Periodically export to PDF for long-term storage

---

## Storage Estimates

| Activity Level | Daily Events | Monthly Size | Annual Size |
|----------------|--------------|--------------|-------------|
| Low (10 users) | ~500 | ~50 MB | ~600 MB |
| Medium (50 users) | ~2,500 | ~250 MB | ~3 GB |
| High (200 users) | ~10,000 | ~1 GB | ~12 GB |

*Estimates assume average event size of 3 KB including query details*

---

## Related Documentation

- [Security Model](../architecture/security-model.md)
- [Validation Summary](validation-summary.md)
- [User Management](../admin-guide/user-management.md)
