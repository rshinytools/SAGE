# Audit Trail

SAGE maintains comprehensive audit logs for regulatory compliance.

---

## Overview

Every user action is logged with:

- Who performed the action
- When it occurred
- What was requested
- What was returned
- How long it took

---

## Logged Events

### Query Events

| Field | Description |
|-------|-------------|
| timestamp | ISO 8601 timestamp |
| event_type | "query" |
| user_id | Authenticated user |
| session_id | Conversation ID |
| query | Original question |
| sql_generated | Generated SQL |
| tables_accessed | Tables queried |
| columns_used | Columns in query |
| result_count | Rows returned |
| confidence_score | 0-100 score |
| execution_time_ms | Processing time |

### Authentication Events

| Field | Description |
|-------|-------------|
| timestamp | ISO 8601 timestamp |
| event_type | "login", "logout", "failed_login" |
| user_id | Attempted user |
| ip_address | Client IP |
| success | true/false |
| failure_reason | If applicable |

### System Events

| Field | Description |
|-------|-------------|
| timestamp | ISO 8601 timestamp |
| event_type | "startup", "shutdown", "config_change" |
| performed_by | User or "system" |
| details | Event specifics |

---

## Log Format

### Query Log Example

```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "event_type": "query",
  "user_id": "analyst_001",
  "session_id": "sess_abc123def456",
  "query": "How many subjects had headache?",
  "sanitization": {
    "is_safe": true,
    "detected_patterns": []
  },
  "entities": [
    {
      "original": "headache",
      "matched": "HEADACHE",
      "confidence": 100
    }
  ],
  "table_resolution": {
    "table": "ADAE",
    "population": "Safety",
    "population_filter": "SAFFL='Y'"
  },
  "sql_generated": "SELECT COUNT(DISTINCT USUBJID) FROM ADAE WHERE SAFFL='Y' AND UPPER(AEDECOD)='HEADACHE'",
  "tables_accessed": ["ADAE"],
  "columns_used": ["USUBJID", "AEDECOD", "SAFFL"],
  "execution": {
    "success": true,
    "row_count": 1,
    "execution_time_ms": 50
  },
  "result_count": 45,
  "confidence_score": 95,
  "confidence_level": "HIGH",
  "total_time_ms": 1500,
  "ip_address": "192.168.1.100"
}
```

---

## Log Storage

### Location

```
logs/
└── audit/
    ├── queries_2024-01-15.jsonl
    ├── queries_2024-01-14.jsonl
    ├── auth_2024-01.jsonl
    └── system_2024-01.jsonl
```

### Retention

| Log Type | Default Retention |
|----------|-------------------|
| Query logs | 7 years |
| Auth logs | 3 years |
| System logs | 1 year |

### Format

- JSON Lines (.jsonl)
- One record per line
- Timestamped filenames
- Compressed after 7 days

---

## Querying Audit Logs

### Using Audit API

```bash
# Get queries for a user
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8002/api/v1/audit/queries?user_id=analyst_001"

# Get queries for a session
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8002/api/v1/audit/queries?session_id=sess_abc123"

# Get queries by date range
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8002/api/v1/audit/queries?from=2024-01-01&to=2024-01-15"
```

### Export to CSV

```bash
curl -H "Authorization: Bearer $TOKEN" \
  -H "Accept: text/csv" \
  "http://localhost:8002/api/v1/audit/export?from=2024-01-01"
```

---

## Audit Reports

### Daily Summary

```
SAGE Audit Summary: 2024-01-15
================================

Queries: 150
Unique Users: 12
Tables Accessed:
  - ADAE: 85 queries
  - ADSL: 45 queries
  - ADLB: 20 queries

Confidence Distribution:
  - HIGH: 120 (80%)
  - MEDIUM: 25 (17%)
  - LOW: 5 (3%)

Failed Queries: 2
  - 10:30 - Term not found
  - 14:15 - Timeout

Authentication:
  - Successful logins: 15
  - Failed logins: 2
================================
```

### User Activity Report

```
User: analyst_001
Period: 2024-01-01 to 2024-01-15
================================

Total Queries: 234
Average Confidence: 92%

Most Queried Tables:
  1. ADAE (156 queries)
  2. ADSL (45 queries)
  3. ADLB (33 queries)

Session Count: 15
Average Session Duration: 45 min

Query Patterns:
  - Subject counts: 45%
  - AE analysis: 35%
  - Demographics: 20%
================================
```

---

## Compliance Requirements

### 21 CFR Part 11

| Requirement | SAGE Implementation |
|-------------|---------------------|
| 11.10(b) - Generate accurate copies | Export to CSV/PDF |
| 11.10(c) - Record protection | Read-only logs, backups |
| 11.10(e) - Audit trail | Comprehensive logging |
| 11.10(g) - Authority checks | RBAC enforcement |
| 11.10(k) - Device checks | Session management |

### EU Annex 11

| Requirement | SAGE Implementation |
|-------------|---------------------|
| 9 - Audit trail | Query/auth logging |
| 12.1 - Security | Access controls |
| 12.4 - Audit trail | Cannot modify logs |

---

## Log Integrity

### Immutability

Logs cannot be modified after creation:
- Append-only file format
- Hash chains for verification
- Regular backup to secure storage

### Verification

```bash
# Verify log integrity
python scripts/verify_audit_logs.py --date 2024-01-15

# Output:
# Checking logs/audit/queries_2024-01-15.jsonl
# Records: 150
# Hash chain: VERIFIED
# No gaps detected
```

---

## Configuration

### Environment Variables

```env
# Audit settings
AUDIT_LOG_PATH=/app/logs/audit
AUDIT_RETENTION_DAYS=2555  # 7 years
AUDIT_ENABLE_COMPRESSION=true
AUDIT_COMPRESSION_AFTER_DAYS=7

# What to log
AUDIT_LOG_QUERIES=true
AUDIT_LOG_AUTH=true
AUDIT_LOG_SYSTEM=true
AUDIT_LOG_FULL_RESPONSE=false  # Include result data
```

---

## Best Practices

1. **Regular Review**: Check logs weekly for anomalies
2. **Secure Storage**: Store backups off-site
3. **Access Control**: Limit who can view logs
4. **Retention Policy**: Follow regulatory requirements
5. **Integrity Checks**: Verify hash chains regularly

---

## Next Steps

- [Data Integrity](data-integrity.md)
- [Access Controls](access-controls.md)
- [Validation Summary](validation-summary.md)
