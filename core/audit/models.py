"""
Audit Models
============

Pydantic models for audit logging system.
"""

from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class AuditAction(str, Enum):
    """Types of audit actions."""
    # Authentication
    LOGIN = "LOGIN"
    LOGIN_FAILED = "LOGIN_FAILED"
    LOGOUT = "LOGOUT"
    TOKEN_REFRESH = "TOKEN_REFRESH"
    PASSWORD_CHANGE = "PASSWORD_CHANGE"

    # Queries
    QUERY = "QUERY"
    QUERY_FAILED = "QUERY_FAILED"

    # Data operations
    DATA_UPLOAD = "DATA_UPLOAD"
    DATA_TRANSFORM = "DATA_TRANSFORM"
    DATA_EXPORT = "DATA_EXPORT"
    DATA_DELETE = "DATA_DELETE"

    # API requests
    API_REQUEST = "API_REQUEST"

    # System events
    SYSTEM_STARTUP = "SYSTEM_STARTUP"
    SYSTEM_SHUTDOWN = "SYSTEM_SHUTDOWN"
    CONFIG_CHANGE = "CONFIG_CHANGE"

    # Metadata operations
    METADATA_APPROVED = "METADATA_APPROVED"
    METADATA_REJECTED = "METADATA_REJECTED"
    METADATA_MODIFIED = "METADATA_MODIFIED"


class AuditStatus(str, Enum):
    """Status of an audit event."""
    SUCCESS = "success"
    FAILURE = "failure"
    ERROR = "error"


class AuditEvent(BaseModel):
    """Event to be logged in audit trail."""
    timestamp: datetime = Field(default_factory=datetime.now)
    user_id: str
    username: str
    action: AuditAction
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    status: AuditStatus = AuditStatus.SUCCESS
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    request_method: Optional[str] = None
    request_path: Optional[str] = None
    request_body: Optional[str] = None
    response_status: Optional[int] = None
    response_body: Optional[str] = None
    duration_ms: Optional[int] = None
    error_message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class QueryAuditDetails(BaseModel):
    """Detailed information about a query/LLM interaction."""
    original_question: str
    sanitized_question: Optional[str] = None
    intent_classification: Optional[str] = None  # DATA, DOCUMENT, HYBRID
    matched_entities: Optional[List[Dict[str, Any]]] = None
    generated_sql: Optional[str] = None
    llm_prompt: Optional[str] = None
    llm_response: Optional[str] = None
    llm_model: Optional[str] = None
    llm_tokens_used: Optional[int] = None
    confidence_score: Optional[float] = None
    confidence_breakdown: Optional[Dict[str, float]] = None
    execution_time_ms: Optional[int] = None
    result_row_count: Optional[int] = None
    tables_accessed: Optional[List[str]] = None
    columns_used: Optional[List[str]] = None


class AuditLog(BaseModel):
    """Complete audit log record from database."""
    id: int
    timestamp: datetime
    user_id: str
    username: str
    action: str
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    status: str
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    request_method: Optional[str] = None
    request_path: Optional[str] = None
    request_body: Optional[str] = None
    response_status: Optional[int] = None
    response_body: Optional[str] = None
    duration_ms: Optional[int] = None
    error_message: Optional[str] = None
    checksum: str
    created_at: datetime
    details: Optional[Dict[str, Any]] = None
    query_details: Optional[QueryAuditDetails] = None
    signatures: Optional[List["ElectronicSignature"]] = None


class AuditFilters(BaseModel):
    """Filters for searching audit logs."""
    user_id: Optional[str] = None
    username: Optional[str] = None
    action: Optional[str] = None
    actions: Optional[List[str]] = None
    resource_type: Optional[str] = None
    status: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    ip_address: Optional[str] = None
    search_text: Optional[str] = None
    page: int = 1
    page_size: int = 50


class AuditStatistics(BaseModel):
    """Statistics about audit logs."""
    total_events: int = 0
    by_action: Dict[str, int] = Field(default_factory=dict)
    by_status: Dict[str, int] = Field(default_factory=dict)
    by_user: Dict[str, int] = Field(default_factory=dict)
    by_resource_type: Dict[str, int] = Field(default_factory=dict)
    average_query_confidence: Optional[float] = None
    average_duration_ms: Optional[float] = None
    date_range: Optional[Dict[str, str]] = None


class ElectronicSignature(BaseModel):
    """Electronic signature for 21 CFR Part 11 compliance."""
    id: Optional[int] = None
    audit_log_id: int
    signer_user_id: str
    signer_username: str
    signature_meaning: str  # 'Reviewed', 'Approved', 'Submitted'
    signature_timestamp: datetime = Field(default_factory=datetime.now)
    signature_hash: Optional[str] = None


class IntegrityCheckResult(BaseModel):
    """Result of integrity verification."""
    log_id: int
    integrity_valid: bool
    stored_checksum: str
    computed_checksum: str
    verified_at: datetime = Field(default_factory=datetime.now)
    discrepancy_details: Optional[str] = None
