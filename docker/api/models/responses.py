# SAGE API Response Models
# =========================
"""Pydantic models for API responses."""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Generic, TypeVar
from datetime import datetime

T = TypeVar('T')


# ============================================
# Base Response Models
# ============================================

class MetaInfo(BaseModel):
    """Response metadata."""
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    request_id: Optional[str] = None


class ErrorDetail(BaseModel):
    """Error details."""
    code: str
    message: str
    details: Optional[Dict[str, Any]] = None


class APIResponse(BaseModel):
    """Standard API response wrapper."""
    success: bool = True
    data: Optional[Any] = None
    error: Optional[ErrorDetail] = None
    meta: MetaInfo = Field(default_factory=MetaInfo)


# ============================================
# Authentication Responses
# ============================================

class TokenResponse(BaseModel):
    """Authentication token response."""
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds
    refresh_token: Optional[str] = None


class UserInfo(BaseModel):
    """User information."""
    username: str
    roles: List[str] = []
    last_login: Optional[str] = None


# ============================================
# Data Factory Responses
# ============================================

class FileInfo(BaseModel):
    """Source file information."""
    filename: str
    size: int
    modified: str
    status: str = "pending"


class TableInfo(BaseModel):
    """Database table information."""
    name: str
    rows: int
    columns: int
    loaded_at: str


class TableSchema(BaseModel):
    """Table schema information."""
    name: str
    columns: List[Dict[str, Any]]


class QueryResult(BaseModel):
    """SQL query result."""
    columns: List[str]
    data: List[List[Any]]
    row_count: int
    execution_time: float


class ProcessingJob(BaseModel):
    """Data processing job status."""
    job_id: str
    status: str  # pending, running, completed, failed
    progress: float = 0.0
    files_processed: int = 0
    total_files: int = 0
    errors: List[str] = []


class ValidationResult(BaseModel):
    """Data validation result."""
    table: str
    quality_score: float
    row_count: int
    issues: List[Dict[str, Any]] = []


# ============================================
# Metadata Factory Responses
# ============================================

class DomainSummary(BaseModel):
    """Domain summary."""
    name: str
    label: str
    variable_count: int
    status: str


class DomainDetail(BaseModel):
    """Domain details with variables."""
    name: str
    label: str
    structure: str = ""
    purpose: str = ""
    keys: List[str] = []
    variables: List[Dict[str, Any]] = []
    status: str = "pending"


class VariableDetail(BaseModel):
    """Variable details."""
    name: str
    domain: str
    label: str
    data_type: str
    length: Optional[int] = None
    codelist: Optional[str] = None
    codelist_values: List[Dict[str, str]] = []
    origin: Optional[str] = None
    core: Optional[str] = None
    description: Optional[str] = None
    derivation: Optional[str] = None
    plain_english: Optional[str] = None
    status: str = "pending"


class CodelistSummary(BaseModel):
    """Codelist summary."""
    name: str
    label: str
    value_count: int
    status: str


class CodelistDetail(BaseModel):
    """Codelist details."""
    name: str
    label: str
    data_type: str = "text"
    values: List[Dict[str, str]] = []
    status: str = "pending"


class MetadataStats(BaseModel):
    """Metadata statistics."""
    total_domains: int
    total_variables: int
    total_codelists: int
    approved_domains: int
    approved_variables: int
    approved_codelists: int
    pending_count: int
    approval_percentage: float


class SearchResult(BaseModel):
    """Search result item."""
    type: str  # domain, variable, codelist
    name: str
    domain: Optional[str] = None
    label: str
    match_fields: List[str] = []


class VersionInfo(BaseModel):
    """Metadata version information."""
    version_id: str
    version_number: int
    content_hash: str
    created_at: str
    created_by: str
    comment: Optional[str] = None


class DiffResult(BaseModel):
    """Version diff result."""
    added: List[Dict[str, Any]] = []
    modified: List[Dict[str, Any]] = []
    deleted: List[Dict[str, Any]] = []
    unchanged_count: int = 0


class DraftResult(BaseModel):
    """LLM draft result."""
    plain_english: str
    confidence: float
    model_used: str


# ============================================
# Project Tracker Responses
# ============================================

class TrackerSummary(BaseModel):
    """Project tracker summary."""
    total_progress: float
    phases_total: int
    phases_complete: int
    tasks_total: int
    tasks_complete: int


class PhaseInfo(BaseModel):
    """Phase information."""
    id: int
    name: str
    description: Optional[str] = None
    status: str
    progress_percent: float
    task_count: int = 0


class PhaseDetail(BaseModel):
    """Phase with tasks."""
    id: int
    name: str
    description: Optional[str] = None
    status: str
    progress_percent: float
    tasks: List[Dict[str, Any]] = []


class TaskInfo(BaseModel):
    """Task information."""
    id: int
    phase_id: int
    name: str
    description: Optional[str] = None
    status: str
    priority: str = "medium"
    assignee: Optional[str] = None
    subtask_count: int = 0


class TaskDetail(BaseModel):
    """Task with subtasks."""
    id: int
    phase_id: int
    name: str
    description: Optional[str] = None
    status: str
    priority: str = "medium"
    assignee: Optional[str] = None
    notes: Optional[str] = None
    subtasks: List[Dict[str, Any]] = []
    created_at: Optional[str] = None
    completed_at: Optional[str] = None


class SubtaskInfo(BaseModel):
    """Subtask information."""
    id: int
    task_id: int
    name: str
    status: str


class ActivityEntry(BaseModel):
    """Activity log entry."""
    id: int
    task_id: Optional[int] = None
    phase_id: Optional[int] = None
    action: str
    details: Optional[str] = None
    user: Optional[str] = None
    timestamp: str


# ============================================
# System Responses
# ============================================

class HealthStatus(BaseModel):
    """System health status."""
    status: str  # healthy, degraded, unhealthy
    services: Dict[str, str] = {}


class SystemInfo(BaseModel):
    """System information."""
    version: str
    uptime: str
    platform: str
    python_version: str
    disk_usage: Dict[str, Any] = {}
    memory_usage: Dict[str, Any] = {}


class ServiceStatus(BaseModel):
    """Service status."""
    name: str
    status: str
    uptime: Optional[str] = None
    port: Optional[int] = None


class BackupInfo(BaseModel):
    """Backup information."""
    filename: str
    size: int
    created_at: str


class LogEntry(BaseModel):
    """Log entry."""
    timestamp: str
    level: str
    service: str
    message: str
