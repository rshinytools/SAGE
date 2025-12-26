# SAGE API Request Models
# ========================
"""Pydantic models for API requests."""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


# ============================================
# Authentication Requests
# ============================================

class LoginRequest(BaseModel):
    """Login request."""
    username: str = Field(..., min_length=1, description="Username")
    password: str = Field(..., min_length=1, description="Password")


class RefreshTokenRequest(BaseModel):
    """Token refresh request."""
    refresh_token: str = Field(..., description="Refresh token")


class PasswordChangeRequest(BaseModel):
    """Password change request."""
    old_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8)


# ============================================
# Data Factory Requests
# ============================================

class ProcessFilesRequest(BaseModel):
    """Request to process data files."""
    files: List[str] = Field(default=[], description="List of filenames to process")
    options: Dict[str, Any] = Field(default={}, description="Processing options")


class SQLQueryRequest(BaseModel):
    """SQL query request."""
    sql: str = Field(..., min_length=1, description="SQL query to execute")
    limit: int = Field(default=1000, ge=1, le=100000, description="Maximum rows to return")


class ValidationRequest(BaseModel):
    """Data validation request."""
    tables: List[str] = Field(default=[], description="Tables to validate")


# ============================================
# Metadata Factory Requests
# ============================================

class DomainUpdateRequest(BaseModel):
    """Domain update request."""
    label: Optional[str] = None
    structure: Optional[str] = None
    purpose: Optional[str] = None
    keys: Optional[List[str]] = None


class VariableUpdateRequest(BaseModel):
    """Variable update request."""
    label: Optional[str] = None
    description: Optional[str] = None
    plain_english: Optional[str] = None
    data_type: Optional[str] = None
    length: Optional[int] = None
    codelist: Optional[str] = None
    origin: Optional[str] = None
    core: Optional[str] = None
    derivation: Optional[str] = None


class ApprovalRequest(BaseModel):
    """Approval/rejection request."""
    comment: Optional[str] = Field(default=None, description="Optional comment")


class CodelistUpdateRequest(BaseModel):
    """Codelist update request."""
    label: Optional[str] = None
    values: Optional[List[Dict[str, str]]] = None


class MetadataSearchRequest(BaseModel):
    """Metadata search request."""
    query: str = Field(..., min_length=1, description="Search query")
    search_type: str = Field(default="all", description="Type: all, domain, variable, codelist")


# ============================================
# System Requests
# ============================================

class ConfigUpdateRequest(BaseModel):
    """Configuration update request."""
    key: str = Field(..., description="Configuration key")
    value: Any = Field(..., description="Configuration value")


class ServiceActionRequest(BaseModel):
    """Service action request."""
    action: str = Field(default="restart", description="Action: restart, stop, start")
