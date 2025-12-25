# SAGE API - Audit Logs Router
# =============================
"""
Audit log endpoints for 21 CFR Part 11 compliance.

Provides:
- Audit log search and retrieval
- Statistics and reporting
- Excel/PDF/CSV export
- Integrity verification
- Electronic signatures
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import FileResponse

# Add project root to path
project_root = Path(os.environ.get('APP_ROOT', '/app'))
sys.path.insert(0, str(project_root))

from .auth import get_current_user

# Import audit service
try:
    from core.audit import (
        get_audit_service,
        AuditFilters,
        AuditLog,
        AuditStatistics,
        QueryAuditDetails,
        IntegrityCheckResult,
        ElectronicSignature,
    )
    AUDIT_AVAILABLE = True
except ImportError as e:
    AUDIT_AVAILABLE = False
    print(f"Warning: Audit module not available: {e}")

router = APIRouter()


# ============================================================================
# Response Models
# ============================================================================

class AuditLogResponse(BaseModel):
    """Response for a single audit log."""
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
    duration_ms: Optional[int] = None
    error_message: Optional[str] = None
    checksum: str
    created_at: datetime
    details: Optional[dict] = None


class AuditLogDetailResponse(AuditLogResponse):
    """Response for audit log with query details."""
    query_details: Optional[dict] = None
    signatures: Optional[List[dict]] = None


class AuditLogsListResponse(BaseModel):
    """Response for paginated audit logs."""
    logs: List[AuditLogResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class AuditStatisticsResponse(BaseModel):
    """Response for audit statistics."""
    total_events: int
    by_action: dict
    by_status: dict
    by_user: dict
    by_resource_type: dict
    average_query_confidence: Optional[float] = None
    average_duration_ms: Optional[float] = None
    date_range: Optional[dict] = None


class IntegrityCheckResponse(BaseModel):
    """Response for integrity verification."""
    log_id: int
    integrity_valid: bool
    stored_checksum: str
    computed_checksum: str
    verified_at: datetime
    discrepancy_details: Optional[str] = None


class SignatureRequest(BaseModel):
    """Request to add electronic signature."""
    meaning: str = Field(..., description="Signature meaning: Reviewed, Approved, or Submitted")


class SignatureResponse(BaseModel):
    """Response for added signature."""
    signature_id: int
    log_id: int
    signer_username: str
    meaning: str
    timestamp: datetime


class QueryDetailsResponse(BaseModel):
    """Response for query audit details."""
    original_question: Optional[str] = None
    sanitized_question: Optional[str] = None
    intent_classification: Optional[str] = None
    matched_entities: Optional[List[dict]] = None
    generated_sql: Optional[str] = None
    llm_prompt: Optional[str] = None
    llm_response: Optional[str] = None
    llm_model: Optional[str] = None
    llm_tokens_used: Optional[int] = None
    confidence_score: Optional[float] = None
    confidence_breakdown: Optional[dict] = None
    execution_time_ms: Optional[int] = None
    result_row_count: Optional[int] = None
    tables_accessed: Optional[List[str]] = None
    columns_used: Optional[List[str]] = None


# ============================================================================
# Helper Functions
# ============================================================================

def check_audit_available():
    """Check if audit module is available."""
    if not AUDIT_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Audit logging module is not available"
        )


def log_to_response(log: AuditLog) -> AuditLogResponse:
    """Convert AuditLog to response model."""
    return AuditLogResponse(
        id=log.id,
        timestamp=log.timestamp,
        user_id=log.user_id,
        username=log.username,
        action=log.action,
        resource_type=log.resource_type,
        resource_id=log.resource_id,
        status=log.status,
        ip_address=log.ip_address,
        user_agent=log.user_agent,
        request_method=log.request_method,
        request_path=log.request_path,
        request_body=log.request_body,
        response_status=log.response_status,
        duration_ms=log.duration_ms,
        error_message=log.error_message,
        checksum=log.checksum,
        created_at=log.created_at,
        details=log.details,
    )


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/logs", response_model=AuditLogsListResponse)
async def get_audit_logs(
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    username: Optional[str] = Query(None, description="Filter by username (partial match)"),
    action: Optional[str] = Query(None, description="Filter by action type"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    status: Optional[str] = Query(None, description="Filter by status (success, failure, error)"),
    start_date: Optional[datetime] = Query(None, description="Filter by start date"),
    end_date: Optional[datetime] = Query(None, description="Filter by end date"),
    search_text: Optional[str] = Query(None, description="Search in paths and error messages"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    current_user: dict = Depends(get_current_user)
):
    """
    Get paginated audit logs with filters.

    Returns a list of audit log entries matching the specified filters.
    """
    check_audit_available()

    filters = AuditFilters(
        user_id=user_id,
        username=username,
        action=action,
        resource_type=resource_type,
        status=status,
        start_date=start_date,
        end_date=end_date,
        search_text=search_text,
        page=page,
        page_size=page_size,
    )

    audit_service = get_audit_service()
    logs, total = audit_service.search_logs(filters)

    total_pages = (total + page_size - 1) // page_size if total > 0 else 1

    return AuditLogsListResponse(
        logs=[log_to_response(log) for log in logs],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@router.get("/logs/{log_id}", response_model=AuditLogDetailResponse)
async def get_audit_log_detail(
    log_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Get a single audit log with full details.

    Includes query details and signatures if available.
    """
    check_audit_available()

    audit_service = get_audit_service()
    log = audit_service.get_log(log_id, include_details=True)

    if not log:
        raise HTTPException(status_code=404, detail=f"Audit log not found: {log_id}")

    response = AuditLogDetailResponse(
        id=log.id,
        timestamp=log.timestamp,
        user_id=log.user_id,
        username=log.username,
        action=log.action,
        resource_type=log.resource_type,
        resource_id=log.resource_id,
        status=log.status,
        ip_address=log.ip_address,
        user_agent=log.user_agent,
        request_method=log.request_method,
        request_path=log.request_path,
        request_body=log.request_body,
        response_status=log.response_status,
        duration_ms=log.duration_ms,
        error_message=log.error_message,
        checksum=log.checksum,
        created_at=log.created_at,
        details=log.details,
        query_details=log.query_details.model_dump() if log.query_details else None,
        signatures=[sig.model_dump() for sig in log.signatures] if log.signatures else None,
    )

    return response


@router.get("/logs/{log_id}/query-details", response_model=QueryDetailsResponse)
async def get_query_details(
    log_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Get detailed query/LLM information for an audit log.

    Returns the full LLM prompt, response, SQL generated, and other details.
    """
    check_audit_available()

    audit_service = get_audit_service()
    details = audit_service.get_query_details(log_id)

    if not details:
        raise HTTPException(status_code=404, detail=f"Query details not found for log: {log_id}")

    return QueryDetailsResponse(
        original_question=details.original_question,
        sanitized_question=details.sanitized_question,
        intent_classification=details.intent_classification,
        matched_entities=details.matched_entities,
        generated_sql=details.generated_sql,
        llm_prompt=details.llm_prompt,
        llm_response=details.llm_response,
        llm_model=details.llm_model,
        llm_tokens_used=details.llm_tokens_used,
        confidence_score=details.confidence_score,
        confidence_breakdown=details.confidence_breakdown,
        execution_time_ms=details.execution_time_ms,
        result_row_count=details.result_row_count,
        tables_accessed=details.tables_accessed,
        columns_used=details.columns_used,
    )


@router.get("/statistics", response_model=AuditStatisticsResponse)
async def get_audit_statistics(
    start_date: Optional[datetime] = Query(None, description="Filter by start date"),
    end_date: Optional[datetime] = Query(None, description="Filter by end date"),
    current_user: dict = Depends(get_current_user)
):
    """
    Get audit statistics for the dashboard.

    Returns counts by action, status, user, and averages.
    """
    check_audit_available()

    audit_service = get_audit_service()
    stats = audit_service.get_statistics(start_date, end_date)

    return AuditStatisticsResponse(
        total_events=stats.total_events,
        by_action=stats.by_action,
        by_status=stats.by_status,
        by_user=stats.by_user,
        by_resource_type=stats.by_resource_type,
        average_query_confidence=stats.average_query_confidence,
        average_duration_ms=stats.average_duration_ms,
        date_range=stats.date_range,
    )


@router.get("/actions", response_model=List[str])
async def get_available_actions(
    current_user: dict = Depends(get_current_user)
):
    """Get list of all available action types."""
    check_audit_available()

    audit_service = get_audit_service()
    return audit_service.get_available_actions()


@router.get("/users")
async def get_available_users(
    current_user: dict = Depends(get_current_user)
):
    """Get list of users who have audit entries."""
    check_audit_available()

    audit_service = get_audit_service()
    return audit_service.get_available_users()


@router.get("/resource-types", response_model=List[str])
async def get_available_resource_types(
    current_user: dict = Depends(get_current_user)
):
    """Get list of all resource types."""
    check_audit_available()

    audit_service = get_audit_service()
    return audit_service.get_available_resource_types()


@router.get("/logs/{log_id}/verify", response_model=IntegrityCheckResponse)
async def verify_audit_integrity(
    log_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Verify the integrity of an audit log record.

    Checks if the record has been tampered with by comparing checksums.
    """
    check_audit_available()

    audit_service = get_audit_service()
    result = audit_service.verify_integrity(log_id)

    return IntegrityCheckResponse(
        log_id=result.log_id,
        integrity_valid=result.integrity_valid,
        stored_checksum=result.stored_checksum,
        computed_checksum=result.computed_checksum,
        verified_at=result.verified_at,
        discrepancy_details=result.discrepancy_details,
    )


@router.post("/logs/{log_id}/signature", response_model=SignatureResponse)
async def add_electronic_signature(
    log_id: int,
    request: SignatureRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Add an electronic signature to an audit record.

    For 21 CFR Part 11 compliance. Valid meanings: Reviewed, Approved, Submitted.
    """
    check_audit_available()

    # Validate meaning
    valid_meanings = {"Reviewed", "Approved", "Submitted"}
    if request.meaning not in valid_meanings:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid signature meaning. Must be one of: {', '.join(valid_meanings)}"
        )

    audit_service = get_audit_service()

    # Verify log exists
    log = audit_service.get_log(log_id, include_details=False)
    if not log:
        raise HTTPException(status_code=404, detail=f"Audit log not found: {log_id}")

    # Add signature
    user_id = current_user.get("sub", "unknown")
    username = current_user.get("sub", "Unknown")
    signature_id = audit_service.add_signature(log_id, user_id, username, request.meaning)

    return SignatureResponse(
        signature_id=signature_id,
        log_id=log_id,
        signer_username=username,
        meaning=request.meaning,
        timestamp=datetime.now(),
    )


# ============================================================================
# Export Endpoints
# ============================================================================

@router.get("/export/excel")
async def export_to_excel(
    user_id: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """
    Export filtered audit logs to Excel file.

    Returns an .xlsx file with formatted audit data.
    """
    check_audit_available()

    filters = AuditFilters(
        user_id=user_id,
        action=action,
        status=status,
        start_date=start_date,
        end_date=end_date,
    )

    audit_service = get_audit_service()

    try:
        file_path = audit_service.export_to_excel(filters)
    except ImportError as e:
        raise HTTPException(
            status_code=501,
            detail="Excel export requires openpyxl. Install with: pip install openpyxl"
        )

    return FileResponse(
        path=file_path,
        filename=f"audit_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@router.get("/export/pdf")
async def export_to_pdf(
    user_id: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """
    Export filtered audit logs to PDF report.

    Returns a formatted PDF report with statistics and log entries.
    """
    check_audit_available()

    filters = AuditFilters(
        user_id=user_id,
        action=action,
        status=status,
        start_date=start_date,
        end_date=end_date,
    )

    audit_service = get_audit_service()

    try:
        file_path = audit_service.export_to_pdf(filters)
    except ImportError as e:
        raise HTTPException(
            status_code=501,
            detail="PDF export requires reportlab. Install with: pip install reportlab"
        )

    return FileResponse(
        path=file_path,
        filename=f"audit_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
        media_type="application/pdf"
    )


@router.get("/export/csv")
async def export_to_csv(
    user_id: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """
    Export filtered audit logs to CSV file.

    Returns a CSV file with audit data.
    """
    check_audit_available()

    filters = AuditFilters(
        user_id=user_id,
        action=action,
        status=status,
        start_date=start_date,
        end_date=end_date,
    )

    audit_service = get_audit_service()
    file_path = audit_service.export_to_csv(filters)

    return FileResponse(
        path=file_path,
        filename=f"audit_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        media_type="text/csv"
    )


@router.get("/export/json")
async def export_to_json(
    user_id: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """
    Export filtered audit logs to JSON file.

    Returns a JSON file with audit data and metadata.
    """
    check_audit_available()

    filters = AuditFilters(
        user_id=user_id,
        action=action,
        status=status,
        start_date=start_date,
        end_date=end_date,
    )

    audit_service = get_audit_service()
    file_path = audit_service.export_to_json(filters)

    return FileResponse(
        path=file_path,
        filename=f"audit_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        media_type="application/json"
    )
