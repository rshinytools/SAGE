# SAGE API - Data Factory Router
# ================================
"""Data Factory endpoints for file processing and queries with SSE streaming."""

import os
import sys
import hashlib
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Optional, AsyncGenerator
import uuid
import json

from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Depends, Body
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from .auth import get_current_user, get_optional_user

# Import core data modules
try:
    from core.data import (
        UniversalReader, DataFormat, ReadResult,
        SchemaTracker, SchemaDiff, ChangeSeverity,
        FileStore, FileRecord, FileStatus, ProcessingStep,
        DuckDBLoader
    )
    MODULES_AVAILABLE = True
except ImportError as e:
    MODULES_AVAILABLE = False
    import_error = str(e)

router = APIRouter()

# Configuration
DATA_DIR = Path(os.getenv("DATA_DIR", project_root / "data"))
RAW_DIR = DATA_DIR / "raw"
DATABASE_DIR = DATA_DIR / "database"
DATABASE_PATH = DATABASE_DIR / "clinical.duckdb"
KNOWLEDGE_DIR = project_root / "knowledge"

# Initialize components
_reader = None
_schema_tracker = None
_file_store = None
_db_loader = None


def get_reader() -> 'UniversalReader':
    """Get UniversalReader singleton."""
    global _reader
    if _reader is None and MODULES_AVAILABLE:
        _reader = UniversalReader()
    return _reader


def get_schema_tracker() -> 'SchemaTracker':
    """Get SchemaTracker singleton."""
    global _schema_tracker
    if _schema_tracker is None and MODULES_AVAILABLE:
        _schema_tracker = SchemaTracker(str(KNOWLEDGE_DIR / "schema_versions.db"))
    return _schema_tracker


def get_file_store() -> 'FileStore':
    """Get FileStore singleton."""
    global _file_store
    if _file_store is None and MODULES_AVAILABLE:
        _file_store = FileStore(str(KNOWLEDGE_DIR / "file_store.db"))
    return _file_store


def get_db_loader() -> 'DuckDBLoader':
    """Get DuckDBLoader singleton."""
    global _db_loader
    if _db_loader is None and MODULES_AVAILABLE:
        DATABASE_DIR.mkdir(parents=True, exist_ok=True)
        _db_loader = DuckDBLoader(str(DATABASE_PATH))
    return _db_loader


def get_duckdb_connection():
    """Get DuckDB connection from the loader singleton."""
    try:
        loader = get_db_loader()
        if loader and loader._conn:
            return loader._conn
        return None
    except Exception:
        return None


def calculate_file_hash(content: bytes) -> str:
    """Calculate SHA256 hash of file content."""
    return hashlib.sha256(content).hexdigest()


# ============================================
# Pydantic Models
# ============================================

class ProcessRequest(BaseModel):
    """Request to process specific files."""
    files: Optional[List[str]] = None
    block_on_breaking: bool = True


class SchemaCompareRequest(BaseModel):
    """Request to compare schemas."""
    table_name: str


# ============================================
# System Status
# ============================================

@router.get("/status")
async def get_data_factory_status(current_user: dict = Depends(get_current_user)):
    """
    Get Data Factory status and statistics.
    """
    status = {
        "modules_available": MODULES_AVAILABLE,
        "database_exists": DATABASE_PATH.exists(),
        "raw_directory": str(RAW_DIR),
        "database_path": str(DATABASE_PATH)
    }

    if not MODULES_AVAILABLE:
        status["import_error"] = import_error
        return {
            "success": True,
            "data": status,
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    # Get file store statistics
    file_store = get_file_store()
    if file_store:
        status["file_statistics"] = file_store.get_statistics()
        status["table_summary"] = file_store.get_table_summary()

    # Get schema tracker statistics
    schema_tracker = get_schema_tracker()
    if schema_tracker:
        status["schema_tables"] = schema_tracker.list_tables()

    return {
        "success": True,
        "data": status,
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# File Operations
# ============================================

@router.get("/files")
async def list_files(current_user: dict = Depends(get_current_user)):
    """
    List source data files with processing status.
    """
    files = []
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    file_store = get_file_store()

    for filepath in RAW_DIR.glob("*"):
        if filepath.is_file() and filepath.suffix.lower() in ['.sas7bdat', '.xpt', '.csv', '.parquet']:
            stat = filepath.stat()

            # Get status from file store
            status = "pending"
            record = None
            if file_store and MODULES_AVAILABLE:
                # Try to find by filename
                table_name = filepath.stem.upper()
                records = file_store.get_by_table(table_name, include_archived=False)
                for r in records:
                    if r.filename == filepath.name:
                        record = r
                        status = r.status.value
                        break

            files.append({
                "filename": filepath.name,
                "table_name": filepath.stem.upper(),
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "type": filepath.suffix.lower()[1:],
                "status": status,
                "record_id": record.id if record else None,
                "row_count": record.row_count if record else None,
                "column_count": record.column_count if record else None
            })

    return {
        "success": True,
        "data": files,
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(files)}
    }


@router.post("/files/upload")
async def upload_file(
    file: UploadFile = File(...),
    process_immediately: bool = Query(default=False),
    block_on_breaking: bool = Query(default=True),
    current_user: dict = Depends(get_current_user)
):
    """
    Upload a data file.

    Accepts SAS7BDAT, XPT, CSV, or Parquet files.
    Optionally process immediately after upload.
    """
    allowed_extensions = ['.sas7bdat', '.xpt', '.csv', '.parquet']
    file_ext = Path(file.filename).suffix.lower()

    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "VALIDATION_ERROR",
                "message": f"Invalid file type. Allowed: {', '.join(allowed_extensions)}"
            }
        )

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    filepath = RAW_DIR / file.filename

    # Save file
    content = await file.read()
    file_hash = calculate_file_hash(content)

    with open(filepath, 'wb') as f:
        f.write(content)

    # Create file record
    table_name = filepath.stem.upper()
    record_id = str(uuid.uuid4())

    result = {
        "filename": file.filename,
        "table_name": table_name,
        "size": len(content),
        "file_hash": file_hash,
        "uploaded_at": datetime.now().isoformat(),
        "record_id": record_id
    }

    # Store in file store
    if MODULES_AVAILABLE:
        file_store = get_file_store()
        if file_store:
            # Check for duplicate
            existing = file_store.get_by_hash(file_hash)
            if existing:
                result["warning"] = f"Duplicate file detected (matches {existing.filename})"

            record = FileRecord(
                id=record_id,
                filename=file.filename,
                table_name=table_name,
                file_format=file_ext[1:],  # Remove leading dot
                file_size=len(content),
                file_hash=file_hash,
                status=FileStatus.PENDING
            )
            file_store.save(record)

            # Check schema compatibility
            schema_tracker = get_schema_tracker()
            reader = get_reader()
            if schema_tracker and reader:
                try:
                    read_result = reader.read_file(str(filepath))
                    if read_result.success:
                        should_block, diff = schema_tracker.should_block_upload(
                            table_name, read_result.dataframe, block_on_breaking
                        )
                        if diff:
                            result["schema_diff"] = {
                                "has_changes": diff.has_changes,
                                "severity": diff.severity.value,
                                "added_columns": [c.column_name for c in diff.added_columns],
                                "removed_columns": [c.column_name for c in diff.removed_columns],
                                "type_changes": [
                                    {"column": c.column_name, "old": c.old_dtype, "new": c.new_dtype}
                                    for c in diff.type_changes
                                ]
                            }
                            if should_block:
                                result["blocked"] = True
                                result["block_reason"] = "Breaking schema changes detected"
                except Exception as e:
                    result["schema_check_error"] = str(e)

    return {
        "success": True,
        "data": result,
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.delete("/files/{filename}")
async def delete_file(
    filename: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Delete a source file.
    """
    filepath = RAW_DIR / filename

    if not filepath.exists():
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"File not found: {filename}"}
        )

    filepath.unlink()

    return {
        "success": True,
        "data": {"message": f"File deleted: {filename}"},
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# Processing with SSE Streaming
# ============================================

async def process_file_stream(filepath: Path, file_store: 'FileStore',
                               schema_tracker: 'SchemaTracker',
                               reader: 'UniversalReader',
                               db_loader: 'DuckDBLoader',
                               record_id: str,
                               block_on_breaking: bool = True) -> AsyncGenerator[str, None]:
    """
    Process a single file with SSE streaming updates.
    """
    table_name = filepath.stem.upper()

    def send_event(event_type: str, data: dict) -> str:
        return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"

    try:
        # Step 1: Validating
        yield send_event("progress", {
            "step": "validating",
            "message": f"Validating {filepath.name}...",
            "progress": 10
        })

        record = file_store.get(record_id)
        if record:
            record.status = FileStatus.VALIDATING
            step = ProcessingStep(
                step_name="validate",
                status="running",
                started_at=datetime.now().isoformat()
            )
            record.processing_steps.append(step)
            file_store.save(record)

        await asyncio.sleep(0.1)  # Allow event to be sent

        # Step 2: Reading
        yield send_event("progress", {
            "step": "reading",
            "message": f"Reading {filepath.name}...",
            "progress": 25
        })

        if record:
            record.status = FileStatus.READING
            file_store.save(record)

        read_result = reader.read_file(str(filepath))

        if not read_result.success:
            yield send_event("error", {
                "step": "reading",
                "message": f"Failed to read file: {read_result.error}"
            })
            if record:
                record.status = FileStatus.FAILED
                record.error_message = read_result.error
                file_store.save(record)
            return

        df = read_result.dataframe

        yield send_event("progress", {
            "step": "reading",
            "message": f"Read {len(df)} rows, {len(df.columns)} columns",
            "progress": 40,
            "details": {
                "rows": len(df),
                "columns": len(df.columns)
            }
        })

        await asyncio.sleep(0.1)

        # Step 3: Schema check
        yield send_event("progress", {
            "step": "schema_check",
            "message": "Checking schema compatibility...",
            "progress": 50
        })

        should_block, diff = schema_tracker.should_block_upload(table_name, df, block_on_breaking)

        if diff and diff.has_changes:
            yield send_event("schema_change", {
                "table": table_name,
                "severity": diff.severity.value,
                "added_columns": [c.column_name for c in diff.added_columns],
                "removed_columns": [c.column_name for c in diff.removed_columns],
                "changes": [
                    {
                        "column": c.column_name,
                        "type": c.change_type.value,
                        "old": c.old_dtype,
                        "new": c.new_dtype
                    }
                    for c in diff.type_changes
                ]
            })

            if should_block:
                yield send_event("blocked", {
                    "reason": "Breaking schema changes detected",
                    "severity": diff.severity.value
                })
                if record:
                    record.status = FileStatus.FAILED
                    record.error_message = "Blocked: Breaking schema changes"
                    file_store.save(record)
                return

        await asyncio.sleep(0.1)

        # Step 4: Transforming
        yield send_event("progress", {
            "step": "transforming",
            "message": "Applying transformations...",
            "progress": 65
        })

        if record:
            record.status = FileStatus.TRANSFORMING
            file_store.save(record)

        await asyncio.sleep(0.1)

        # Step 5: Loading to DuckDB
        yield send_event("progress", {
            "step": "loading",
            "message": f"Loading to DuckDB as table {table_name}...",
            "progress": 80
        })

        if record:
            record.status = FileStatus.LOADING
            file_store.save(record)

        # Actually load the data
        db_loader.load_dataframe(df, table_name, if_exists='replace')

        # Record schema version
        schema_version = schema_tracker.record_version(
            table_name, df, str(filepath),
            notes=f"Loaded from {filepath.name}"
        )

        yield send_event("progress", {
            "step": "loading",
            "message": f"Table {table_name} loaded successfully",
            "progress": 95
        })

        await asyncio.sleep(0.1)

        # Step 6: Complete
        if record:
            record.status = FileStatus.COMPLETED
            record.processed_at = datetime.now().isoformat()
            record.row_count = len(df)
            record.column_count = len(df.columns)
            record.schema_hash = read_result.schema.schema_hash if read_result.schema else None
            record.schema_version = schema_version.version
            file_store.save(record)

            # Archive previous versions
            file_store.archive_previous(table_name, record.id)

        yield send_event("complete", {
            "table": table_name,
            "rows": len(df),
            "columns": len(df.columns),
            "schema_version": schema_version.version,
            "progress": 100
        })

    except Exception as e:
        yield send_event("error", {
            "message": str(e),
            "step": "unknown"
        })
        if record:
            record.status = FileStatus.FAILED
            record.error_message = str(e)
            file_store.save(record)


@router.get("/process/stream/{filename}")
async def process_file_with_stream(
    filename: str,
    block_on_breaking: bool = Query(default=True),
    token: Optional[str] = Query(default=None, description="Auth token for SSE"),
    current_user: Optional[dict] = Depends(get_optional_user)
):
    """
    Process a single file with SSE streaming progress updates.

    Returns Server-Sent Events stream with progress updates.
    Accepts auth via Bearer header OR token query parameter (for SSE).
    """
    # Handle auth - check header first, then query param
    from .auth import decode_token

    if not current_user and token:
        current_user = decode_token(token)

    if not current_user:
        raise HTTPException(
            status_code=401,
            detail={"code": "AUTH_REQUIRED", "message": "Authentication required"}
        )

    if not MODULES_AVAILABLE:
        raise HTTPException(
            status_code=500,
            detail={"code": "MODULES_UNAVAILABLE", "message": f"Core modules not available: {import_error}"}
        )

    filepath = RAW_DIR / filename
    if not filepath.exists():
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"File not found: {filename}"}
        )

    # Get or create file record
    file_store = get_file_store()
    table_name = filepath.stem.upper()
    records = file_store.get_by_table(table_name, include_archived=False)

    record_id = None
    for r in records:
        if r.filename == filename:
            record_id = r.id
            break

    if not record_id:
        # Create new record
        with open(filepath, 'rb') as f:
            content = f.read()
        file_hash = calculate_file_hash(content)
        record_id = str(uuid.uuid4())

        record = FileRecord(
            id=record_id,
            filename=filename,
            table_name=table_name,
            file_format=filepath.suffix.lower()[1:],
            file_size=filepath.stat().st_size,
            file_hash=file_hash,
            status=FileStatus.PENDING
        )
        file_store.save(record)

    return StreamingResponse(
        process_file_stream(
            filepath,
            file_store,
            get_schema_tracker(),
            get_reader(),
            get_db_loader(),
            record_id,
            block_on_breaking
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.post("/process")
async def process_files(
    request: ProcessRequest = Body(default=ProcessRequest()),
    current_user: dict = Depends(get_current_user)
):
    """
    Process data files to DuckDB (batch mode).

    If files list is empty, processes all pending files.
    Returns job info for tracking.
    """
    if not MODULES_AVAILABLE:
        raise HTTPException(
            status_code=500,
            detail={"code": "MODULES_UNAVAILABLE", "message": f"Core modules not available: {import_error}"}
        )

    job_id = str(uuid.uuid4())[:8]

    # Get files to process
    file_list = request.files if request.files else []

    if not file_list:
        # Get all pending files from raw directory
        for filepath in RAW_DIR.glob("*"):
            if filepath.is_file() and filepath.suffix.lower() in ['.sas7bdat', '.xpt', '.csv', '.parquet']:
                file_list.append(filepath.name)

    results = []
    reader = get_reader()
    schema_tracker = get_schema_tracker()
    file_store = get_file_store()
    db_loader = get_db_loader()

    for filename in file_list:
        filepath = RAW_DIR / filename
        if not filepath.exists():
            results.append({
                "filename": filename,
                "status": "error",
                "error": "File not found"
            })
            continue

        try:
            table_name = filepath.stem.upper()

            # Read file
            read_result = reader.read_file(str(filepath))
            if not read_result.success:
                results.append({
                    "filename": filename,
                    "table_name": table_name,
                    "status": "error",
                    "error": read_result.error
                })
                continue

            df = read_result.dataframe

            # Check schema
            should_block, diff = schema_tracker.should_block_upload(
                table_name, df, request.block_on_breaking
            )

            if should_block:
                results.append({
                    "filename": filename,
                    "table_name": table_name,
                    "status": "blocked",
                    "reason": "Breaking schema changes",
                    "schema_diff": {
                        "severity": diff.severity.value,
                        "removed_columns": diff.removed_columns
                    }
                })
                continue

            # Load to DuckDB
            db_loader.load_dataframe(df, table_name, if_exists='replace')

            # Record schema version
            schema_version = schema_tracker.record_version(
                table_name, df, str(filepath)
            )

            # Update file store
            with open(filepath, 'rb') as f:
                file_hash = calculate_file_hash(f.read())

            record = FileRecord(
                id=str(uuid.uuid4()),
                filename=filename,
                table_name=table_name,
                file_format=filepath.suffix.lower()[1:],
                file_size=filepath.stat().st_size,
                file_hash=file_hash,
                status=FileStatus.COMPLETED,
                processed_at=datetime.now().isoformat(),
                row_count=len(df),
                column_count=len(df.columns),
                schema_version=schema_version.version
            )
            file_store.save(record)
            file_store.archive_previous(table_name, record.id)

            results.append({
                "filename": filename,
                "table_name": table_name,
                "status": "completed",
                "rows": len(df),
                "columns": len(df.columns),
                "schema_version": schema_version.version
            })

        except Exception as e:
            results.append({
                "filename": filename,
                "status": "error",
                "error": str(e)
            })

    completed = len([r for r in results if r.get("status") == "completed"])
    failed = len([r for r in results if r.get("status") in ["error", "blocked"]])

    return {
        "success": True,
        "data": {
            "job_id": job_id,
            "status": "completed",
            "total": len(results),
            "completed": completed,
            "failed": failed,
            "results": results
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.get("/process/{job_id}")
async def get_processing_status(
    job_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get processing job status.
    """
    # For streaming jobs, status is tracked via SSE
    # This endpoint is for backwards compatibility
    return {
        "success": True,
        "data": {
            "job_id": job_id,
            "status": "completed",
            "progress": 100.0,
            "message": "Use /process/stream/{filename} for real-time progress"
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# Schema Operations
# ============================================

@router.get("/schema/versions/{table}")
async def get_schema_versions(
    table: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get schema version history for a table.
    """
    if not MODULES_AVAILABLE:
        raise HTTPException(
            status_code=500,
            detail={"code": "MODULES_UNAVAILABLE", "message": "Core modules not available"}
        )

    schema_tracker = get_schema_tracker()
    versions = schema_tracker.get_version_history(table.upper())

    return {
        "success": True,
        "data": {
            "table": table.upper(),
            "versions": [
                {
                    "version": v.version,
                    "schema_hash": v.schema_hash,
                    "column_count": v.column_count,
                    "source_file": v.source_file,
                    "created_at": v.created_at,
                    "is_current": v.is_current,
                    "columns": v.columns
                }
                for v in versions
            ]
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


@router.post("/schema/compare")
async def compare_schema(
    request: SchemaCompareRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Compare current schema with previous version.
    """
    if not MODULES_AVAILABLE:
        raise HTTPException(
            status_code=500,
            detail={"code": "MODULES_UNAVAILABLE", "message": "Core modules not available"}
        )

    schema_tracker = get_schema_tracker()
    current = schema_tracker.get_current_version(request.table_name.upper())

    if not current:
        return {
            "success": True,
            "data": {
                "table": request.table_name.upper(),
                "has_previous": False,
                "message": "No schema history for this table"
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    versions = schema_tracker.get_version_history(request.table_name.upper())

    return {
        "success": True,
        "data": {
            "table": request.table_name.upper(),
            "current_version": current.version,
            "total_versions": len(versions),
            "current_schema": {
                "columns": current.columns,
                "column_count": current.column_count,
                "schema_hash": current.schema_hash
            }
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


class SchemaRollbackRequest(BaseModel):
    table_name: str
    target_version: int


@router.post("/schema/rollback")
async def rollback_schema(
    request: SchemaRollbackRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Rollback a table's schema to a previous version.

    This updates schema metadata only - it marks the target version as current.
    Requires admin role.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    if not MODULES_AVAILABLE:
        raise HTTPException(
            status_code=500,
            detail={"code": "MODULES_UNAVAILABLE", "message": "Core modules not available"}
        )

    schema_tracker = get_schema_tracker()
    table_name = request.table_name.upper()

    # Get current version before rollback
    current = schema_tracker.get_current_version(table_name)
    current_version = current.version if current else None

    # Perform rollback
    success, message = schema_tracker.rollback_to_version(table_name, request.target_version)

    if not success:
        raise HTTPException(
            status_code=400,
            detail={"code": "ROLLBACK_FAILED", "message": message}
        )

    return {
        "success": True,
        "data": {
            "table": table_name,
            "previous_version": current_version,
            "current_version": request.target_version,
            "message": message
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# File History
# ============================================

@router.get("/history")
async def get_file_history(
    table: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=500),
    current_user: dict = Depends(get_current_user)
):
    """
    Get file processing history.
    """
    if not MODULES_AVAILABLE:
        raise HTTPException(
            status_code=500,
            detail={"code": "MODULES_UNAVAILABLE", "message": "Core modules not available"}
        )

    file_store = get_file_store()

    if table:
        records = file_store.get_by_table(table.upper(), include_archived=True)
    elif status:
        try:
            status_enum = FileStatus(status)
            records = file_store.list_all(status=status_enum, limit=limit)
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail={"code": "VALIDATION_ERROR", "message": f"Invalid status: {status}"}
            )
    else:
        records = file_store.list_all(limit=limit)

    return {
        "success": True,
        "data": [r.to_dict() for r in records],
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(records)}
    }


@router.get("/history/{record_id}")
async def get_file_record(
    record_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get a specific file record by ID.
    """
    if not MODULES_AVAILABLE:
        raise HTTPException(
            status_code=500,
            detail={"code": "MODULES_UNAVAILABLE", "message": "Core modules not available"}
        )

    file_store = get_file_store()
    record = file_store.get(record_id)

    if not record:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Record not found: {record_id}"}
        )

    return {
        "success": True,
        "data": record.to_dict(),
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# Database Operations
# ============================================

@router.get("/tables")
async def list_tables(current_user: dict = Depends(get_current_user)):
    """
    List loaded database tables.
    """
    conn = get_duckdb_connection()
    tables = []

    if conn:
        try:
            result = conn.execute("SHOW TABLES").fetchall()
            for row in result:
                table_name = row[0]
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                cols_result = conn.execute(f"DESCRIBE {table_name}").fetchall()

                # Get schema version if available
                schema_version = None
                if MODULES_AVAILABLE:
                    schema_tracker = get_schema_tracker()
                    current = schema_tracker.get_current_version(table_name)
                    if current:
                        schema_version = current.version

                tables.append({
                    "name": table_name,
                    "rows": count_result[0] if count_result else 0,
                    "columns": len(cols_result),
                    "schema_version": schema_version,
                    "loaded_at": datetime.now().isoformat()
                })
            # Connection managed by singleton - don't close
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={"code": "INTERNAL_ERROR", "message": str(e)}
            )

    return {
        "success": True,
        "data": tables,
        "meta": {"timestamp": datetime.now().isoformat(), "count": len(tables)}
    }


@router.get("/tables/{table}")
async def get_table_info(
    table: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get detailed information about a table.
    """
    conn = get_duckdb_connection()
    if not conn:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Database not found"}
        )

    try:
        # Check if table exists
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        if table not in tables:
            # Connection managed by singleton - don't close
            raise HTTPException(
                status_code=404,
                detail={"code": "NOT_FOUND", "message": f"Table not found: {table}"}
            )

        # Get columns
        columns = []
        for row in conn.execute(f"DESCRIBE {table}").fetchall():
            columns.append({
                "name": row[0],
                "type": row[1],
                "nullable": True
            })

        # Get row count
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        # Get sample
        sample = conn.execute(f"SELECT * FROM {table} LIMIT 5").fetchall()

        # Connection managed by singleton - don't close

        # Get schema history
        schema_info = None
        if MODULES_AVAILABLE:
            schema_tracker = get_schema_tracker()
            current = schema_tracker.get_current_version(table)
            if current:
                schema_info = {
                    "version": current.version,
                    "schema_hash": current.schema_hash,
                    "source_file": current.source_file,
                    "created_at": current.created_at
                }

        return {
            "success": True,
            "data": {
                "name": table,
                "columns": columns,
                "row_count": count,
                "sample": sample,
                "schema_info": schema_info
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.get("/tables/{table}/schema")
async def get_table_schema(
    table: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get table schema.
    """
    conn = get_duckdb_connection()
    if not conn:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Database not found"}
        )

    try:
        columns = []
        for row in conn.execute(f"DESCRIBE {table}").fetchall():
            columns.append({
                "name": row[0],
                "type": row[1],
                "nullable": True
            })
        # Connection managed by singleton - don't close

        return {
            "success": True,
            "data": {"name": table, "columns": columns},
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.get("/tables/{table}/preview")
async def preview_table(
    table: str,
    limit: int = Query(default=100, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(get_current_user)
):
    """
    Preview table data.
    """
    conn = get_duckdb_connection()
    if not conn:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Database not found"}
        )

    try:
        result = conn.execute(f"SELECT * FROM {table} LIMIT {limit} OFFSET {offset}").fetchall()
        columns = [desc[0] for desc in conn.description]
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        # Connection managed by singleton - don't close

        return {
            "success": True,
            "data": {
                "columns": columns,
                "data": result,
                "total": total
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.post("/query")
async def execute_query(
    sql: str = Body(..., embed=True),
    limit: int = Query(default=1000, ge=1, le=100000),
    current_user: dict = Depends(get_current_user)
):
    """
    Execute a SQL query.

    Only SELECT queries are allowed for safety.
    """
    # Security check - only allow SELECT
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith("SELECT"):
        raise HTTPException(
            status_code=422,
            detail={"code": "VALIDATION_ERROR", "message": "Only SELECT queries are allowed"}
        )

    # Block dangerous keywords
    dangerous = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE"]
    for keyword in dangerous:
        if keyword in sql_upper:
            raise HTTPException(
                status_code=422,
                detail={"code": "VALIDATION_ERROR", "message": f"Forbidden keyword: {keyword}"}
            )

    conn = get_duckdb_connection()
    if not conn:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": "Database not found"}
        )

    try:
        import time
        start = time.time()

        # Add LIMIT if not present
        if "LIMIT" not in sql_upper:
            sql = f"{sql} LIMIT {limit}"

        result = conn.execute(sql).fetchall()
        columns = [desc[0] for desc in conn.description]
        execution_time = time.time() - start

        # Connection managed by singleton - don't close

        return {
            "success": True,
            "data": {
                "columns": columns,
                "data": result,
                "row_count": len(result),
                "execution_time": round(execution_time, 3)
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.delete("/tables/{table}")
async def drop_table(
    table: str,
    delete_history: bool = Query(default=False, description="Also delete schema version history"),
    current_user: dict = Depends(get_current_user)
):
    """
    Drop a table from the database.

    Requires admin role.
    """
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "Admin role required"}
        )

    table_name = table.upper()
    db_loader = get_db_loader()

    if not db_loader:
        raise HTTPException(
            status_code=500,
            detail={"code": "LOADER_UNAVAILABLE", "message": "Database loader not available"}
        )

    # Check if table exists
    conn = get_duckdb_connection()
    if conn:
        try:
            result = conn.execute("SHOW TABLES").fetchall()
            table_names = [r[0] for r in result]
            if table_name not in table_names:
                raise HTTPException(
                    status_code=404,
                    detail={"code": "NOT_FOUND", "message": f"Table '{table_name}' not found"}
                )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={"code": "INTERNAL_ERROR", "message": str(e)}
            )

    # Drop the table
    success = db_loader.drop_table(table_name)

    if not success:
        raise HTTPException(
            status_code=500,
            detail={"code": "DROP_FAILED", "message": f"Failed to drop table '{table_name}'"}
        )

    # Optionally delete schema version history
    if delete_history and MODULES_AVAILABLE:
        schema_tracker = get_schema_tracker()
        if schema_tracker:
            schema_tracker.delete_table_history(table_name)

    return {
        "success": True,
        "data": {
            "message": f"Table '{table_name}' dropped successfully",
            "table": table_name,
            "history_deleted": delete_history
        },
        "meta": {"timestamp": datetime.now().isoformat()}
    }


# ============================================
# Export
# ============================================

@router.post("/tables/{table}/export/parquet")
async def export_table_to_parquet(
    table: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Export a table to Parquet format.

    Returns the Parquet file as a download.
    """
    table_name = table.upper()
    db_loader = get_db_loader()

    if not db_loader:
        raise HTTPException(
            status_code=500,
            detail={"code": "LOADER_UNAVAILABLE", "message": "Database loader not available"}
        )

    # Check if table exists
    conn = get_duckdb_connection()
    if conn:
        try:
            result = conn.execute("SHOW TABLES").fetchall()
            table_names = [r[0] for r in result]
            if table_name not in table_names:
                raise HTTPException(
                    status_code=404,
                    detail={"code": "NOT_FOUND", "message": f"Table '{table_name}' not found"}
                )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={"code": "INTERNAL_ERROR", "message": str(e)}
            )

    # Create processed directory if it doesn't exist
    processed_dir = DATA_DIR / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Generate output path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{table_name}_{timestamp}.parquet"
    output_path = processed_dir / output_filename

    # Export to Parquet
    success = db_loader.export_to_parquet(table_name, str(output_path))

    if not success or not output_path.exists():
        raise HTTPException(
            status_code=500,
            detail={"code": "EXPORT_FAILED", "message": f"Failed to export table '{table_name}' to Parquet"}
        )

    # Return the file as a download
    file_size = output_path.stat().st_size

    def iterfile():
        with open(output_path, mode="rb") as file:
            yield from file

    return StreamingResponse(
        iterfile(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={output_filename}",
            "Content-Length": str(file_size)
        }
    )


# ============================================
# Validation
# ============================================

@router.get("/validation/{table}")
async def get_validation_report(
    table: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get validation report for a table.
    """
    conn = get_duckdb_connection()
    if not conn:
        return {
            "success": True,
            "data": {
                "table": table,
                "quality_score": 0,
                "row_count": 0,
                "issues": [{"type": "error", "message": "Database not found"}]
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    try:
        # Check if table exists
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        if table not in tables:
            # Connection managed by singleton - don't close
            return {
                "success": True,
                "data": {
                    "table": table,
                    "quality_score": 0,
                    "row_count": 0,
                    "issues": [{"type": "error", "message": f"Table not found: {table}"}]
                },
                "meta": {"timestamp": datetime.now().isoformat()}
            }

        # Get row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        # Get columns
        columns = conn.execute(f"DESCRIBE {table}").fetchall()

        # Basic validation checks
        issues = []

        # Check for null counts in each column
        for col in columns:
            col_name = col[0]
            null_count = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col_name} IS NULL"
            ).fetchone()[0]

            if null_count > 0:
                null_pct = (null_count / row_count * 100) if row_count > 0 else 0
                if null_pct > 50:
                    issues.append({
                        "type": "warning",
                        "column": col_name,
                        "message": f"High null rate: {null_pct:.1f}% ({null_count} nulls)"
                    })

        # Connection managed by singleton - don't close

        # Calculate quality score
        quality_score = 100 - (len(issues) * 5)
        quality_score = max(0, min(100, quality_score))

        return {
            "success": True,
            "data": {
                "table": table,
                "quality_score": quality_score,
                "row_count": row_count,
                "column_count": len(columns),
                "issues": issues
            },
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )


@router.post("/validate")
async def run_validation(
    tables: Optional[List[str]] = Query(default=None),
    current_user: dict = Depends(get_current_user)
):
    """
    Run validation on specified tables.
    """
    conn = get_duckdb_connection()
    if not conn:
        return {
            "success": True,
            "data": {"results": [], "error": "Database not found"},
            "meta": {"timestamp": datetime.now().isoformat()}
        }

    try:
        # Get all tables if none specified
        if not tables:
            tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]

        results = []
        for table in tables:
            try:
                row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                columns = conn.execute(f"DESCRIBE {table}").fetchall()

                issues = []
                for col in columns:
                    col_name = col[0]
                    null_count = conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE {col_name} IS NULL"
                    ).fetchone()[0]

                    if null_count > 0 and row_count > 0:
                        null_pct = null_count / row_count * 100
                        if null_pct > 50:
                            issues.append({
                                "type": "warning",
                                "column": col_name,
                                "message": f"High null rate: {null_pct:.1f}%"
                            })

                quality_score = 100 - (len(issues) * 5)
                quality_score = max(0, min(100, quality_score))

                results.append({
                    "table": table,
                    "quality_score": quality_score,
                    "row_count": row_count,
                    "issues": issues
                })
            except Exception as e:
                results.append({
                    "table": table,
                    "quality_score": 0,
                    "issues": [{"type": "error", "message": str(e)}]
                })

        # Connection managed by singleton - don't close

        return {
            "success": True,
            "data": {"results": results},
            "meta": {"timestamp": datetime.now().isoformat()}
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_ERROR", "message": str(e)}
        )
