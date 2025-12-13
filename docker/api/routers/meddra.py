# SAGE API - MedDRA Router
# =========================
"""MedDRA Library endpoints for term lookup and management."""

import os
import sys
import shutil
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging

from fastapi import APIRouter, HTTPException, Query, UploadFile, File, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from dataclasses import asdict

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from .auth import get_current_user, get_optional_user

# Import core MedDRA modules
try:
    from core.meddra import (
        MedDRALoader,
        MedDRALookup,
        MedDRAVersion,
        MedDRATerm,
        MedDRAHierarchy,
        SearchResult,
        LookupResult,
    )
    MEDDRA_AVAILABLE = True
except ImportError as e:
    MEDDRA_AVAILABLE = False
    import_error = str(e)

logger = logging.getLogger(__name__)

router = APIRouter()

# Configuration
DATA_DIR = Path(os.getenv("DATA_DIR", project_root / "data"))
DATABASE_PATH = DATA_DIR / "database" / "clinical.duckdb"
KNOWLEDGE_DIR = Path(os.getenv("KNOWLEDGE_DIR", project_root / "knowledge"))
UPLOAD_DIR = DATA_DIR / "uploads" / "meddra"

# Ensure directories exist
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# State
_loading_in_progress: bool = False


# ============================================================================
# Pydantic Models
# ============================================================================

class MedDRAStatusResponse(BaseModel):
    """MedDRA status response."""
    available: bool
    current_version: Optional[Dict[str, Any]]
    loading_in_progress: bool


class MedDRATermResponse(BaseModel):
    """MedDRA term response."""
    code: str
    name: str
    level: str
    parent_code: Optional[str] = None
    parent_name: Optional[str] = None


class MedDRAHierarchyResponse(BaseModel):
    """MedDRA hierarchy response."""
    soc: MedDRATermResponse
    hlgt: MedDRATermResponse
    hlt: MedDRATermResponse
    pt: MedDRATermResponse
    llt: Optional[MedDRATermResponse] = None


class MedDRASearchResultResponse(BaseModel):
    """MedDRA search result."""
    term: MedDRATermResponse
    match_score: float
    hierarchy: MedDRAHierarchyResponse


class MedDRASearchResponse(BaseModel):
    """MedDRA search response."""
    query: str
    level_filter: Optional[str]
    count: int
    results: List[MedDRASearchResultResponse]


class MedDRALookupResponse(BaseModel):
    """MedDRA lookup response."""
    found: bool
    query: str
    exact_match: Optional[MedDRATermResponse]
    hierarchy: Optional[MedDRAHierarchyResponse]
    related_terms: List[MedDRATermResponse]
    message: str


# ============================================================================
# Helper Functions
# ============================================================================

def get_loader() -> MedDRALoader:
    """Get MedDRA loader instance."""
    return MedDRALoader(str(DATABASE_PATH), str(KNOWLEDGE_DIR))


def get_lookup() -> MedDRALookup:
    """Get MedDRA lookup instance."""
    return MedDRALookup(str(DATABASE_PATH))


def term_to_response(term: MedDRATerm) -> MedDRATermResponse:
    """Convert MedDRATerm to response model."""
    return MedDRATermResponse(
        code=term.code,
        name=term.name,
        level=term.level,
        parent_code=term.parent_code,
        parent_name=term.parent_name
    )


def hierarchy_to_response(hierarchy: MedDRAHierarchy) -> MedDRAHierarchyResponse:
    """Convert MedDRAHierarchy to response model."""
    return MedDRAHierarchyResponse(
        soc=term_to_response(hierarchy.soc),
        hlgt=term_to_response(hierarchy.hlgt),
        hlt=term_to_response(hierarchy.hlt),
        pt=term_to_response(hierarchy.pt),
        llt=term_to_response(hierarchy.llt) if hierarchy.llt else None
    )


async def load_meddra_file(file_path: str):
    """Load MedDRA file in background."""
    global _loading_in_progress

    try:
        _loading_in_progress = True
        loader = get_loader()

        # Run in thread pool to not block
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, loader.load_from_sas, file_path)

        logger.info(f"MedDRA loaded successfully from {file_path}")

    except Exception as e:
        logger.error(f"Failed to load MedDRA: {e}")
        raise
    finally:
        _loading_in_progress = False


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/status")
async def get_status() -> MedDRAStatusResponse:
    """Get MedDRA status."""
    if not MEDDRA_AVAILABLE:
        return MedDRAStatusResponse(
            available=False,
            current_version=None,
            loading_in_progress=False
        )

    loader = get_loader()
    version = loader.get_status()

    return MedDRAStatusResponse(
        available=loader.is_available(),
        current_version=asdict(version) if version else None,
        loading_in_progress=_loading_in_progress
    )


@router.post("/preview")
async def preview_meddra_file(
    file: UploadFile = File(...)
) -> Dict[str, Any]:
    """
    Preview a MedDRA SAS7BDAT file structure without loading it.

    Returns column names and sample data to verify format before loading.
    """
    import pandas as pd

    if not file.filename.endswith('.sas7bdat'):
        raise HTTPException(
            status_code=400,
            detail="File must be a SAS7BDAT file"
        )

    # Save temp file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    temp_path = UPLOAD_DIR / f"preview_{timestamp}.sas7bdat"

    try:
        with open(temp_path, 'wb') as f:
            content = await file.read()
            f.write(content)

        # Read file
        try:
            df = pd.read_sas(temp_path, encoding='latin1')
        except Exception:
            df = pd.read_sas(temp_path, encoding='utf-8')

        # Get column info
        columns = list(df.columns)
        columns_lower = [c.lower() for c in columns]

        # Check for expected columns
        expected_columns = {
            'soc_code': ['soc_code', 'soc_cd', 'soccd', 'primary_soc_cd'],
            'soc_name': ['soc_name', 'soc_nm', 'socname', 'primary_soc_name'],
            'hlgt_code': ['hlgt_code', 'hlgt_cd', 'hlgtcd'],
            'hlgt_name': ['hlgt_name', 'hlgt_nm', 'hlgtname'],
            'hlt_code': ['hlt_code', 'hlt_cd', 'hltcd'],
            'hlt_name': ['hlt_name', 'hlt_nm', 'hltname'],
            'pt_code': ['pt_code', 'pt_cd', 'ptcd', 'meddra_pt_cd'],
            'pt_name': ['pt_name', 'pt_nm', 'ptname', 'meddra_pt_name'],
            'llt_code': ['llt_code', 'llt_cd', 'lltcd'],
            'llt_name': ['llt_name', 'llt_nm', 'lltname'],
        }

        found_mappings = {}
        missing_required = []

        for expected, variants in expected_columns.items():
            found = None
            for variant in variants:
                if variant in columns_lower:
                    found = columns[columns_lower.index(variant)]
                    break
            found_mappings[expected] = found
            if found is None and expected not in ['llt_code', 'llt_name']:
                missing_required.append(expected)

        # Get sample data
        sample_data = df.head(5).to_dict(orient='records')

        return {
            "filename": file.filename,
            "rows": len(df),
            "columns": columns,
            "column_mappings": found_mappings,
            "missing_required": missing_required,
            "can_load": len(missing_required) == 0,
            "sample_data": sample_data,
            "message": "File can be loaded!" if len(missing_required) == 0 else f"Missing required columns: {', '.join(missing_required)}"
        }

    finally:
        # Clean up temp file
        if temp_path.exists():
            temp_path.unlink()


@router.post("/upload")
async def upload_meddra(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None
) -> Dict[str, Any]:
    """Upload and load MedDRA SAS7BDAT file."""
    if not MEDDRA_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="MedDRA module not available"
        )

    if _loading_in_progress:
        raise HTTPException(
            status_code=409,
            detail="MedDRA loading already in progress"
        )

    if not file.filename.endswith('.sas7bdat'):
        raise HTTPException(
            status_code=400,
            detail="File must be a SAS7BDAT file"
        )

    # Save uploaded file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = UPLOAD_DIR / f"meddra_{timestamp}.sas7bdat"

    try:
        with open(file_path, 'wb') as f:
            content = await file.read()
            f.write(content)

        # Start loading in background
        background_tasks.add_task(load_meddra_file, str(file_path))

        return {
            "success": True,
            "message": "MedDRA file uploaded. Loading in background...",
            "file_path": str(file_path)
        }

    except Exception as e:
        # Clean up on error
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload file: {str(e)}"
        )


@router.delete("/version")
async def delete_version() -> Dict[str, Any]:
    """Delete current MedDRA version."""
    if not MEDDRA_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="MedDRA module not available"
        )

    loader = get_loader()

    if not loader.is_available():
        raise HTTPException(
            status_code=404,
            detail="No MedDRA version loaded"
        )

    loader.delete_version()

    return {
        "success": True,
        "message": "MedDRA version deleted"
    }


@router.get("/search")
async def search_terms(
    query: str = Query(..., min_length=1),
    level: Optional[str] = Query(None, regex="^(SOC|HLGT|HLT|PT|LLT)$"),
    limit: int = Query(20, ge=1, le=100)
) -> MedDRASearchResponse:
    """Search MedDRA terms."""
    if not MEDDRA_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="MedDRA module not available"
        )

    lookup = get_lookup()

    if not get_loader().is_available():
        raise HTTPException(
            status_code=404,
            detail="MedDRA not loaded. Upload a MedDRA dictionary first."
        )

    results = lookup.search(query, level, limit)

    return MedDRASearchResponse(
        query=query,
        level_filter=level,
        count=len(results),
        results=[
            MedDRASearchResultResponse(
                term=term_to_response(r.term),
                match_score=r.match_score,
                hierarchy=hierarchy_to_response(r.hierarchy) if r.hierarchy else None
            )
            for r in results
        ]
    )


@router.get("/lookup")
async def lookup_term(
    term: str = Query(..., min_length=1)
) -> MedDRALookupResponse:
    """
    Look up a term in MedDRA.

    Handles exact matches, abbreviations (MI, HTN), and returns suggestions.
    """
    if not MEDDRA_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="MedDRA module not available"
        )

    lookup = get_lookup()

    if not get_loader().is_available():
        raise HTTPException(
            status_code=404,
            detail="MedDRA not loaded. Upload a MedDRA dictionary first."
        )

    result = lookup.lookup(term)

    return MedDRALookupResponse(
        found=result.found,
        query=result.query,
        exact_match=term_to_response(result.exact_match) if result.exact_match else None,
        hierarchy=hierarchy_to_response(result.hierarchy) if result.hierarchy else None,
        related_terms=[term_to_response(t) for t in result.related_terms],
        message=result.message
    )


@router.get("/term/{code}")
async def get_term_by_code(code: str) -> Dict[str, Any]:
    """Get term by MedDRA code."""
    if not MEDDRA_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="MedDRA module not available"
        )

    lookup = get_lookup()

    if not get_loader().is_available():
        raise HTTPException(
            status_code=404,
            detail="MedDRA not loaded"
        )

    term, hierarchy = lookup.get_term_by_code(code)

    if not term:
        raise HTTPException(
            status_code=404,
            detail=f"Term with code '{code}' not found"
        )

    return {
        "term": term_to_response(term),
        "hierarchy": hierarchy_to_response(hierarchy) if hierarchy else None
    }


@router.get("/term/{code}/children")
async def get_term_children(code: str) -> Dict[str, Any]:
    """Get children of a term."""
    if not MEDDRA_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="MedDRA module not available"
        )

    lookup = get_lookup()

    if not get_loader().is_available():
        raise HTTPException(
            status_code=404,
            detail="MedDRA not loaded"
        )

    parent, children = lookup.get_children(code)

    if not parent:
        raise HTTPException(
            status_code=404,
            detail=f"Term with code '{code}' not found"
        )

    return {
        "parent": term_to_response(parent),
        "children": [term_to_response(c) for c in children]
    }


@router.get("/browse/soc")
async def browse_socs() -> Dict[str, Any]:
    """Get all System Organ Classes."""
    if not MEDDRA_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="MedDRA module not available"
        )

    lookup = get_lookup()

    if not get_loader().is_available():
        raise HTTPException(
            status_code=404,
            detail="MedDRA not loaded"
        )

    socs = lookup.get_all_socs()

    return {
        "socs": [term_to_response(s) for s in socs]
    }


@router.get("/browse/soc/{soc_code}")
async def browse_soc_children(soc_code: str) -> Dict[str, Any]:
    """Get children of a specific SOC."""
    if not MEDDRA_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="MedDRA module not available"
        )

    lookup = get_lookup()

    if not get_loader().is_available():
        raise HTTPException(
            status_code=404,
            detail="MedDRA not loaded"
        )

    parent, children = lookup.get_children(soc_code)

    if not parent:
        raise HTTPException(
            status_code=404,
            detail=f"SOC with code '{soc_code}' not found"
        )

    return {
        "soc": term_to_response(parent),
        "children": [term_to_response(c) for c in children]
    }


@router.get("/browse/soc/{soc_code}/pts")
async def browse_soc_pts(soc_code: str) -> Dict[str, Any]:
    """
    Get all Preferred Terms (PTs) under a SOC.
    Simplified hierarchy view: SOC → PT (skipping HLGT/HLT).
    """
    if not MEDDRA_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="MedDRA module not available"
        )

    lookup = get_lookup()

    if not get_loader().is_available():
        raise HTTPException(
            status_code=404,
            detail="MedDRA not loaded"
        )

    soc, pts = lookup.get_pts_by_soc(soc_code)

    if not soc:
        raise HTTPException(
            status_code=404,
            detail=f"SOC with code '{soc_code}' not found"
        )

    return {
        "soc": term_to_response(soc),
        "pt_count": len(pts),
        "pts": [term_to_response(pt) for pt in pts]
    }


@router.get("/browse/pt/{pt_code}/llts")
async def browse_pt_llts(pt_code: str) -> Dict[str, Any]:
    """
    Get all Lowest Level Terms (LLTs) under a PT.
    """
    if not MEDDRA_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="MedDRA module not available"
        )

    lookup = get_lookup()

    if not get_loader().is_available():
        raise HTTPException(
            status_code=404,
            detail="MedDRA not loaded"
        )

    pt, llts = lookup.get_llts_by_pt(pt_code)

    if not pt:
        raise HTTPException(
            status_code=404,
            detail=f"PT with code '{pt_code}' not found"
        )

    return {
        "pt": term_to_response(pt),
        "llt_count": len(llts),
        "llts": [term_to_response(llt) for llt in llts]
    }


@router.get("/statistics")
async def get_statistics() -> Dict[str, Any]:
    """Get MedDRA statistics."""
    if not MEDDRA_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="MedDRA module not available"
        )

    loader = get_loader()
    version = loader.get_status()

    if not loader.is_available() or not version:
        raise HTTPException(
            status_code=404,
            detail="MedDRA not loaded"
        )

    lookup = get_lookup()
    stats = lookup.get_statistics()

    return {
        "version": version.version,
        **stats
    }
