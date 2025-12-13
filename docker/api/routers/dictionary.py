# SAGE API - Dictionary Router
# =============================
"""
Factory 3 Dictionary endpoints for fuzzy matching.

Note: Semantic/embedding search has been intentionally excluded.
Clinical data requires exact terminology matching (MedDRA controlled vocabulary).
Synonym matching is handled via MedDRA hierarchy, not AI embeddings.
"""

import os
import sys
import json
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging

from fastapi import APIRouter, HTTPException, Query, Body, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from .auth import get_current_user, get_optional_user

# Import core dictionary modules
try:
    from core.dictionary import (
        ValueScanner,
        FuzzyMatcher,
        SchemaMapper,
        TermResolver,
        create_term_resolver,
    )
    DICTIONARY_AVAILABLE = True
except ImportError as e:
    DICTIONARY_AVAILABLE = False
    import_error = str(e)

# Import MedDRA modules
try:
    from core.meddra import MedDRALoader, MedDRALookup
    MEDDRA_AVAILABLE = True
except ImportError:
    MEDDRA_AVAILABLE = False

logger = logging.getLogger(__name__)

router = APIRouter()

# Configuration
DATA_DIR = Path(os.getenv("DATA_DIR", project_root / "data"))
DATABASE_PATH = DATA_DIR / "database" / "clinical.duckdb"
KNOWLEDGE_DIR = Path(os.getenv("KNOWLEDGE_DIR", project_root / "knowledge"))
METADATA_PATH = KNOWLEDGE_DIR / "golden_metadata.json"
FUZZY_INDEX_PATH = KNOWLEDGE_DIR / "fuzzy_index.pkl"
SCHEMA_MAP_PATH = KNOWLEDGE_DIR / "schema_map.json"
STATUS_PATH = KNOWLEDGE_DIR / "dictionary_status.json"

# Singleton instances
_fuzzy_matcher: Optional['FuzzyMatcher'] = None
_term_resolver: Optional['TermResolver'] = None
_build_in_progress: bool = False
_build_progress: int = 0  # 0-100 percentage
_build_step: str = ""  # Current step description


# ============================================================================
# Pydantic Models
# ============================================================================

class SearchRequest(BaseModel):
    """Search request model."""
    query: str = Field(..., min_length=1, description="Search query")
    threshold: float = Field(70.0, ge=0, le=100, description="Minimum score threshold")
    limit: int = Field(10, ge=1, le=100, description="Maximum results")
    table_filter: Optional[str] = Field(None, description="Filter by table name")
    column_filter: Optional[str] = Field(None, description="Filter by column name")


class SearchResult(BaseModel):
    """Search result model."""
    value: str
    score: float
    table: str
    column: str
    match_type: str
    id: str


class BuildRequest(BaseModel):
    """Dictionary build request model."""
    rebuild: bool = Field(False, description="Clear and rebuild all indexes")
    tables: Optional[List[str]] = Field(None, description="Specific tables to process")


class DictionaryStatus(BaseModel):
    """Dictionary build status model."""
    available: bool
    last_build: Optional[str]
    build_duration_seconds: Optional[float]
    build_in_progress: bool
    build_progress: int = 0  # 0-100 percentage
    build_step: str = ""  # Current step description
    fuzzy_index: Optional[Dict[str, Any]]
    schema_map: Optional[Dict[str, Any]]


class ClarificationResponse(BaseModel):
    """Response when term not found - suggests similar terms."""
    found: bool
    query: str
    message: str
    suggestions: List[SearchResult]


class TermResolutionRequest(BaseModel):
    """Request to resolve clinical terms."""
    terms: List[str] = Field(..., min_items=1, description="Terms to resolve")


class ResolvedTermResponse(BaseModel):
    """A resolved clinical term."""
    original: str
    resolved: str
    source: str  # dataset_exact, dataset_fuzzy, meddra_abbreviation, meddra_exact
    confidence: float
    table: Optional[str] = None
    column: Optional[str] = None
    meddra_code: Optional[str] = None
    meddra_level: Optional[str] = None


class TermSuggestionResponse(BaseModel):
    """A suggestion for term clarification."""
    value: str
    source: str  # "dataset" or "meddra"
    score: float
    table: Optional[str] = None
    column: Optional[str] = None
    meddra_code: Optional[str] = None
    meddra_level: Optional[str] = None


class SingleResolutionResponse(BaseModel):
    """Resolution result for a single term."""
    success: bool
    query: str
    resolved_term: Optional[ResolvedTermResponse] = None
    needs_clarification: bool = False
    message: str = ""
    suggestions: List[TermSuggestionResponse] = []


class TermResolutionResponse(BaseModel):
    """Response for term resolution request."""
    all_resolved: bool
    resolved_count: int
    needs_clarification_count: int
    results: Dict[str, SingleResolutionResponse]


# ============================================================================
# Helper Functions
# ============================================================================

def get_fuzzy_matcher() -> Optional['FuzzyMatcher']:
    """Get or create FuzzyMatcher instance."""
    global _fuzzy_matcher
    if _fuzzy_matcher is None and DICTIONARY_AVAILABLE:
        if FUZZY_INDEX_PATH.exists():
            try:
                _fuzzy_matcher = FuzzyMatcher.load(str(FUZZY_INDEX_PATH))
                logger.info(f"Loaded fuzzy index with {len(_fuzzy_matcher)} entries")
            except Exception as e:
                logger.error(f"Failed to load fuzzy index: {e}")
    return _fuzzy_matcher


def reload_indexes():
    """Reload all indexes after rebuild."""
    global _fuzzy_matcher, _term_resolver
    _fuzzy_matcher = None
    _term_resolver = None
    # Trigger reload
    get_fuzzy_matcher()
    get_term_resolver()


def get_term_resolver() -> Optional['TermResolver']:
    """Get or create TermResolver instance."""
    global _term_resolver
    if _term_resolver is None and DICTIONARY_AVAILABLE:
        fuzzy = get_fuzzy_matcher()
        meddra = None

        # Try to load MedDRA lookup
        if MEDDRA_AVAILABLE and DATABASE_PATH.exists():
            try:
                loader = MedDRALoader(str(DATABASE_PATH), str(KNOWLEDGE_DIR))
                if loader.is_available():
                    meddra = MedDRALookup(str(DATABASE_PATH))
                    logger.info("MedDRA lookup initialized for TermResolver")
            except Exception as e:
                logger.warning(f"MedDRA not available for TermResolver: {e}")

        if fuzzy or meddra:
            _term_resolver = TermResolver(
                fuzzy_matcher=fuzzy,
                meddra_lookup=meddra
            )
            logger.info(f"TermResolver initialized (fuzzy: {fuzzy is not None}, meddra: {meddra is not None})")

    return _term_resolver


def _update_progress(progress: int, step: str):
    """Update build progress."""
    global _build_progress, _build_step
    _build_progress = progress
    _build_step = step
    logger.info(f"Build progress: {progress}% - {step}")


async def run_dictionary_build(request: BuildRequest):
    """Run dictionary build in background."""
    global _build_in_progress, _build_progress, _build_step

    try:
        _build_in_progress = True
        _build_progress = 0
        _build_step = "Initializing..."
        start_time = datetime.now()

        # Step 1: Initialize components (10%)
        _update_progress(5, "Initializing components...")
        scanner = ValueScanner(str(DATABASE_PATH))
        mapper = SchemaMapper(str(DATABASE_PATH))
        _update_progress(10, "Components initialized")

        # Step 2: Scan values (10% -> 40%)
        _update_progress(15, "Scanning database tables...")
        scan_results = scanner.scan_all_tables()
        _update_progress(40, f"Scanned {len(scan_results)} tables")

        # Step 3: Transform scan results (40% -> 50%)
        _update_progress(45, "Transforming scan results...")
        values_dict = {}
        for table, columns in scan_results.items():
            values_dict[table] = {}
            for column, scan_result in columns.items():
                values_dict[table][column] = scan_result.values
        _update_progress(50, "Scan results transformed")

        # Step 4: Build fuzzy index (50% -> 70%)
        _update_progress(55, "Building fuzzy index...")
        fuzzy = FuzzyMatcher()
        fuzzy.build_index(values_dict)
        _update_progress(65, "Saving fuzzy index...")
        fuzzy.save(str(FUZZY_INDEX_PATH))
        _update_progress(70, f"Fuzzy index built: {len(fuzzy)} entries")

        # Step 5: Build schema map (70% -> 90%)
        _update_progress(75, "Building schema map...")
        schema_map = mapper.build_schema_map()
        _update_progress(85, "Saving schema map...")
        mapper.save_schema_map(schema_map, str(SCHEMA_MAP_PATH))
        _update_progress(90, f"Schema map built: {len(schema_map.tables)} tables")

        # Step 6: Save status and reload (90% -> 100%)
        _update_progress(95, "Finalizing...")
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        status = {
            "last_build": end_time.isoformat(),
            "build_duration_seconds": duration,
            "fuzzy_index": {
                "path": str(FUZZY_INDEX_PATH),
                "entries": len(fuzzy)
            },
            "schema_map": {
                "path": str(SCHEMA_MAP_PATH),
                "tables": len(schema_map.tables)
            }
        }

        with open(STATUS_PATH, 'w') as f:
            json.dump(status, f, indent=2)

        # Reload indexes
        reload_indexes()
        _update_progress(100, "Build complete!")
        logger.info(f"Dictionary build completed in {duration:.1f}s")

    except Exception as e:
        logger.error(f"Dictionary build failed: {e}")
        _build_step = f"Error: {str(e)}"
        raise
    finally:
        _build_in_progress = False


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/status")
async def get_dictionary_status() -> DictionaryStatus:
    """Get dictionary build status and statistics."""
    status = DictionaryStatus(
        available=DICTIONARY_AVAILABLE,
        last_build=None,
        build_duration_seconds=None,
        build_in_progress=_build_in_progress,
        build_progress=_build_progress if _build_in_progress else 0,
        build_step=_build_step if _build_in_progress else "",
        fuzzy_index=None,
        schema_map=None
    )

    # Load status file if exists
    if STATUS_PATH.exists():
        try:
            with open(STATUS_PATH, 'r') as f:
                data = json.load(f)
                status.last_build = data.get("last_build")
                status.build_duration_seconds = data.get("build_duration_seconds")
                status.fuzzy_index = data.get("fuzzy_index")
                status.schema_map = data.get("schema_map")
        except Exception as e:
            logger.warning(f"Failed to load status: {e}")

    # Get live statistics
    fuzzy = get_fuzzy_matcher()
    if fuzzy:
        if status.fuzzy_index is None:
            status.fuzzy_index = {}
        status.fuzzy_index["entries"] = len(fuzzy)
        status.fuzzy_index["loaded"] = True

    return status


@router.post("/build")
async def trigger_dictionary_build(
    request: BuildRequest,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """Trigger dictionary rebuild."""
    if not DICTIONARY_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Dictionary module not available"
        )

    if _build_in_progress:
        raise HTTPException(
            status_code=409,
            detail="Build already in progress"
        )

    if not DATABASE_PATH.exists():
        raise HTTPException(
            status_code=400,
            detail="Database not found. Run Factory 1 first."
        )

    # Start background build
    background_tasks.add_task(run_dictionary_build, request)

    return {
        "status": "started",
        "message": "Dictionary build started in background",
        "options": {
            "rebuild": request.rebuild,
            "tables": request.tables
        }
    }


@router.post("/search")
async def search_values(request: SearchRequest) -> Dict[str, Any]:
    """
    Search for values using fuzzy matching.

    Returns matching clinical data values with scores.
    Used for typo correction and partial matching.
    """
    if not DICTIONARY_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Dictionary module not available"
        )

    fuzzy = get_fuzzy_matcher()
    if not fuzzy:
        raise HTTPException(
            status_code=404,
            detail="Fuzzy index not found. Run dictionary build first."
        )

    if request.table_filter and request.column_filter:
        matches = fuzzy.match_in_column(
            request.query,
            request.table_filter,
            request.column_filter,
            request.threshold,
            request.limit
        )
    else:
        matches = fuzzy.match(
            request.query,
            request.threshold,
            request.limit
        )

    results = [
        SearchResult(
            value=m.value,
            score=m.score,
            table=m.table,
            column=m.column,
            match_type=m.match_type,
            id=m.id
        )
        for m in matches
    ]

    return {
        "query": request.query,
        "threshold": request.threshold,
        "count": len(results),
        "results": [r.model_dump() for r in results]
    }


@router.post("/lookup")
async def lookup_term(
    term: str = Body(..., embed=True),
    threshold: float = Body(85.0, embed=True),
    limit: int = Body(10, embed=True)
) -> ClarificationResponse:
    """
    Look up a clinical term with clarification support.

    If exact or close match found, returns found=True.
    If not found, returns found=False with suggestions for clarification.

    This endpoint is designed for the inference pipeline to validate
    terms before generating SQL.
    """
    if not DICTIONARY_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Dictionary module not available"
        )

    fuzzy = get_fuzzy_matcher()
    if not fuzzy:
        raise HTTPException(
            status_code=404,
            detail="Fuzzy index not found. Run dictionary build first."
        )

    # First try exact match (case insensitive)
    exact_matches = fuzzy.match(term, threshold=100, limit=1)
    if exact_matches:
        return ClarificationResponse(
            found=True,
            query=term,
            message=f"Exact match found: {exact_matches[0].value}",
            suggestions=[
                SearchResult(
                    value=m.value,
                    score=m.score,
                    table=m.table,
                    column=m.column,
                    match_type="exact",
                    id=m.id
                )
                for m in exact_matches
            ]
        )

    # Try fuzzy match
    fuzzy_matches = fuzzy.match(term, threshold=threshold, limit=limit)

    if fuzzy_matches and fuzzy_matches[0].score >= 90:
        # High confidence fuzzy match (likely typo)
        return ClarificationResponse(
            found=True,
            query=term,
            message=f"Close match found: {fuzzy_matches[0].value} (score: {fuzzy_matches[0].score:.1f}%)",
            suggestions=[
                SearchResult(
                    value=m.value,
                    score=m.score,
                    table=m.table,
                    column=m.column,
                    match_type=m.match_type,
                    id=m.id
                )
                for m in fuzzy_matches
            ]
        )

    # No good match - return suggestions for clarification
    # Lower threshold to get more suggestions
    suggestions = fuzzy.match(term, threshold=50, limit=limit)

    if suggestions:
        return ClarificationResponse(
            found=False,
            query=term,
            message=f"'{term}' not found in dataset. Did you mean one of these?",
            suggestions=[
                SearchResult(
                    value=m.value,
                    score=m.score,
                    table=m.table,
                    column=m.column,
                    match_type=m.match_type,
                    id=m.id
                )
                for m in suggestions
            ]
        )
    else:
        return ClarificationResponse(
            found=False,
            query=term,
            message=f"'{term}' not found in dataset and no similar terms found.",
            suggestions=[]
        )


@router.get("/values/{table}/{column}")
async def get_column_values(
    table: str,
    column: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
) -> Dict[str, Any]:
    """Get unique values for a specific table.column."""
    if not DICTIONARY_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Dictionary module not available"
        )

    fuzzy = get_fuzzy_matcher()
    if not fuzzy:
        raise HTTPException(
            status_code=404,
            detail="Fuzzy index not found. Run dictionary build first."
        )

    table_upper = table.upper()
    column_upper = column.upper()

    values = fuzzy._values_by_table.get(table_upper, {}).get(column_upper, [])

    # Apply pagination
    total = len(values)
    paginated = values[offset:offset + limit]

    return {
        "table": table_upper,
        "column": column_upper,
        "total": total,
        "offset": offset,
        "limit": limit,
        "values": paginated
    }


@router.get("/schema-map")
async def get_schema_map() -> Dict[str, Any]:
    """Get the schema map JSON for column -> table lookups."""
    if not SCHEMA_MAP_PATH.exists():
        raise HTTPException(
            status_code=404,
            detail="Schema map not found. Run dictionary build first."
        )

    try:
        with open(SCHEMA_MAP_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read schema map: {str(e)}"
        )


@router.get("/tables")
async def list_indexed_tables() -> Dict[str, Any]:
    """List all tables with indexed values."""
    if not DICTIONARY_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Dictionary module not available"
        )

    fuzzy = get_fuzzy_matcher()
    if not fuzzy:
        raise HTTPException(
            status_code=404,
            detail="Fuzzy index not found. Run dictionary build first."
        )

    tables = {}
    for table, columns in fuzzy._values_by_table.items():
        tables[table] = {
            "columns": list(columns.keys()),
            "value_count": sum(len(vals) for vals in columns.values())
        }

    return {
        "count": len(tables),
        "tables": tables
    }


@router.get("/statistics")
async def get_statistics() -> Dict[str, Any]:
    """Get detailed dictionary statistics."""
    if not DICTIONARY_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Dictionary module not available"
        )

    stats = {
        "fuzzy": None,
        "schema_map": None
    }

    fuzzy = get_fuzzy_matcher()
    if fuzzy:
        stats["fuzzy"] = fuzzy.get_statistics()

    if SCHEMA_MAP_PATH.exists():
        try:
            with open(SCHEMA_MAP_PATH, 'r') as f:
                schema = json.load(f)
                stats["schema_map"] = {
                    "tables": len(schema.get("tables", {})),
                    "columns": len(schema.get("columns", {})),
                    "generated_at": schema.get("generated_at")
                }
        except Exception:
            pass

    return stats


@router.post("/correct")
async def correct_spelling(
    query: str = Body(..., embed=True),
    threshold: float = Body(60.0, embed=True),
    limit: int = Body(5, embed=True)
) -> Dict[str, Any]:
    """Suggest spelling corrections for a query using fuzzy matching."""
    if not DICTIONARY_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Dictionary module not available"
        )

    fuzzy = get_fuzzy_matcher()
    if not fuzzy:
        raise HTTPException(
            status_code=404,
            detail="Fuzzy index not found. Run dictionary build first."
        )

    matches = fuzzy.match(query, threshold, limit)
    suggestions = [
        {
            "original": query,
            "suggestion": m.value,
            "score": m.score,
            "table": m.table,
            "column": m.column
        }
        for m in matches
    ]

    return {
        "query": query,
        "count": len(suggestions),
        "suggestions": suggestions
    }


@router.post("/resolve")
async def resolve_terms(request: TermResolutionRequest) -> TermResolutionResponse:
    """
    Resolve clinical terms using combined fuzzy matching and MedDRA lookup.

    This is the primary endpoint for Factory 4 (Inference Engine) to use
    when validating clinical terms before generating SQL.

    Resolution flow:
    1. Exact match in dataset → Use directly
    2. High-confidence fuzzy match (>92%) → Typo correction
    3. MedDRA abbreviation (MI → MYOCARDIAL INFARCTION)
    4. MedDRA exact term → Validate against controlled vocabulary
    5. No match → Return suggestions for clarification

    Returns:
        TermResolutionResponse with resolution status for each term
    """
    if not DICTIONARY_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Dictionary module not available"
        )

    resolver = get_term_resolver()
    if not resolver:
        raise HTTPException(
            status_code=404,
            detail="Term resolver not available. Run dictionary build and/or load MedDRA first."
        )

    results = {}
    resolved_count = 0
    needs_clarification_count = 0

    for term in request.terms:
        result = resolver.resolve(term)

        # Convert to response model
        resolved_term = None
        if result.resolved_term:
            resolved_term = ResolvedTermResponse(
                original=result.resolved_term.original,
                resolved=result.resolved_term.resolved,
                source=result.resolved_term.source.value,
                confidence=result.resolved_term.confidence,
                table=result.resolved_term.table,
                column=result.resolved_term.column,
                meddra_code=result.resolved_term.meddra_code,
                meddra_level=result.resolved_term.meddra_level
            )

        suggestions = [
            TermSuggestionResponse(
                value=s.value,
                source=s.source,
                score=s.score,
                table=s.table,
                column=s.column,
                meddra_code=s.meddra_code,
                meddra_level=s.meddra_level
            )
            for s in result.suggestions
        ]

        results[term] = SingleResolutionResponse(
            success=result.success,
            query=result.query,
            resolved_term=resolved_term,
            needs_clarification=result.needs_clarification,
            message=result.message,
            suggestions=suggestions
        )

        if result.success:
            resolved_count += 1
        if result.needs_clarification:
            needs_clarification_count += 1

    return TermResolutionResponse(
        all_resolved=needs_clarification_count == 0,
        resolved_count=resolved_count,
        needs_clarification_count=needs_clarification_count,
        results=results
    )


@router.post("/resolve-single")
async def resolve_single_term(
    term: str = Body(..., embed=True)
) -> SingleResolutionResponse:
    """
    Resolve a single clinical term.

    Convenience endpoint for resolving one term at a time.
    Uses combined fuzzy matching and MedDRA lookup.
    """
    if not DICTIONARY_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Dictionary module not available"
        )

    resolver = get_term_resolver()
    if not resolver:
        raise HTTPException(
            status_code=404,
            detail="Term resolver not available. Run dictionary build and/or load MedDRA first."
        )

    result = resolver.resolve(term)

    resolved_term = None
    if result.resolved_term:
        resolved_term = ResolvedTermResponse(
            original=result.resolved_term.original,
            resolved=result.resolved_term.resolved,
            source=result.resolved_term.source.value,
            confidence=result.resolved_term.confidence,
            table=result.resolved_term.table,
            column=result.resolved_term.column,
            meddra_code=result.resolved_term.meddra_code,
            meddra_level=result.resolved_term.meddra_level
        )

    suggestions = [
        TermSuggestionResponse(
            value=s.value,
            source=s.source,
            score=s.score,
            table=s.table,
            column=s.column,
            meddra_code=s.meddra_code,
            meddra_level=s.meddra_level
        )
        for s in result.suggestions
    ]

    return SingleResolutionResponse(
        success=result.success,
        query=result.query,
        resolved_term=resolved_term,
        needs_clarification=result.needs_clarification,
        message=result.message,
        suggestions=suggestions
    )
