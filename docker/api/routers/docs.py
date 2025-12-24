# SAGE API - Documentation Router
# =================================
"""
Documentation endpoints for "Ask the System" feature.

Provides:
- Documentation search and retrieval
- Category browsing
- System knowledge queries
"""

import sys
from pathlib import Path
from typing import Optional, List
from pydantic import BaseModel, Field

from fastapi import APIRouter, HTTPException, Query, Depends

# Add project root to path
# In container: routers at /app/routers, core at /app/core, docs at /app/docs
# On host: this file is at docker/api/routers/docs.py
import os
project_root = Path(os.environ.get('APP_ROOT', '/app'))
sys.path.insert(0, str(project_root))

from .auth import get_current_user
from core.engine import (
    SystemKnowledge,
    get_system_knowledge,
    SearchResult as CoreSearchResult
)

router = APIRouter()


# ============================================================================
# Response Models
# ============================================================================

class DocumentSection(BaseModel):
    """A section within a document."""
    heading: str
    content: str
    level: int = 2


class DocumentSummary(BaseModel):
    """Summary of a document for listing."""
    id: str
    title: str
    category: str
    path: str
    summary: str = ""


class DocumentDetail(BaseModel):
    """Full document details."""
    id: str
    title: str
    category: str
    path: str
    summary: str
    sections: List[DocumentSection]
    keywords: List[str]
    word_count: int


class SearchResultItem(BaseModel):
    """A single search result."""
    doc_id: str
    title: str
    category: str
    path: str
    relevance_score: float
    matched_keywords: List[str]
    summary: str
    sections: List[DocumentSection] = []


class SearchResponse(BaseModel):
    """Response from documentation search."""
    query: str
    results: List[SearchResultItem]
    total_results: int


class CategoryInfo(BaseModel):
    """Information about a documentation category."""
    name: str
    document_count: int
    documents: List[DocumentSummary]


class CategoriesResponse(BaseModel):
    """Response listing all categories."""
    categories: List[CategoryInfo]
    total_documents: int


class AskResponse(BaseModel):
    """Response from 'Ask the System' query."""
    query: str
    is_meta_query: bool
    answer: str
    sources: List[DocumentSummary]


class DocsStatsResponse(BaseModel):
    """Documentation statistics."""
    total_documents: int
    total_categories: int
    total_keywords: int
    generated_at: str


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/stats", response_model=DocsStatsResponse)
async def get_documentation_stats(
    current_user: dict = Depends(get_current_user)
):
    """Get documentation statistics."""
    knowledge = get_system_knowledge()
    if not knowledge.load():
        raise HTTPException(
            status_code=503,
            detail="Documentation index not available"
        )

    docs = knowledge.get_all_documents()
    categories = knowledge.get_categories()

    # Count unique keywords
    all_keywords = set()
    for doc in docs:
        all_keywords.update(doc.get('keywords', []))

    return DocsStatsResponse(
        total_documents=len(docs),
        total_categories=len(categories),
        total_keywords=len(all_keywords),
        generated_at=knowledge._index.get('generated_at', '')
    )


@router.get("/categories", response_model=CategoriesResponse)
async def list_categories(
    current_user: dict = Depends(get_current_user)
):
    """List all documentation categories."""
    knowledge = get_system_knowledge()
    if not knowledge.load():
        raise HTTPException(
            status_code=503,
            detail="Documentation index not available"
        )

    categories = knowledge.get_categories()
    category_list = []

    for name, docs in categories.items():
        category_list.append(CategoryInfo(
            name=name,
            document_count=len(docs),
            documents=[
                DocumentSummary(
                    id=d['id'],
                    title=d['title'],
                    category=name,
                    path=d['path']
                ) for d in docs
            ]
        ))

    # Sort by category name
    category_list.sort(key=lambda x: x.name)

    total_docs = sum(cat.document_count for cat in category_list)

    return CategoriesResponse(
        categories=category_list,
        total_documents=total_docs
    )


@router.get("/documents", response_model=List[DocumentSummary])
async def list_documents(
    category: Optional[str] = Query(None, description="Filter by category"),
    current_user: dict = Depends(get_current_user)
):
    """List all documents, optionally filtered by category."""
    knowledge = get_system_knowledge()
    if not knowledge.load():
        raise HTTPException(
            status_code=503,
            detail="Documentation index not available"
        )

    docs = knowledge.get_all_documents()

    if category:
        docs = [d for d in docs if d.get('category', '').lower() == category.lower()]

    return [
        DocumentSummary(
            id=d['id'],
            title=d['title'],
            category=d.get('category', ''),
            path=d['path'],
            summary=d.get('summary', '')
        ) for d in docs
    ]


@router.get("/documents/{doc_id:path}", response_model=DocumentDetail)
async def get_document(
    doc_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get a specific document by ID."""
    knowledge = get_system_knowledge()
    if not knowledge.load():
        raise HTTPException(
            status_code=503,
            detail="Documentation index not available"
        )

    doc = knowledge.get_document(doc_id)
    if not doc:
        raise HTTPException(
            status_code=404,
            detail=f"Document not found: {doc_id}"
        )

    return DocumentDetail(
        id=doc['id'],
        title=doc['title'],
        category=doc.get('category', ''),
        path=doc['path'],
        summary=doc.get('summary', ''),
        sections=[
            DocumentSection(
                heading=s['heading'],
                content=s['content'],
                level=s.get('level', 2)
            ) for s in doc.get('sections', [])
        ],
        keywords=doc.get('keywords', []),
        word_count=doc.get('word_count', 0)
    )


@router.get("/search", response_model=SearchResponse)
async def search_documentation(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(10, ge=1, le=50, description="Maximum results"),
    current_user: dict = Depends(get_current_user)
):
    """Search documentation by keywords."""
    knowledge = get_system_knowledge()
    if not knowledge.load():
        raise HTTPException(
            status_code=503,
            detail="Documentation index not available"
        )

    results = knowledge.search(q, max_results=limit)

    return SearchResponse(
        query=q,
        results=[
            SearchResultItem(
                doc_id=r.doc_id,
                title=r.title,
                category=r.category,
                path=r.path,
                relevance_score=r.relevance_score,
                matched_keywords=r.matched_keywords,
                summary=r.summary,
                sections=[
                    DocumentSection(
                        heading=s['heading'],
                        content=s['content'],
                        level=s.get('level', 2)
                    ) for s in r.sections
                ]
            ) for r in results
        ],
        total_results=len(results)
    )


@router.get("/ask", response_model=AskResponse)
async def ask_the_system(
    q: str = Query(..., min_length=3, description="Your question about SAGE"),
    current_user: dict = Depends(get_current_user)
):
    """
    Ask a question about the SAGE system.

    This endpoint answers questions about how SAGE works, its features,
    and configuration. Examples:
    - "How does SAGE handle SQL injection?"
    - "What is confidence scoring?"
    - "How does fuzzy matching work?"
    """
    knowledge = get_system_knowledge()
    if not knowledge.load():
        raise HTTPException(
            status_code=503,
            detail="Documentation index not available"
        )

    response = knowledge.answer_query(q)

    return AskResponse(
        query=q,
        is_meta_query=response.is_meta_query,
        answer=response.answer,
        sources=[
            DocumentSummary(
                id=s.get('path', '').replace('.md', '').replace('/', '-'),
                title=s['title'],
                category=s['category'],
                path=s['path']
            ) for s in response.sources
        ]
    )


@router.get("/content/{doc_path:path}")
async def get_raw_markdown(
    doc_path: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get the raw markdown content of a document.

    This returns the actual markdown file content for rendering.
    """
    docs_dir = project_root / "docs"

    # Ensure path ends with .md
    if not doc_path.endswith('.md'):
        doc_path = doc_path + '.md'

    file_path = docs_dir / doc_path

    # Security check - ensure path is within docs directory
    try:
        file_path = file_path.resolve()
        if not str(file_path).startswith(str(docs_dir.resolve())):
            raise HTTPException(
                status_code=403,
                detail="Access denied"
            )
    except Exception:
        raise HTTPException(
            status_code=404,
            detail="Document not found"
        )

    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Document not found: {doc_path}"
        )

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        return {
            "path": doc_path,
            "content": content
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error reading document: {str(e)}"
        )
