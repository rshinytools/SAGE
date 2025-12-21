"""
SAGE System Knowledge - "Ask the System" Feature
=================================================

Provides keyword-based search over SAGE documentation to answer
questions about the platform itself.

Example queries:
- "How does SAGE handle SQL injection?"
- "What is confidence scoring?"
- "How does fuzzy matching work?"
"""

import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from functools import lru_cache

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """A single search result from the knowledge base."""
    doc_id: str
    title: str
    category: str
    path: str
    relevance_score: float
    matched_keywords: List[str]
    summary: str
    sections: List[Dict[str, str]]


@dataclass
class SystemKnowledgeResponse:
    """Response from a system knowledge query."""
    query: str
    is_meta_query: bool
    results: List[SearchResult]
    answer: str
    sources: List[Dict[str, str]]


class SystemKnowledge:
    """
    System Knowledge base for answering questions about SAGE.

    Uses keyword-based search over indexed documentation to find
    relevant answers without requiring an LLM for simple lookups.
    """

    # Patterns that indicate a meta-query about the system
    META_QUERY_PATTERNS = [
        r'\bhow does sage\b',
        r'\bhow does the (?:system|platform|pipeline)\b',
        r'\bwhat is (?:the )?(?:confidence|scoring|pipeline|factory)\b',
        r'\bexplain (?:the )?(?:architecture|security|validation)\b',
        r'\bhow (?:does|is) (?:sql|phi|pii|injection)\b',
        r'\bwhat (?:columns|tables|variables) (?:are|does)\b',
        r'\bhow (?:to|do i|can i) (?:load|configure|use|query)\b',
        r'\bwhat is (?:meddra|cdisc|adam|sdtm)\b',
        r'\btell me about (?:the )?(?:system|platform|sage)\b',
        r'\bdocumentation\b',
        r'\bhelp (?:with|me)\b',
        r'\bhow (?:does|do|is) (?:fuzzy|matching|search)\b',
        r'\bwhat (?:are|is) (?:gamp|alcoa|audit|compliance)\b',
    ]

    def __init__(self, index_path: Optional[Path] = None):
        """
        Initialize the SystemKnowledge with documentation index.

        Args:
            index_path: Path to system_docs.json. If None, uses default location.
        """
        if index_path is None:
            index_path = Path(__file__).parent.parent.parent / 'knowledge' / 'system_docs.json'

        self.index_path = index_path
        self._index: Optional[Dict[str, Any]] = None
        self._loaded = False

    def load(self) -> bool:
        """Load the documentation index."""
        if self._loaded:
            return True

        try:
            if not self.index_path.exists():
                logger.warning(f"System docs index not found at {self.index_path}")
                return False

            with open(self.index_path, 'r', encoding='utf-8') as f:
                self._index = json.load(f)

            self._loaded = True
            logger.info(f"Loaded system knowledge: {len(self._index.get('documents', []))} documents")
            return True

        except Exception as e:
            logger.error(f"Failed to load system knowledge: {e}")
            return False

    def is_meta_query(self, query: str) -> bool:
        """
        Check if a query is asking about the system itself.

        Args:
            query: The user's question

        Returns:
            True if this is a meta-query about SAGE
        """
        query_lower = query.lower().strip()

        # Check against meta-query patterns
        for pattern in self.META_QUERY_PATTERNS:
            if re.search(pattern, query_lower):
                return True

        # Check for specific system keywords
        system_keywords = ['sage', 'documentation', 'help', 'how to', 'factory', 'pipeline']
        if any(kw in query_lower for kw in system_keywords) and '?' in query:
            return True

        return False

    def search(self, query: str, max_results: int = 5) -> List[SearchResult]:
        """
        Search documentation for relevant results.

        Args:
            query: The search query
            max_results: Maximum number of results to return

        Returns:
            List of SearchResult objects sorted by relevance
        """
        if not self.load():
            return []

        # Extract keywords from query
        query_keywords = self._extract_query_keywords(query)
        if not query_keywords:
            return []

        results = []
        keyword_index = self._index.get('keyword_index', {})
        documents = {d['id']: d for d in self._index.get('documents', [])}

        # Score each document based on keyword matches
        doc_scores: Dict[str, Tuple[float, List[str]]] = {}

        for keyword in query_keywords:
            # Exact match
            if keyword in keyword_index:
                for entry in keyword_index[keyword]:
                    doc_id = entry['doc_id']
                    if doc_id not in doc_scores:
                        doc_scores[doc_id] = (0.0, [])
                    score, matched = doc_scores[doc_id]
                    doc_scores[doc_id] = (score + 1.0, matched + [keyword])

            # Partial match (prefix)
            for idx_keyword, entries in keyword_index.items():
                if idx_keyword.startswith(keyword) or keyword.startswith(idx_keyword):
                    for entry in entries:
                        doc_id = entry['doc_id']
                        if doc_id not in doc_scores:
                            doc_scores[doc_id] = (0.0, [])
                        score, matched = doc_scores[doc_id]
                        if idx_keyword not in matched:
                            doc_scores[doc_id] = (score + 0.5, matched + [idx_keyword])

        # Convert to SearchResult objects
        for doc_id, (score, matched_keywords) in doc_scores.items():
            if doc_id not in documents:
                continue

            doc = documents[doc_id]

            # Boost score based on title match
            title_lower = doc.get('title', '').lower()
            for kw in query_keywords:
                if kw in title_lower:
                    score += 2.0

            # Filter sections to relevant ones
            relevant_sections = []
            for section in doc.get('sections', []):
                section_text = (section.get('heading', '') + ' ' + section.get('content', '')).lower()
                if any(kw in section_text for kw in query_keywords):
                    relevant_sections.append(section)

            results.append(SearchResult(
                doc_id=doc_id,
                title=doc.get('title', ''),
                category=doc.get('category', ''),
                path=doc.get('path', ''),
                relevance_score=score,
                matched_keywords=list(set(matched_keywords)),
                summary=doc.get('summary', ''),
                sections=relevant_sections[:3]
            ))

        # Sort by relevance and return top results
        results.sort(key=lambda x: x.relevance_score, reverse=True)
        return results[:max_results]

    def answer_query(self, query: str) -> SystemKnowledgeResponse:
        """
        Answer a meta-query about the system.

        Args:
            query: The user's question

        Returns:
            SystemKnowledgeResponse with answer and sources
        """
        is_meta = self.is_meta_query(query)
        results = self.search(query)

        if not results:
            return SystemKnowledgeResponse(
                query=query,
                is_meta_query=is_meta,
                results=[],
                answer="I couldn't find specific documentation for that question. Try browsing the documentation categories or rephrasing your question.",
                sources=[]
            )

        # Build answer from top results
        answer_parts = []
        sources = []

        for i, result in enumerate(results[:3]):
            if i == 0:
                answer_parts.append(f"**{result.title}**\n\n{result.summary}")
            else:
                answer_parts.append(f"**{result.title}**: {result.summary[:150]}...")

            sources.append({
                'title': result.title,
                'path': result.path,
                'category': result.category
            })

            # Add relevant section content for top result
            if i == 0 and result.sections:
                for section in result.sections[:2]:
                    answer_parts.append(f"\n### {section['heading']}\n{section['content'][:500]}")

        answer = "\n\n".join(answer_parts)

        return SystemKnowledgeResponse(
            query=query,
            is_meta_query=is_meta,
            results=results,
            answer=answer,
            sources=sources
        )

    def get_categories(self) -> Dict[str, List[Dict[str, str]]]:
        """Get all documentation categories with their documents."""
        if not self.load():
            return {}
        return self._index.get('categories', {})

    def get_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific document by ID."""
        if not self.load():
            return None

        for doc in self._index.get('documents', []):
            if doc.get('id') == doc_id:
                return doc
        return None

    def get_all_documents(self) -> List[Dict[str, Any]]:
        """Get all indexed documents."""
        if not self.load():
            return []
        return self._index.get('documents', [])

    def _extract_query_keywords(self, query: str) -> List[str]:
        """Extract searchable keywords from a query."""
        # Lowercase and tokenize
        query_lower = query.lower()

        # Remove common question words
        stop_words = {
            'how', 'does', 'what', 'is', 'the', 'a', 'an', 'and', 'or', 'but',
            'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'can', 'do',
            'i', 'you', 'we', 'it', 'this', 'that', 'these', 'those', 'are',
            'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
            'about', 'tell', 'me', 'please', 'help', 'explain', 'show'
        }

        # Extract words
        words = re.findall(r'\b\w+\b', query_lower)
        keywords = [w for w in words if w not in stop_words and len(w) > 2]

        # Add compound terms
        compound_terms = {
            'sql injection': 'injection',
            'fuzzy matching': 'fuzzy',
            'confidence score': 'confidence',
            'audit trail': 'audit',
            'data integrity': 'integrity',
            'phi blocking': 'phi'
        }
        for term, keyword in compound_terms.items():
            if term in query_lower:
                keywords.append(keyword)

        return list(set(keywords))


# Singleton instance for reuse
_system_knowledge: Optional[SystemKnowledge] = None


def get_system_knowledge() -> SystemKnowledge:
    """Get the singleton SystemKnowledge instance."""
    global _system_knowledge
    if _system_knowledge is None:
        _system_knowledge = SystemKnowledge()
    return _system_knowledge
