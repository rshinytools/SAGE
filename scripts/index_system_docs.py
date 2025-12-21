#!/usr/bin/env python
# SAGE - Documentation Indexer
# ============================
# Indexes all system documentation for "Ask the System" feature
"""
Documentation Indexer

This script creates a searchable index of all SAGE documentation:
1. Scans all Markdown files in docs/
2. Extracts content, headings, and keywords
3. Creates knowledge/system_docs.json for static search

Usage:
    python scripts/index_system_docs.py
    python scripts/index_system_docs.py --verbose
"""

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DocumentationIndexer:
    """
    Indexes SAGE documentation for keyword-based search.

    Creates a structured JSON index with:
    - Document metadata (path, title, category)
    - Section content with headings
    - Extracted keywords for matching
    """

    # Keywords to extract for clinical/technical topics
    TOPIC_KEYWORDS = {
        'security': ['security', 'injection', 'phi', 'pii', 'authentication', 'jwt', 'token', 'audit', 'blocked', 'sanitize', 'validation'],
        'data': ['sas', 'sas7bdat', 'parquet', 'duckdb', 'database', 'table', 'column', 'schema', 'load', 'ingest'],
        'metadata': ['metadata', 'excel', 'specification', 'cdisc', 'adam', 'sdtm', 'variable', 'codelist'],
        'dictionary': ['fuzzy', 'matching', 'index', 'rapidfuzz', 'synonym', 'value', 'scan'],
        'meddra': ['meddra', 'soc', 'hlgt', 'hlt', 'pt', 'llt', 'adverse', 'event', 'terminology', 'hierarchy'],
        'engine': ['pipeline', 'query', 'sql', 'generate', 'execute', 'confidence', 'score', 'llm', 'claude'],
        'compliance': ['gamp', 'validation', '21 cfr', 'audit', 'alcoa', 'regulatory', 'part 11'],
        'architecture': ['factory', 'architecture', 'flow', 'component', 'service', 'docker', 'api'],
    }

    def __init__(self, docs_dir: Path, output_path: Path):
        self.docs_dir = docs_dir
        self.output_path = output_path
        self.index: Dict[str, Any] = {
            'version': '1.0',
            'generated_at': datetime.now().isoformat(),
            'documents': [],
            'keyword_index': {},
            'categories': {}
        }

    def run(self) -> Dict[str, Any]:
        """Run the indexing pipeline."""
        logger.info(f"Scanning documentation in {self.docs_dir}")

        # Find all markdown files
        md_files = list(self.docs_dir.rglob('*.md'))
        logger.info(f"Found {len(md_files)} markdown files")

        for md_file in md_files:
            try:
                doc = self._parse_document(md_file)
                if doc:
                    self.index['documents'].append(doc)
                    self._update_keyword_index(doc)
                    self._update_categories(doc)
            except Exception as e:
                logger.error(f"Error parsing {md_file}: {e}")

        # Sort documents by category and title
        self.index['documents'].sort(key=lambda x: (x.get('category', ''), x.get('title', '')))

        # Save index
        self._save_index()

        logger.info(f"Indexed {len(self.index['documents'])} documents")
        logger.info(f"Created {len(self.index['keyword_index'])} keyword entries")

        return self.index

    def _parse_document(self, file_path: Path) -> Dict[str, Any]:
        """Parse a markdown document into structured data."""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Get relative path for display
        rel_path = file_path.relative_to(self.docs_dir)

        # Extract title (first H1)
        title_match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
        title = title_match.group(1).strip() if title_match else rel_path.stem.replace('-', ' ').title()

        # Determine category from path
        category = self._get_category(rel_path)

        # Extract sections
        sections = self._extract_sections(content)

        # Extract keywords
        keywords = self._extract_keywords(content, title)

        # Build document entry
        doc = {
            'id': str(rel_path).replace('\\', '/').replace('.md', ''),
            'path': str(rel_path).replace('\\', '/'),
            'title': title,
            'category': category,
            'sections': sections,
            'keywords': keywords,
            'summary': self._extract_summary(content),
            'word_count': len(content.split())
        }

        return doc

    def _get_category(self, rel_path: Path) -> str:
        """Determine document category from path."""
        parts = rel_path.parts

        if len(parts) == 1:
            return 'overview'

        category_map = {
            'getting-started': 'Getting Started',
            'architecture': 'Architecture',
            'factories': 'Factories',
            'factory1-data': 'Factory 1 - Data',
            'factory2-metadata': 'Factory 2 - Metadata',
            'factory3-dictionary': 'Factory 3 - Dictionary',
            'factory35-meddra': 'Factory 3.5 - MedDRA',
            'factory4-engine': 'Factory 4 - Engine',
            'user-guide': 'User Guide',
            'admin-guide': 'Admin Guide',
            'compliance': 'Compliance',
            'api-reference': 'API Reference',
            'code-reference': 'Code Reference'
        }

        for part in parts:
            if part in category_map:
                return category_map[part]

        return parts[0].replace('-', ' ').title()

    def _extract_sections(self, content: str) -> List[Dict[str, str]]:
        """Extract sections from markdown content."""
        sections = []

        # Split by headings (## or ###)
        pattern = r'^(#{2,3})\s+(.+)$'
        matches = list(re.finditer(pattern, content, re.MULTILINE))

        for i, match in enumerate(matches):
            heading = match.group(2).strip()
            start = match.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(content)

            section_content = content[start:end].strip()
            # Clean up code blocks and special formatting for search
            clean_content = self._clean_for_search(section_content)

            if clean_content:
                sections.append({
                    'heading': heading,
                    'content': clean_content[:1000],  # Limit content size
                    'level': len(match.group(1))
                })

        return sections[:20]  # Limit sections per document

    def _clean_for_search(self, text: str) -> str:
        """Clean markdown content for search indexing."""
        # Remove code blocks
        text = re.sub(r'```[\s\S]*?```', '', text)
        # Remove inline code
        text = re.sub(r'`[^`]+`', '', text)
        # Remove links but keep text
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
        # Remove images
        text = re.sub(r'!\[.*?\]\(.*?\)', '', text)
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        # Remove horizontal rules
        text = re.sub(r'^---+$', '', text, flags=re.MULTILINE)
        # Normalize whitespace
        text = ' '.join(text.split())
        return text.strip()

    def _extract_keywords(self, content: str, title: str) -> List[str]:
        """Extract relevant keywords from content."""
        keywords = set()

        content_lower = content.lower()
        title_lower = title.lower()

        # Add title words (excluding common words)
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were'}
        for word in re.findall(r'\b\w+\b', title_lower):
            if word not in stop_words and len(word) > 2:
                keywords.add(word)

        # Check for topic keywords
        for topic, topic_keywords in self.TOPIC_KEYWORDS.items():
            for kw in topic_keywords:
                if kw in content_lower:
                    keywords.add(kw)
                    keywords.add(topic)

        # Extract technical terms (uppercase or CamelCase)
        technical = re.findall(r'\b[A-Z][A-Z0-9_]+\b', content)  # UPPERCASE
        keywords.update(t.lower() for t in technical if len(t) > 2)

        # Extract common CDISC/clinical terms
        clinical_terms = ['adsl', 'adae', 'adlb', 'dm', 'ae', 'lb', 'vs', 'ex', 'cm', 'mh',
                         'usubjid', 'saffl', 'ittfl', 'efffl', 'arm', 'aedecod', 'aebodsys']
        for term in clinical_terms:
            if term in content_lower:
                keywords.add(term)

        return sorted(list(keywords))[:50]  # Limit keywords

    def _extract_summary(self, content: str) -> str:
        """Extract a summary from the document (first paragraph after title)."""
        # Skip title and find first substantive paragraph
        lines = content.split('\n')
        in_code_block = False
        summary_lines = []

        for line in lines:
            if line.startswith('```'):
                in_code_block = not in_code_block
                continue
            if in_code_block:
                continue
            if line.startswith('#'):
                continue
            if line.startswith('---'):
                continue

            clean = line.strip()
            if clean and not clean.startswith('|') and not clean.startswith('-'):
                summary_lines.append(clean)
                if len(' '.join(summary_lines)) > 200:
                    break

        summary = ' '.join(summary_lines)
        if len(summary) > 300:
            summary = summary[:297] + '...'
        return summary

    def _update_keyword_index(self, doc: Dict[str, Any]):
        """Update the keyword index with document."""
        for keyword in doc.get('keywords', []):
            if keyword not in self.index['keyword_index']:
                self.index['keyword_index'][keyword] = []
            self.index['keyword_index'][keyword].append({
                'doc_id': doc['id'],
                'title': doc['title'],
                'category': doc['category']
            })

    def _update_categories(self, doc: Dict[str, Any]):
        """Update the category index."""
        category = doc.get('category', 'Other')
        if category not in self.index['categories']:
            self.index['categories'][category] = []
        self.index['categories'][category].append({
            'id': doc['id'],
            'title': doc['title'],
            'path': doc['path']
        })

    def _save_index(self):
        """Save the index to JSON file."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, 'w', encoding='utf-8') as f:
            json.dump(self.index, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved index to {self.output_path}")


def main():
    parser = argparse.ArgumentParser(description='Index SAGE documentation')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    docs_dir = project_root / 'docs'
    output_path = project_root / 'knowledge' / 'system_docs.json'

    if not docs_dir.exists():
        logger.error(f"Documentation directory not found: {docs_dir}")
        sys.exit(1)

    indexer = DocumentationIndexer(docs_dir, output_path)
    index = indexer.run()

    # Print summary
    print("\n" + "=" * 50)
    print("SAGE Documentation Index Created")
    print("=" * 50)
    print(f"Documents indexed: {len(index['documents'])}")
    print(f"Keywords extracted: {len(index['keyword_index'])}")
    print(f"Categories: {len(index['categories'])}")
    print(f"\nCategories:")
    for cat, docs in index['categories'].items():
        print(f"  - {cat}: {len(docs)} docs")
    print(f"\nOutput: {output_path}")
    print("=" * 50)


if __name__ == '__main__':
    main()
