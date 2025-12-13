# SAGE - Input Sanitizer
# =======================
"""
Input Sanitizer
===============
Security layer that checks user input for:
1. PHI/PII patterns (SSN, emails, phone numbers)
2. SQL injection attempts
3. Prompt injection attempts
4. Malicious patterns

This is STEP 1 of the 9-step pipeline.
"""

import re
import logging
from typing import List, Set, Pattern
from dataclasses import dataclass, field

from .models import SanitizationResult

logger = logging.getLogger(__name__)


@dataclass
class SanitizerConfig:
    """Configuration for input sanitizer."""
    # Maximum query length
    max_query_length: int = 2000

    # PHI/PII patterns to block
    block_phi: bool = True

    # SQL injection detection
    block_sql_injection: bool = True

    # Prompt injection detection
    block_prompt_injection: bool = True

    # Custom blocklist words
    custom_blocklist: Set[str] = field(default_factory=set)


class InputSanitizer:
    """
    Sanitizes user input for security.

    This class implements the first step of the inference pipeline,
    ensuring that malicious or sensitive content is blocked before
    any processing occurs.

    Example:
        sanitizer = InputSanitizer()
        result = sanitizer.sanitize("How many patients had headaches?")
        if result.is_safe:
            # Process query
            pass
        else:
            # Block and log
            print(f"Blocked: {result.blocked_reason}")
    """

    # PHI/PII patterns
    PHI_PATTERNS = {
        'ssn': re.compile(r'\b\d{3}[-.\s]?\d{2}[-.\s]?\d{4}\b'),
        'email': re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
        'phone': re.compile(r'\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b'),
        'credit_card': re.compile(r'\b(?:\d{4}[-.\s]?){3}\d{4}\b'),
        'mrn': re.compile(r'\b(?:MRN|mrn|Medical Record)\s*[:=#]?\s*\d{6,10}\b', re.IGNORECASE),
        'dob': re.compile(r'\b(?:DOB|dob|birth\s*date)\s*[:=]?\s*\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b', re.IGNORECASE),
        'patient_name': re.compile(r'\b(?:patient|subject)\s+(?:name|id)\s*[:=]\s*[A-Za-z]+\s+[A-Za-z]+\b', re.IGNORECASE),
    }

    # SQL injection patterns
    SQL_INJECTION_PATTERNS = {
        'union_select': re.compile(r'\bUNION\s+(?:ALL\s+)?SELECT\b', re.IGNORECASE),
        'drop_table': re.compile(r'\bDROP\s+(?:TABLE|DATABASE|INDEX)\b', re.IGNORECASE),
        'delete_from': re.compile(r'\bDELETE\s+FROM\b', re.IGNORECASE),
        'insert_into': re.compile(r'\bINSERT\s+INTO\b', re.IGNORECASE),
        'update_set': re.compile(r'\bUPDATE\s+\w+\s+SET\b', re.IGNORECASE),
        'semicolon_command': re.compile(r';\s*(?:DROP|DELETE|INSERT|UPDATE|TRUNCATE|ALTER|CREATE)\b', re.IGNORECASE),
        'or_true': re.compile(r"'\s*OR\s+['\d]\s*=\s*['\d]", re.IGNORECASE),
        'comment_injection': re.compile(r'(?:--|#|/\*)\s*(?:DROP|DELETE|SELECT)', re.IGNORECASE),
        'exec_command': re.compile(r'\b(?:EXEC|EXECUTE|xp_)\w*\b', re.IGNORECASE),
        'info_schema': re.compile(r'\bINFORMATION_SCHEMA\b', re.IGNORECASE),
    }

    # Prompt injection patterns
    PROMPT_INJECTION_PATTERNS = {
        'ignore_instructions': re.compile(r'ignore\s+(?:previous|all|above|prior)\s+instructions?', re.IGNORECASE),
        'new_instructions': re.compile(r'(?:new|different|override)\s+instructions?', re.IGNORECASE),
        'system_prompt': re.compile(r'(?:system|base)\s+prompt', re.IGNORECASE),
        'jailbreak': re.compile(r'(?:jailbreak|bypass|escape)\s+(?:mode|filter|safety)', re.IGNORECASE),
        'pretend': re.compile(r'pretend\s+(?:you\s+are|to\s+be)', re.IGNORECASE),
        'roleplay': re.compile(r'(?:roleplay|act)\s+as\s+(?:a|an)', re.IGNORECASE),
        'dan_mode': re.compile(r'\bDAN\s+mode\b', re.IGNORECASE),
        'reveal_prompt': re.compile(r'(?:show|reveal|display)\s+(?:your\s+)?(?:prompt|instructions)', re.IGNORECASE),
    }

    def __init__(self, config: SanitizerConfig = None):
        """
        Initialize the input sanitizer.

        Args:
            config: Configuration options
        """
        self.config = config or SanitizerConfig()

    def sanitize(self, query: str) -> SanitizationResult:
        """
        Sanitize user input.

        Args:
            query: User's raw query

        Returns:
            SanitizationResult with safety status and details
        """
        if not query or not query.strip():
            return SanitizationResult(
                is_safe=False,
                sanitized_query="",
                original_query=query or "",
                blocked_reason="Empty query"
            )

        detected_patterns = []
        warnings = []

        # Check length
        if len(query) > self.config.max_query_length:
            return SanitizationResult(
                is_safe=False,
                sanitized_query="",
                original_query=query,
                blocked_reason=f"Query exceeds maximum length ({self.config.max_query_length} chars)"
            )

        # Check PHI/PII
        if self.config.block_phi:
            phi_found = self._check_phi(query)
            if phi_found:
                detected_patterns.extend(phi_found)
                return SanitizationResult(
                    is_safe=False,
                    sanitized_query="",
                    original_query=query,
                    blocked_reason=f"PHI/PII detected: {', '.join(phi_found)}",
                    detected_patterns=detected_patterns
                )

        # Check SQL injection
        if self.config.block_sql_injection:
            sql_injection_found = self._check_sql_injection(query)
            if sql_injection_found:
                detected_patterns.extend(sql_injection_found)
                logger.warning(f"SQL injection attempt blocked: {sql_injection_found}")
                return SanitizationResult(
                    is_safe=False,
                    sanitized_query="",
                    original_query=query,
                    blocked_reason=f"SQL injection pattern detected: {', '.join(sql_injection_found)}",
                    detected_patterns=detected_patterns
                )

        # Check prompt injection
        if self.config.block_prompt_injection:
            prompt_injection_found = self._check_prompt_injection(query)
            if prompt_injection_found:
                detected_patterns.extend(prompt_injection_found)
                logger.warning(f"Prompt injection attempt blocked: {prompt_injection_found}")
                return SanitizationResult(
                    is_safe=False,
                    sanitized_query="",
                    original_query=query,
                    blocked_reason=f"Prompt injection pattern detected: {', '.join(prompt_injection_found)}",
                    detected_patterns=detected_patterns
                )

        # Check custom blocklist
        if self.config.custom_blocklist:
            blocklist_found = self._check_blocklist(query)
            if blocklist_found:
                detected_patterns.extend(blocklist_found)
                return SanitizationResult(
                    is_safe=False,
                    sanitized_query="",
                    original_query=query,
                    blocked_reason=f"Blocked terms found: {', '.join(blocklist_found)}",
                    detected_patterns=detected_patterns
                )

        # Clean and normalize query
        sanitized = self._normalize_query(query)

        # Check for suspicious but not blocking patterns
        if self._has_unusual_characters(query):
            warnings.append("Query contains unusual characters that were normalized")

        return SanitizationResult(
            is_safe=True,
            sanitized_query=sanitized,
            original_query=query,
            detected_patterns=detected_patterns,
            warnings=warnings
        )

    def _check_phi(self, query: str) -> List[str]:
        """Check for PHI/PII patterns."""
        found = []
        for name, pattern in self.PHI_PATTERNS.items():
            if pattern.search(query):
                found.append(f"PHI:{name}")
        return found

    def _check_sql_injection(self, query: str) -> List[str]:
        """Check for SQL injection patterns."""
        found = []
        for name, pattern in self.SQL_INJECTION_PATTERNS.items():
            if pattern.search(query):
                found.append(f"SQL:{name}")
        return found

    def _check_prompt_injection(self, query: str) -> List[str]:
        """Check for prompt injection patterns."""
        found = []
        for name, pattern in self.PROMPT_INJECTION_PATTERNS.items():
            if pattern.search(query):
                found.append(f"PROMPT:{name}")
        return found

    def _check_blocklist(self, query: str) -> List[str]:
        """Check for custom blocklist terms."""
        query_lower = query.lower()
        found = []
        for term in self.config.custom_blocklist:
            if term.lower() in query_lower:
                found.append(term)
        return found

    def _normalize_query(self, query: str) -> str:
        """Normalize query for processing."""
        # Strip whitespace
        query = query.strip()

        # Normalize multiple spaces
        query = re.sub(r'\s+', ' ', query)

        # Remove null bytes
        query = query.replace('\x00', '')

        return query

    def _has_unusual_characters(self, query: str) -> bool:
        """Check for unusual characters."""
        # Allow alphanumeric, common punctuation, and spaces
        normal_pattern = re.compile(r'^[\w\s.,;:!?\'"()\-+%@#$&*/=<>]+$', re.UNICODE)
        return not normal_pattern.match(query)

    def is_safe(self, query: str) -> bool:
        """Quick check if query is safe."""
        result = self.sanitize(query)
        return result.is_safe
