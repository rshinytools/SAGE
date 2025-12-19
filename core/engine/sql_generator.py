# SAGE - SQL Generator
# =====================
"""
SQL Generator
=============
Generates SQL queries using the LLM.

Supports providers:
- Claude (Anthropic API) - Primary
- Mock (testing)

This is STEP 5 of the 9-step pipeline.
"""

import re
import time
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from .models import GeneratedSQL, LLMContext
from .llm_providers import (
    LLMConfig, LLMProvider, LLMRequest, LLMResponse,
    BaseLLMProvider, create_llm_provider, get_current_provider,
    set_provider, get_available_providers
)

logger = logging.getLogger(__name__)


# =============================================================================
# ERROR CLASSES
# =============================================================================

class LLMError(Exception):
    """Base exception for LLM-related errors."""
    pass


class LLMTimeoutError(LLMError):
    """Raised when LLM request times out."""

    def __init__(self, model: str, timeout: int):
        self.model = model
        self.timeout = timeout
        super().__init__(
            f"The AI model ({model}) is taking longer than expected. "
            f"Timeout after {timeout} seconds. "
            "Try a simpler question or try again later."
        )


class LLMConnectionError(LLMError):
    """Raised when cannot connect to LLM service."""

    def __init__(self, host: str, original_error: str = None):
        self.host = host
        self.original_error = original_error
        super().__init__(
            f"Cannot connect to the AI service at {host}. "
            "The service may be starting up or unavailable. "
            "Please try again in a moment."
        )


class LLMModelError(LLMError):
    """Raised when the model returns an invalid response."""

    def __init__(self, model: str, reason: str = None):
        self.model = model
        self.reason = reason
        super().__init__(
            f"The AI model ({model}) could not generate a valid SQL query. "
            f"Reason: {reason or 'Unknown'}. "
            "Try rephrasing your question."
        )


# =============================================================================
# RESULT DATACLASS
# =============================================================================

@dataclass
class GenerationResult:
    """Result of SQL generation from the LLM."""
    sql: Optional[str] = None
    raw_response: str = ""
    model_used: str = ""
    generation_time_ms: float = 0.0
    success: bool = False
    error: Optional[str] = None
    reasoning: Optional[str] = None


# =============================================================================
# MOCK SQL GENERATOR (FOR TESTING)
# =============================================================================

class MockSQLGenerator:
    """
    Mock SQL generator for testing without LLM.

    Generates basic SQL based on context patterns.
    """

    def __init__(self):
        """Initialize mock generator."""
        pass

    def generate(self, context: LLMContext, retry_count: int = 2) -> GeneratedSQL:
        """Generate mock SQL."""
        start_time = time.time()

        # Parse context to generate appropriate SQL
        sql = self._generate_mock_sql(context)
        reasoning = "Mock generation based on context patterns"

        generation_time = (time.time() - start_time) * 1000

        return GeneratedSQL(
            sql=sql,
            reasoning=reasoning,
            tables_used=self._extract_tables(sql),
            columns_used=self._extract_columns(sql),
            filters_applied=self._extract_filters(sql),
            generation_time_ms=generation_time,
            model_used="mock",
            raw_response=None
        )

    def _generate_mock_sql(self, context: LLMContext) -> str:
        """Generate mock SQL from context."""
        # Extract table from system prompt
        table_match = re.search(r'Use table:\s*(\w+)', context.system_prompt)
        table = table_match.group(1) if table_match else "ADAE"

        # Extract population filter
        filter_match = re.search(r'Apply population filter:\s*(.+?)(?:\n|$)',
                                 context.system_prompt)
        pop_filter = filter_match.group(1).strip() if filter_match else None

        # Check if it's a count query
        is_count = 'how many' in context.user_prompt.lower() or 'count' in context.user_prompt.lower()

        # Build SQL
        if is_count:
            sql = f"SELECT COUNT(DISTINCT USUBJID) as subject_count FROM {table}"
        else:
            sql = f"SELECT * FROM {table}"

        # Add WHERE clause
        conditions = []
        if pop_filter and pop_filter != 'None required':
            conditions.append(pop_filter)

        # Look for entity conditions
        entity_matches = re.findall(r"Use (\w+) = '(\w+)'", context.user_prompt)
        for col, val in entity_matches:
            conditions.append(f"{col} = '{val}'")

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        return sql

    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL."""
        tables = []
        from_match = re.search(r'\bFROM\s+(\w+)', sql, re.IGNORECASE)
        if from_match:
            tables.append(from_match.group(1).upper())
        join_matches = re.findall(r'\bJOIN\s+(\w+)', sql, re.IGNORECASE)
        tables.extend([t.upper() for t in join_matches])
        return list(set(tables))

    def _extract_columns(self, sql: str) -> List[str]:
        """Extract column names from SQL."""
        col_pattern = re.compile(r'\b([A-Z][A-Z0-9_]*)\b')
        matches = col_pattern.findall(sql.upper())
        keywords = {'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'JOIN', 'LEFT', 'RIGHT',
                   'INNER', 'OUTER', 'ON', 'AS', 'COUNT', 'DISTINCT', 'GROUP', 'BY',
                   'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'ALL', 'IN', 'NOT',
                   'NULL', 'IS', 'LIKE', 'BETWEEN', 'EXISTS', 'CASE', 'WHEN', 'THEN',
                   'ELSE', 'END', 'ASC', 'DESC', 'SUM', 'AVG', 'MIN', 'MAX', 'TRUE',
                   'FALSE', 'Y', 'N'}
        return list(set(c for c in matches if c not in keywords))

    def _extract_filters(self, sql: str) -> List[str]:
        """Extract WHERE clause filters."""
        where_match = re.search(r'\bWHERE\s+(.*?)(?:GROUP|ORDER|LIMIT|$)',
                                sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1).strip()
            conditions = re.split(r'\s+AND\s+|\s+OR\s+', where_clause, flags=re.IGNORECASE)
            return [c.strip() for c in conditions if c.strip()]
        return []

    def is_available(self) -> bool:
        """Mock is always available."""
        return True


# =============================================================================
# UNIFIED SQL GENERATOR (Multi-Provider)
# =============================================================================

class UnifiedSQLGenerator:
    """
    Unified SQL generator that supports multiple LLM providers.

    Uses the LLM provider abstraction to support:
    - Claude (Anthropic API)
    - Mock (testing)

    Includes safety auditing for external API calls.

    Example:
        generator = UnifiedSQLGenerator()  # Uses env config
        result = generator.generate(context)
    """

    def __init__(self, config: Optional[LLMConfig] = None, provider: Optional[BaseLLMProvider] = None):
        """
        Initialize unified generator.

        Args:
            config: LLM configuration (uses env if not provided)
            provider: Pre-configured provider (creates from config if not provided)
        """
        self.config = config or LLMConfig.from_env()
        self._provider = provider

    @property
    def provider(self) -> BaseLLMProvider:
        """Get the LLM provider (creates if needed)."""
        if self._provider is None:
            self._provider = create_llm_provider(self.config)
        return self._provider

    def set_provider(self, provider_type: LLMProvider, **kwargs):
        """
        Change the LLM provider at runtime.

        Args:
            provider_type: New provider to use
            **kwargs: Additional config options
        """
        self.config.provider = provider_type
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        self._provider = create_llm_provider(self.config)
        logger.info(f"SQL generator switched to provider: {provider_type.value}")

    def generate(self,
                 context: LLMContext,
                 retry_count: int = 2
                ) -> GeneratedSQL:
        """
        Generate SQL from context using configured provider.

        Args:
            context: LLM context with prompts and schema
            retry_count: Number of retries on failure

        Returns:
            GeneratedSQL with query and reasoning
        """
        start_time = time.time()

        # Build the prompt
        full_prompt = self._build_full_prompt(context)

        # Create LLM request
        request = LLMRequest(
            prompt=full_prompt,
            system_prompt=context.system_prompt,
            max_tokens=self.config.max_tokens,
            temperature=self.config.temperature
        )

        # Try with retries
        last_error = None
        for attempt in range(retry_count + 1):
            try:
                response = self.provider.generate(request)

                if response and response.content:
                    sql, reasoning = self._parse_response(response.content)

                    if sql:
                        generation_time = (time.time() - start_time) * 1000

                        return GeneratedSQL(
                            sql=sql,
                            reasoning=reasoning,
                            tables_used=self._extract_tables(sql),
                            columns_used=self._extract_columns(sql),
                            filters_applied=self._extract_filters(sql),
                            generation_time_ms=generation_time,
                            model_used=response.model,
                            raw_response=response.content
                        )
                    else:
                        logger.warning(f"Attempt {attempt + 1}: No SQL found in response")

            except ValueError as e:
                # Safety layer blocked - don't retry
                logger.error(f"Safety layer blocked request: {e}")
                raise

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                last_error = e

        # All retries failed
        if last_error:
            raise last_error
        raise RuntimeError("SQL generation failed: no valid SQL in response")

    def _build_full_prompt(self, context: LLMContext) -> str:
        """Build the complete prompt for the LLM."""
        parts = [
            context.schema_context,
            "",
            context.entity_context,
            "",
            context.clinical_rules,
            "",
            context.user_prompt
        ]
        return "\n".join(p for p in parts if p)

    def _parse_response(self, response: str) -> tuple:
        """Parse LLM response to extract SQL and reasoning."""
        sql = ""
        reasoning = ""

        # Extract thinking/reasoning
        think_match = re.search(r'<think>(.*?)</think>', response, re.DOTALL)
        if think_match:
            reasoning = think_match.group(1).strip()

        # Extract SQL from code block
        sql_match = re.search(r'```sql\s*(.*?)\s*```', response, re.DOTALL | re.IGNORECASE)
        if sql_match:
            sql = sql_match.group(1).strip()
        else:
            # Try without language specifier
            sql_match = re.search(r'```\s*(SELECT.*?)\s*```', response, re.DOTALL | re.IGNORECASE)
            if sql_match:
                sql = sql_match.group(1).strip()
            else:
                # Try bare SELECT
                sql_match = re.search(r'(SELECT\s+.*?(?:;|$))', response, re.DOTALL | re.IGNORECASE)
                if sql_match:
                    sql = sql_match.group(1).strip()

        sql = self._clean_sql(sql)
        return sql, reasoning

    def _clean_sql(self, sql: str) -> str:
        """Clean and normalize SQL."""
        if not sql:
            return ""

        sql = sql.rstrip(';').strip()
        sql = re.sub(r'\s+', ' ', sql)

        keywords = ['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'JOIN', 'LEFT', 'RIGHT',
                   'INNER', 'OUTER', 'ON', 'AS', 'COUNT', 'DISTINCT', 'GROUP', 'BY',
                   'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'ALL', 'IN', 'NOT',
                   'NULL', 'IS', 'LIKE', 'BETWEEN', 'EXISTS', 'CASE', 'WHEN', 'THEN',
                   'ELSE', 'END', 'ASC', 'DESC', 'SUM', 'AVG', 'MIN', 'MAX']

        for kw in keywords:
            pattern = re.compile(r'\b' + kw + r'\b', re.IGNORECASE)
            sql = pattern.sub(kw, sql)

        return sql

    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL."""
        tables = []
        from_match = re.search(r'\bFROM\s+(\w+)', sql, re.IGNORECASE)
        if from_match:
            tables.append(from_match.group(1).upper())
        join_matches = re.findall(r'\bJOIN\s+(\w+)', sql, re.IGNORECASE)
        tables.extend([t.upper() for t in join_matches])
        return list(set(tables))

    def _extract_columns(self, sql: str) -> List[str]:
        """Extract column names from SQL."""
        col_pattern = re.compile(r'\b([A-Z][A-Z0-9_]*)\b')
        matches = col_pattern.findall(sql.upper())
        keywords = {'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'JOIN', 'LEFT', 'RIGHT',
                   'INNER', 'OUTER', 'ON', 'AS', 'COUNT', 'DISTINCT', 'GROUP', 'BY',
                   'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'ALL', 'IN', 'NOT',
                   'NULL', 'IS', 'LIKE', 'BETWEEN', 'EXISTS', 'CASE', 'WHEN', 'THEN',
                   'ELSE', 'END', 'ASC', 'DESC', 'SUM', 'AVG', 'MIN', 'MAX', 'TRUE',
                   'FALSE', 'Y', 'N'}
        return list(set(c for c in matches if c not in keywords))

    def _extract_filters(self, sql: str) -> List[str]:
        """Extract WHERE clause filters."""
        where_match = re.search(r'\bWHERE\s+(.*?)(?:GROUP|ORDER|LIMIT|$)',
                                sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1).strip()
            conditions = re.split(r'\s+AND\s+|\s+OR\s+', where_clause, flags=re.IGNORECASE)
            return [c.strip() for c in conditions if c.strip()]
        return []

    def is_available(self) -> bool:
        """Check if current provider is available."""
        return self.provider.is_available()

    def get_provider_info(self) -> Dict[str, Any]:
        """Get information about current provider."""
        return {
            "provider": self.provider.get_provider_name(),
            "model": self.provider.get_model_name(),
            "available": self.provider.is_available(),
            "safety_audit_enabled": self.config.enable_safety_audit
        }


def create_sql_generator(config: Optional[LLMConfig] = None) -> UnifiedSQLGenerator:
    """
    Factory function to create an SQL generator.

    Args:
        config: LLM configuration (uses env if not provided)

    Returns:
        Configured UnifiedSQLGenerator
    """
    return UnifiedSQLGenerator(config)
