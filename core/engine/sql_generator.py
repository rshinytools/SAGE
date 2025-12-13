# SAGE - SQL Generator
# =====================
"""
SQL Generator
=============
Generates SQL queries using the LLM (DeepSeek-R1 via Ollama).

This is STEP 5 of the 9-step pipeline.
"""

import re
import time
import logging
import requests
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from .models import GeneratedSQL, LLMContext

logger = logging.getLogger(__name__)


@dataclass
class OllamaConfig:
    """Configuration for Ollama API."""
    host: str = "http://localhost:11434"
    model: str = "deepseek-r1:8b"
    fallback_model: str = "llama3.1:8b-instruct-q8_0"
    timeout: int = 60
    temperature: float = 0.1
    max_tokens: int = 2000


class SQLGenerator:
    """
    Generates SQL using LLM.

    Uses Ollama API to communicate with DeepSeek-R1 or fallback model.
    Handles response parsing, retry logic, and error handling.

    Example:
        generator = SQLGenerator(ollama_host="http://localhost:11434")
        result = generator.generate(context)
        if result.sql:
            print(result.sql)
    """

    def __init__(self, config: Optional[OllamaConfig] = None):
        """
        Initialize SQL generator.

        Args:
            config: Ollama configuration
        """
        self.config = config or OllamaConfig()

    def generate(self,
                 context: LLMContext,
                 retry_count: int = 2
                ) -> GeneratedSQL:
        """
        Generate SQL from context.

        Args:
            context: LLM context with prompts and schema
            retry_count: Number of retries on failure

        Returns:
            GeneratedSQL with query and reasoning
        """
        start_time = time.time()

        # Build full prompt
        full_prompt = self._build_full_prompt(context)

        # Try primary model first
        for attempt in range(retry_count + 1):
            model = self.config.model if attempt == 0 else self.config.fallback_model

            try:
                response = self._call_ollama(full_prompt, model)

                if response:
                    sql, reasoning = self._parse_response(response)

                    if sql:
                        generation_time = (time.time() - start_time) * 1000

                        # Extract tables and columns from SQL
                        tables = self._extract_tables(sql)
                        columns = self._extract_columns(sql)
                        filters = self._extract_filters(sql)

                        return GeneratedSQL(
                            sql=sql,
                            reasoning=reasoning,
                            tables_used=tables,
                            columns_used=columns,
                            filters_applied=filters,
                            generation_time_ms=generation_time,
                            model_used=model,
                            raw_response=response
                        )

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed with {model}: {e}")
                if attempt == retry_count:
                    raise

        # Should not reach here
        raise RuntimeError("SQL generation failed after all retries")

    def _build_full_prompt(self, context: LLMContext) -> str:
        """Build the complete prompt for the LLM."""
        parts = [
            context.system_prompt,
            "",
            context.schema_context,
            "",
            context.entity_context,
            "",
            context.clinical_rules,
            "",
            context.user_prompt
        ]
        return "\n".join(parts)

    def _call_ollama(self, prompt: str, model: str) -> Optional[str]:
        """
        Call Ollama API.

        Args:
            prompt: Full prompt text
            model: Model to use

        Returns:
            Response text or None
        """
        url = f"{self.config.host}/api/generate"

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": self.config.temperature,
                "num_predict": self.config.max_tokens,
            }
        }

        try:
            response = requests.post(
                url,
                json=payload,
                timeout=self.config.timeout
            )
            response.raise_for_status()

            data = response.json()
            return data.get("response", "")

        except requests.exceptions.ConnectionError:
            logger.error(f"Cannot connect to Ollama at {self.config.host}")
            raise RuntimeError(f"Ollama not available at {self.config.host}")

        except requests.exceptions.Timeout:
            logger.error("Ollama request timed out")
            raise RuntimeError("LLM request timed out")

        except Exception as e:
            logger.error(f"Ollama API error: {e}")
            raise

    def _parse_response(self, response: str) -> tuple:
        """
        Parse LLM response to extract SQL and reasoning.

        Args:
            response: Raw LLM response

        Returns:
            Tuple of (sql, reasoning)
        """
        sql = ""
        reasoning = ""

        # Extract thinking/reasoning (DeepSeek-R1 style)
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
                # Try to find bare SELECT statement
                sql_match = re.search(r'(SELECT\s+.*?(?:;|$))', response, re.DOTALL | re.IGNORECASE)
                if sql_match:
                    sql = sql_match.group(1).strip()

        # Clean SQL
        sql = self._clean_sql(sql)

        return sql, reasoning

    def _clean_sql(self, sql: str) -> str:
        """Clean and normalize SQL."""
        if not sql:
            return ""

        # Remove trailing semicolon
        sql = sql.rstrip(';').strip()

        # Normalize whitespace
        sql = re.sub(r'\s+', ' ', sql)

        # Capitalize SQL keywords
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

        # FROM clause
        from_match = re.search(r'\bFROM\s+(\w+)', sql, re.IGNORECASE)
        if from_match:
            tables.append(from_match.group(1).upper())

        # JOIN clauses
        join_matches = re.findall(r'\bJOIN\s+(\w+)', sql, re.IGNORECASE)
        tables.extend([t.upper() for t in join_matches])

        return list(set(tables))

    def _extract_columns(self, sql: str) -> List[str]:
        """Extract column names from SQL."""
        columns = []

        # Simple extraction - find word patterns that look like columns
        # This is a basic implementation
        col_pattern = re.compile(r'\b([A-Z][A-Z0-9_]*)\b')
        matches = col_pattern.findall(sql.upper())

        # Filter out SQL keywords
        keywords = {'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'JOIN', 'LEFT', 'RIGHT',
                   'INNER', 'OUTER', 'ON', 'AS', 'COUNT', 'DISTINCT', 'GROUP', 'BY',
                   'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'ALL', 'IN', 'NOT',
                   'NULL', 'IS', 'LIKE', 'BETWEEN', 'EXISTS', 'CASE', 'WHEN', 'THEN',
                   'ELSE', 'END', 'ASC', 'DESC', 'SUM', 'AVG', 'MIN', 'MAX', 'TRUE',
                   'FALSE', 'Y', 'N'}

        columns = [c for c in matches if c not in keywords]

        return list(set(columns))

    def _extract_filters(self, sql: str) -> List[str]:
        """Extract WHERE clause filters."""
        filters = []

        # Find WHERE clause
        where_match = re.search(r'\bWHERE\s+(.*?)(?:GROUP|ORDER|LIMIT|$)',
                                sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1).strip()

            # Split by AND/OR
            conditions = re.split(r'\s+AND\s+|\s+OR\s+', where_clause, flags=re.IGNORECASE)
            filters = [c.strip() for c in conditions if c.strip()]

        return filters

    def is_available(self) -> bool:
        """Check if Ollama is available."""
        try:
            response = requests.get(f"{self.config.host}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False

    def list_models(self) -> List[str]:
        """List available models."""
        try:
            response = requests.get(f"{self.config.host}/api/tags", timeout=5)
            if response.status_code == 200:
                data = response.json()
                return [m['name'] for m in data.get('models', [])]
        except:
            pass
        return []


class MockSQLGenerator(SQLGenerator):
    """
    Mock SQL generator for testing without Ollama.

    Generates basic SQL based on context patterns.
    """

    def __init__(self):
        """Initialize mock generator."""
        super().__init__(OllamaConfig())

    def generate(self,
                 context: LLMContext,
                 retry_count: int = 2
                ) -> GeneratedSQL:
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
