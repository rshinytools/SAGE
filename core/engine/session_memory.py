# SAGE - Session Memory Module
# =============================
"""
Session Memory
==============
Maintains conversation context for better understanding.

Key Capabilities:
- Remember previous queries and responses
- Resolve references ("those", "them", "that")
- Learn from corrections
- Maintain session context
"""

import logging
import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta
from collections import deque

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class QueryFilter:
    """A single filter condition from a query."""
    natural: str          # "ITT population", "age 65 or above"
    sql: str              # "ITTFL = 'Y'", "AGE >= 65"
    table: str            # "ADSL", "ADAE"
    column: Optional[str] = None  # "ITTFL", "AGE"


@dataclass
class QueryContext:
    """Rich context for a single query - used for follow-up understanding."""
    query: str                              # Original user query
    sql: str                                # Executed SQL
    table: str                              # Primary table used
    filters: List[QueryFilter] = field(default_factory=list)  # Parsed filters
    filters_natural: str = ""               # Human-readable: "ITT population, age >= 65"
    filters_sql: str = ""                   # SQL: "ITTFL = 'Y' AND AGE >= 65"
    result_description: str = ""            # "233 subjects in ITT population"
    result_count: Optional[int] = None      # 233
    population: Optional[str] = None        # "ITT", "Safety"
    success: bool = True


@dataclass
class ConversationTurn:
    """A single turn in the conversation."""
    query: str
    response_type: str  # answer, clarification, error, etc.
    answer: Optional[str] = None
    data_summary: Optional[str] = None  # Summary of data returned
    entities_mentioned: List[str] = field(default_factory=list)
    table_used: Optional[str] = None
    population_used: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    # Enhanced context for follow-ups
    query_context: Optional[QueryContext] = None


@dataclass
class Correction:
    """A user correction to learn from."""
    original_query: str
    original_answer: str
    correction: str
    learned_preference: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class SessionContext:
    """Current session context."""
    # Last referenced entities
    last_entities: List[str] = field(default_factory=list)

    # Last table/population used
    last_table: Optional[str] = None
    last_population: Optional[str] = None

    # Last count result (for "of those" references)
    last_count: Optional[int] = None
    last_subject_set: Optional[str] = None  # Description of subjects

    # Last SQL query and criteria (for "list them" type follow-ups)
    last_sql: Optional[str] = None
    last_query: Optional[str] = None  # The original user query

    # Enhanced: Last query context (rich context for follow-ups)
    last_query_context: Optional[QueryContext] = None

    # Enhanced: Accumulated filters across conversation (for REFINE queries)
    # Each refinement builds on ALL previous filters
    accumulated_filters: List[QueryFilter] = field(default_factory=list)

    # User preferences learned in session
    preferences: Dict[str, str] = field(default_factory=dict)

    def get_accumulated_filters_sql(self) -> str:
        """Get all accumulated filters as SQL WHERE conditions."""
        if not self.accumulated_filters:
            return ""
        return " AND ".join(f.sql for f in self.accumulated_filters)

    def get_accumulated_filters_natural(self) -> str:
        """Get all accumulated filters as human-readable description."""
        if not self.accumulated_filters:
            return ""
        return ", ".join(f.natural for f in self.accumulated_filters)

    def add_filter(self, filter: QueryFilter):
        """Add a filter to accumulated filters."""
        # Avoid duplicates
        for existing in self.accumulated_filters:
            if existing.sql == filter.sql:
                return
        self.accumulated_filters.append(filter)

    def clear_accumulated_filters(self):
        """Clear accumulated filters (for new query chains)."""
        self.accumulated_filters = []

    def to_dict(self) -> Dict[str, Any]:
        return {
            'last_entities': self.last_entities,
            'last_table': self.last_table,
            'last_population': self.last_population,
            'last_count': self.last_count,
            'last_subject_set': self.last_subject_set,
            'last_sql': self.last_sql,
            'last_query': self.last_query,
            'accumulated_filters': [
                {'natural': f.natural, 'sql': f.sql, 'table': f.table}
                for f in self.accumulated_filters
            ],
            'preferences': self.preferences
        }


# =============================================================================
# SESSION MEMORY
# =============================================================================

class SessionMemory:
    """
    Maintains conversation context for better understanding.
    """

    # Reference words that point to previous context
    REFERENCE_WORDS = {
        'pronouns': ['those', 'them', 'these', 'they', 'it', 'that'],
        'relative': ['of those', 'of them', 'among them', 'of these'],
        'continuation': ['also', 'additionally', 'what about', 'and']
    }

    # Maximum turns to remember
    MAX_HISTORY = 20

    def __init__(self, session_id: str = None):
        """
        Initialize session memory.

        Args:
            session_id: Unique session identifier
        """
        self.session_id = session_id or self._generate_session_id()
        self.history: deque = deque(maxlen=self.MAX_HISTORY)
        self.context = SessionContext()
        self.corrections: List[Correction] = []
        self.created_at = datetime.now()

    def _generate_session_id(self) -> str:
        """Generate a unique session ID."""
        import uuid
        return f"sess_{uuid.uuid4().hex[:12]}"

    def add_turn(
        self,
        query: str,
        response_type: str,
        answer: Optional[str] = None,
        data: Optional[List[Dict]] = None,
        entities: Optional[List[str]] = None,
        table: Optional[str] = None,
        population: Optional[str] = None,
        sql: Optional[str] = None,
        is_refinement: bool = False,
        filters: Optional[List[QueryFilter]] = None
    ):
        """
        Add a conversation turn.

        Args:
            query: User query
            response_type: Type of response (answer, error, etc.)
            answer: The answer text
            data: Data returned (if any)
            entities: Entities mentioned
            table: Table used
            population: Population used
            sql: SQL query executed (for follow-up context)
            is_refinement: Whether this is a refinement of previous query
            filters: Parsed filter conditions from this query
        """
        # Create summary of data
        data_summary = None
        result_count = None
        if data:
            if len(data) == 1 and len(data[0]) == 1:
                # Single value - likely a count
                key, value = list(data[0].items())[0]
                data_summary = f"{key}: {value}"
                if 'count' in key.lower():
                    self.context.last_count = value
                    result_count = value
            else:
                data_summary = f"{len(data)} records returned"
                result_count = len(data)

        # Store SQL for follow-up queries like "list them"
        if sql:
            self.context.last_sql = sql
            self.context.last_query = query

        # Build rich query context
        query_context = None
        if sql and response_type == 'answer':
            # Parse filters from SQL if not provided
            parsed_filters = filters or self._parse_filters_from_sql(sql, table or '')

            # Build result description
            result_desc = self._build_result_description(query, answer, result_count, population)

            query_context = QueryContext(
                query=query,
                sql=sql,
                table=table or '',
                filters=parsed_filters,
                filters_natural=", ".join(f.natural for f in parsed_filters) if parsed_filters else "",
                filters_sql=self._extract_where_clause(sql),
                result_description=result_desc,
                result_count=result_count,
                population=population,
                success=True
            )

            # Store as last query context
            self.context.last_query_context = query_context

            # Update accumulated filters
            # CRITICAL FIX: Don't rely on is_refinement flag (depends on LLM classification)
            # Instead, use a smarter approach:
            # - Keep accumulating filters across the conversation
            # - Only clear when explicitly starting a new topic (detected by lack of context references)
            # - The add_filter() method already prevents duplicates
            #
            # The previous logic (clearing when not is_refinement) caused filter loss
            # when the LLM misclassified a refinement query as a new query.
            #
            # Filters will be naturally cleared when:
            # - Session times out
            # - User explicitly starts a new conversation
            # - User says "new question" or similar

            # Add new filters to accumulated (add_filter prevents duplicates)
            if parsed_filters:
                for f in parsed_filters:
                    self.context.add_filter(f)

        turn = ConversationTurn(
            query=query,
            response_type=response_type,
            answer=answer,
            data_summary=data_summary,
            entities_mentioned=entities or [],
            table_used=table,
            population_used=population,
            query_context=query_context
        )

        self.history.append(turn)

        # Update context
        self._update_context(turn)

        logger.info(f"Session {self.session_id}: Added turn {len(self.history)}, "
                   f"accumulated_filters={len(self.context.accumulated_filters)}")

    def _parse_filters_from_sql(self, sql: str, table: str) -> List[QueryFilter]:
        """Parse filter conditions from SQL WHERE clause."""
        filters = []
        where_clause = self._extract_where_clause(sql)
        if not where_clause:
            return filters

        # Common clinical filters to detect
        # Note: Patterns must handle UPPER() wrapping from sql_validator
        # e.g., UPPER(SAFFL) = UPPER('Y') as well as SAFFL = 'Y'
        filter_patterns = [
            # Population filters (with optional UPPER wrapping)
            (r"(?:UPPER\()?\s*ITTFL\s*\)?\s*=\s*(?:UPPER\()?\s*'Y'\s*\)?", "ITT population", "ITTFL", "ITTFL = 'Y'"),
            (r"(?:UPPER\()?\s*SAFFL\s*\)?\s*=\s*(?:UPPER\()?\s*'Y'\s*\)?", "Safety population", "SAFFL", "SAFFL = 'Y'"),
            (r"(?:UPPER\()?\s*EFFFL\s*\)?\s*=\s*(?:UPPER\()?\s*'Y'\s*\)?", "Efficacy population", "EFFFL", "EFFFL = 'Y'"),
            (r"(?:UPPER\()?\s*PPROTFL\s*\)?\s*=\s*(?:UPPER\()?\s*'Y'\s*\)?", "Per-protocol population", "PPROTFL", "PPROTFL = 'Y'"),
            # Demographics (AGE doesn't get UPPER wrapped, SEX does)
            (r"AGE\s*>=\s*(\d+)", "age {0} or above", "AGE", None),
            (r"AGE\s*>\s*(\d+)", "age above {0}", "AGE", None),
            (r"AGE\s*<=\s*(\d+)", "age {0} or below", "AGE", None),
            (r"AGE\s*<\s*(\d+)", "age below {0}", "AGE", None),
            (r"(?:UPPER\()?\s*SEX\s*\)?\s*=\s*(?:UPPER\()?\s*'([MF])'\s*\)?", "{0} subjects", "SEX", None),
            # Adverse events (with optional UPPER wrapping)
            (r"(?:UPPER\()?\s*AESER\s*\)?\s*=\s*(?:UPPER\()?\s*'Y'\s*\)?", "serious adverse events", "AESER", "AESER = 'Y'"),
            (r"(?:UPPER\()?\s*AEREL\s*\)?\s*(?:LIKE|=)\s*(?:UPPER\()?\s*'%?RELATED%?'\s*\)?", "drug-related events", "AEREL", None),
            (r"(?:UPPER\()?\s*AEDECOD\s*\)?\s*=\s*(?:UPPER\()?\s*'([^']+)'\s*\)?", "adverse event: {0}", "AEDECOD", None),
            # AEDECOD IN clause - match whole clause with nested UPPER()
            # Use positive lookahead to find the closing ) after all UPPER() calls
            (r"(?:UPPER\()?\s*AEDECOD\s*\)?\s*IN\s*\([^)]*(?:\)[^)]*)*\)", "adverse event filter", "AEDECOD", None),
            (r"(?:UPPER\()?\s*AELLT\s*\)?\s*=\s*(?:UPPER\()?\s*'([^']+)'\s*\)?", "adverse event: {0}", "AELLT", None),
            # AELLT IN clause
            (r"(?:UPPER\()?\s*AELLT\s*\)?\s*IN\s*\([^)]*(?:\)[^)]*)*\)", "adverse event filter", "AELLT", None),
            (r"ATOXGR\s*>=\s*(\d+)", "Grade {0}+ toxicity", "ATOXGR", None),
            (r"(?:UPPER\()?\s*TRTEMFL\s*\)?\s*=\s*(?:UPPER\()?\s*'Y'\s*\)?", "treatment-emergent events", "TRTEMFL", "TRTEMFL = 'Y'"),
        ]

        for pattern, natural_template, column, canonical_sql in filter_patterns:
            match = re.search(pattern, where_clause, re.IGNORECASE)
            if match:
                # Build natural description
                if match.groups():
                    natural = natural_template.format(*match.groups())
                else:
                    natural = natural_template

                # Get the SQL fragment - use canonical form if provided, otherwise matched text
                if canonical_sql:
                    sql_fragment = canonical_sql
                else:
                    # For patterns with captured groups, rebuild the SQL
                    sql_fragment = match.group(0)
                    # Clean up UPPER() wrapping for storage - we want clean SQL for re-injection
                    sql_fragment = re.sub(r'UPPER\(([^)]+)\)', r'\1', sql_fragment)
                    sql_fragment = sql_fragment.strip()

                filters.append(QueryFilter(
                    natural=natural,
                    sql=sql_fragment,
                    table=table,
                    column=column
                ))

        return filters

    def _extract_where_clause(self, sql: str) -> str:
        """Extract WHERE clause from SQL."""
        match = re.search(r'WHERE\s+(.+?)(?:\s+LIMIT|\s+ORDER|\s+GROUP|\s*$)',
                         sql, re.IGNORECASE | re.DOTALL)
        return match.group(1).strip() if match else ""

    def _build_result_description(self, query: str, answer: str, count: int,
                                   population: str) -> str:
        """Build a human-readable result description."""
        if not answer:
            return ""

        # Strip markdown
        clean_answer = re.sub(r'\*\*', '', answer)

        # Try to extract meaningful description
        if count is not None:
            if population:
                return f"{count} subjects in {population}"
            else:
                # Try to infer from answer
                return clean_answer[:100] if len(clean_answer) > 100 else clean_answer

        return clean_answer[:100] if len(clean_answer) > 100 else clean_answer

    def _update_context(self, turn: ConversationTurn):
        """Update context from the latest turn."""
        # Update last entities
        if turn.entities_mentioned:
            self.context.last_entities = turn.entities_mentioned

        # Update last table/population
        if turn.table_used:
            self.context.last_table = turn.table_used
        if turn.population_used:
            self.context.last_population = turn.population_used

        # Update last subject set description
        if turn.answer and turn.response_type == 'answer':
            # Strip markdown formatting first (** for bold)
            clean_answer = re.sub(r'\*\*', '', turn.answer)

            # Extract subject description from answer
            # E.g., "21 subjects reported Headache." -> "subjects who reported Headache"
            # Also handles "There are 232 subjects in the safety population."
            patterns = [
                # "X subjects reported Y" -> "subjects who reported Y"
                r'(\d+)\s+(subjects?|patients?)\s+reported\s+(.+?)\.?$',
                # "X subjects had Y" -> "subjects who had Y"
                r'(\d+)\s+(subjects?|patients?)\s+had\s+(.+?)\.?$',
                # "There are X subjects in Y" -> "subjects in Y"
                r'There are\s+(\d+)\s+(subjects?|patients?)\s+in\s+(.+?)\.?$',
                # "X subjects matching Y" -> "subjects matching Y"
                r'(\d+)\s+(subjects?|patients?)\s+(.+?)\.?$',
            ]

            for pattern in patterns:
                match = re.search(pattern, clean_answer, re.I)
                if match:
                    self.context.last_count = int(match.group(1))
                    action = match.group(3).strip().rstrip('.')
                    # Build description
                    if 'reported' in clean_answer.lower():
                        self.context.last_subject_set = f"subjects who reported {action}"
                    elif 'had' in clean_answer.lower():
                        self.context.last_subject_set = f"subjects who had {action}"
                    elif ' in ' in clean_answer.lower():
                        self.context.last_subject_set = f"subjects in {action}"
                    else:
                        self.context.last_subject_set = f"subjects {action}"
                    break

    def resolve_references(self, query: str) -> Tuple[str, bool]:
        """
        Resolve references to previous context.

        NOTE: This method no longer uses pattern matching. Reference resolution
        is now handled by the LLM-based QueryAnalyzer which can understand
        the infinite ways users might phrase follow-up questions.

        The QueryAnalyzer detects intents like DETAIL_PREVIOUS (list them) and
        REFINE_PREVIOUS (of those), and the pipeline handles the transformation.

        This method is kept for backward compatibility but just returns
        the original query unchanged.

        Args:
            query: User query

        Returns:
            Tuple of (resolved query, was_modified=False)
        """
        # Pattern matching removed - LLM handles this now via QueryAnalyzer
        # The LLM can understand thousands of ways to say the same thing
        # See query_analyzer.py CONTEXT_AWARE_ANALYSIS_PROMPT for follow-up detection
        logger.debug(f"resolve_references called (LLM handles this now): '{query}'")
        return query, False

    def _transform_count_to_select(self, count_sql: str) -> Optional[str]:
        """
        Transform a COUNT SQL query to a SELECT query that lists subjects.

        This preserves ALL filter criteria from the original query, including JOINs.

        Examples:
            Simple query:
                Input:  SELECT COUNT(DISTINCT USUBJID) FROM ADAE WHERE SAFFL='Y' AND AEDECOD='Headache'
                Output: SELECT DISTINCT USUBJID, AEDECOD, ATOXGR FROM ADAE WHERE SAFFL='Y' AND AEDECOD='Headache'

            JOIN query:
                Input:  SELECT COUNT(DISTINCT a.USUBJID) FROM ADSL a JOIN ADAE e ON a.USUBJID = e.USUBJID WHERE a.SAFFL = 'Y' AND e.AELLT = 'Diarrhea'
                Output: SELECT DISTINCT a.USUBJID, a.AGE, e.AELLT FROM ADSL a JOIN ADAE e ON a.USUBJID = e.USUBJID WHERE a.SAFFL = 'Y' AND e.AELLT = 'Diarrhea'

        Args:
            count_sql: A COUNT SQL query

        Returns:
            A SELECT SQL query listing subjects, or None if transformation fails
        """
        if not count_sql:
            return None

        sql_upper = count_sql.upper()

        # Only transform COUNT queries
        if 'COUNT(' not in sql_upper:
            return None

        try:
            # Check if this is a JOIN query
            is_join_query = 'JOIN' in sql_upper

            if is_join_query:
                # For JOIN queries, preserve the entire FROM...JOIN structure
                # Extract: FROM table1 alias1 JOIN table2 alias2 ON condition
                from_join_match = re.search(
                    r'(FROM\s+\w+\s+\w+\s+JOIN\s+\w+\s+\w+\s+ON\s+[^W]+)',
                    count_sql,
                    re.IGNORECASE | re.DOTALL
                )

                if from_join_match:
                    from_join_clause = from_join_match.group(1).strip()
                else:
                    # Fallback: extract everything from FROM to WHERE
                    from_match = re.search(r'(FROM\s+.+?)(?=\s+WHERE|\s*$)', count_sql, re.IGNORECASE | re.DOTALL)
                    from_join_clause = from_match.group(1).strip() if from_match else None

                if not from_join_clause:
                    logger.warning("Could not extract FROM/JOIN clause from JOIN query")
                    return None

                # Extract table aliases
                # Pattern: FROM ADSL a JOIN ADAE e
                alias_match = re.search(r'FROM\s+(\w+)\s+(\w+)\s+JOIN\s+(\w+)\s+(\w+)', count_sql, re.IGNORECASE)
                if alias_match:
                    table1, alias1, table2, alias2 = alias_match.groups()
                    table1_upper = table1.upper()
                    table2_upper = table2.upper()

                    # Build SELECT columns with aliases
                    select_cols = f"DISTINCT {alias1}.USUBJID"

                    # Add columns from table1 (demographics)
                    if table1_upper in ['ADSL', 'DM']:
                        select_cols += f", {alias1}.AGE, {alias1}.SEX"

                    # Add columns from table2 (adverse events)
                    if table2_upper in ['ADAE', 'AE']:
                        select_cols += f", {alias2}.AEDECOD, {alias2}.AELLT, {alias2}.AESER"
                    elif table2_upper in ['ADSL', 'DM']:
                        select_cols += f", {alias2}.AGE, {alias2}.SEX"
                else:
                    # Fallback if alias pattern doesn't match
                    select_cols = "DISTINCT USUBJID"

                # Extract WHERE clause
                where_match = re.search(r'(WHERE\s+.+?)(?:\s+LIMIT|\s+ORDER|\s+GROUP|\s*$)', count_sql, re.IGNORECASE | re.DOTALL)
                where_clause = where_match.group(1).strip() if where_match else ""

                # Remove LIMIT from WHERE clause if present
                if where_clause:
                    where_clause = re.sub(r'\s+LIMIT\s+\d+', '', where_clause, flags=re.IGNORECASE)

                # Build final query preserving JOIN
                list_sql = f"SELECT {select_cols} {from_join_clause}"
                if where_clause:
                    list_sql += f" {where_clause}"
                list_sql += " LIMIT 100"

            else:
                # Simple query without JOIN
                # Note: The alias pattern must NOT match SQL keywords like WHERE, LIMIT, ORDER, GROUP
                table_match = re.search(
                    r'FROM\s+(\w+)(?:\s+(?!WHERE|LIMIT|ORDER|GROUP|HAVING)([a-zA-Z]\w*))?\s',
                    count_sql,
                    re.IGNORECASE
                )
                if not table_match:
                    return None

                table = table_match.group(1)
                alias = table_match.group(2)  # Will be None if no alias (or if alias would be a keyword)

                # Extract WHERE clause
                where_match = re.search(r'(WHERE\s+.+?)(?:\s+LIMIT|\s+ORDER|\s+GROUP|\s*$)', count_sql, re.IGNORECASE | re.DOTALL)
                where_clause = where_match.group(1).strip() if where_match else ""

                # Remove alias prefixes if no alias in FROM clause
                if where_clause and not alias:
                    where_clause = re.sub(r'\b[a-zA-Z]\s*\.\s*', '', where_clause)

                # Remove LIMIT from WHERE clause
                if where_clause:
                    where_clause = re.sub(r'\s+LIMIT\s+\d+', '', where_clause, flags=re.IGNORECASE)

                # Determine columns based on table type
                if table.upper() in ['ADAE', 'AE']:
                    select_cols = "DISTINCT USUBJID, AEDECOD, AELLT, ATOXGR, AESER"
                elif table.upper() in ['ADSL', 'DM']:
                    select_cols = "DISTINCT USUBJID, AGE, SEX, RACE"
                else:
                    select_cols = "DISTINCT USUBJID"

                # Build final query
                list_sql = f"SELECT {select_cols} FROM {table}"
                if alias:
                    list_sql += f" {alias}"
                if where_clause:
                    list_sql += f" {where_clause}"
                list_sql += " LIMIT 100"

            logger.info(f"Transformed COUNT to SELECT: {list_sql}")
            return list_sql

        except Exception as e:
            logger.warning(f"Failed to transform COUNT SQL: {e}")
            return None

    def get_conversation_context(self, max_turns: int = 3) -> str:
        """
        Get recent conversation context as text.

        Args:
            max_turns: Maximum turns to include

        Returns:
            Context string for LLM
        """
        if not self.history:
            return ""

        recent = list(self.history)[-max_turns:]
        context_lines = ["Recent conversation:"]

        for turn in recent:
            context_lines.append(f"User: {turn.query}")
            if turn.answer:
                # Truncate long answers
                answer = turn.answer[:200] + "..." if len(turn.answer) > 200 else turn.answer
                context_lines.append(f"SAGE: {answer}")

        return "\n".join(context_lines)

    def get_conversation_context_for_llm(self, max_turns: int = 5) -> str:
        """
        Build a comprehensive conversation context for LLM SQL generation.

        This provides the LLM with everything it needs to understand
        follow-up queries and preserve context across refinements.

        Args:
            max_turns: Maximum turns to include

        Returns:
            Formatted context string for LLM
        """
        if not self.history:
            return ""

        lines = []
        lines.append("=" * 60)
        lines.append("CONVERSATION CONTEXT (for understanding follow-up queries)")
        lines.append("=" * 60)

        # Show conversation history
        recent = list(self.history)[-max_turns:]
        for i, turn in enumerate(recent, 1):
            lines.append(f"\n--- Turn {i} ---")
            lines.append(f"User asked: \"{turn.query}\"")

            if turn.query_context:
                ctx = turn.query_context
                lines.append(f"Table used: {ctx.table}")
                if ctx.filters_natural:
                    lines.append(f"Filters applied: {ctx.filters_natural}")
                if ctx.result_description:
                    lines.append(f"Result: {ctx.result_description}")
                if ctx.filters_sql:
                    lines.append(f"SQL WHERE clause: {ctx.filters_sql}")
            elif turn.answer:
                answer = turn.answer[:150] + "..." if len(turn.answer) > 150 else turn.answer
                lines.append(f"Response: {answer}")

        # Show accumulated filters (critical for refinements)
        if self.context.accumulated_filters:
            lines.append("\n" + "=" * 60)
            lines.append("ACCUMULATED FILTERS (MUST be preserved in follow-up queries)")
            lines.append("=" * 60)
            lines.append(f"Natural language: {self.context.get_accumulated_filters_natural()}")
            lines.append(f"SQL conditions: {self.context.get_accumulated_filters_sql()}")

        # Show current context summary
        if self.context.last_query_context:
            ctx = self.context.last_query_context
            lines.append("\n" + "=" * 60)
            lines.append("MOST RECENT QUERY CONTEXT")
            lines.append("=" * 60)
            lines.append(f"Query: {ctx.query}")
            lines.append(f"Table: {ctx.table}")
            lines.append(f"Result: {ctx.result_description}")
            if ctx.population:
                lines.append(f"Population: {ctx.population}")

        return "\n".join(lines)

    def get_refinement_context(self) -> Dict[str, Any]:
        """
        Get context specifically for REFINE_PREVIOUS queries.

        Returns dict with:
        - accumulated_filters_sql: SQL to preserve
        - accumulated_filters_natural: Human description
        - last_table: Table to use
        - last_result: Description of last result
        - needs_join: Whether new query might need JOIN
        """
        return {
            'accumulated_filters_sql': self.context.get_accumulated_filters_sql(),
            'accumulated_filters_natural': self.context.get_accumulated_filters_natural(),
            'last_table': self.context.last_table,
            'last_population': self.context.last_population,
            'last_result_count': self.context.last_count,
            'last_result_description': (
                self.context.last_query_context.result_description
                if self.context.last_query_context else None
            ),
            'filter_count': len(self.context.accumulated_filters),
            'filters': [
                {'natural': f.natural, 'sql': f.sql, 'table': f.table}
                for f in self.context.accumulated_filters
            ]
        }

    def add_correction(
        self,
        original_query: str,
        original_answer: str,
        correction: str
    ):
        """
        Learn from a user correction.

        Args:
            original_query: The original query
            original_answer: The original (wrong) answer
            correction: User's correction
        """
        self.corrections.append(Correction(
            original_query=original_query,
            original_answer=original_answer,
            correction=correction
        ))

        # Try to learn a preference
        self._learn_preference(original_query, correction)

        logger.info(f"Learned correction: '{original_answer}' -> '{correction}'")

    def _learn_preference(self, query: str, correction: str):
        """Learn user preference from correction."""
        # Check for population preference
        if 'safety' in correction.lower():
            self.context.preferences['default_population'] = 'Safety'
        elif 'itt' in correction.lower():
            self.context.preferences['default_population'] = 'ITT'

        # Check for severity preference
        if 'serious' in correction.lower():
            self.context.preferences['include_severity'] = 'serious_only'

    def get_preferences(self) -> Dict[str, str]:
        """Get learned preferences for this session."""
        return self.context.preferences

    def has_context(self) -> bool:
        """Check if there is conversation context."""
        return len(self.history) > 0

    def get_last_turn(self) -> Optional[ConversationTurn]:
        """Get the last conversation turn."""
        if self.history:
            return self.history[-1]
        return None

    def get_last_table(self) -> Optional[str]:
        """Get the last table used."""
        return self.context.last_table

    def get_last_population(self) -> Optional[str]:
        """Get the last population used."""
        return self.context.last_population

    def clear(self):
        """Clear session memory."""
        self.history.clear()
        self.context = SessionContext()
        self.corrections.clear()
        logger.info(f"Session {self.session_id} cleared")

    def to_dict(self) -> Dict[str, Any]:
        """Convert session memory to dictionary."""
        return {
            'session_id': self.session_id,
            'created_at': self.created_at.isoformat(),
            'turn_count': len(self.history),
            'context': self.context.to_dict(),
            'corrections_count': len(self.corrections)
        }


# =============================================================================
# SESSION MANAGER
# =============================================================================

class SessionManager:
    """
    Manages multiple user sessions.
    """

    # Session timeout in minutes
    SESSION_TIMEOUT = 60

    def __init__(self):
        """Initialize session manager."""
        self.sessions: Dict[str, SessionMemory] = {}

    def get_session(self, session_id: str) -> SessionMemory:
        """
        Get or create a session.

        Args:
            session_id: Session identifier

        Returns:
            SessionMemory instance
        """
        if session_id not in self.sessions:
            self.sessions[session_id] = SessionMemory(session_id)
            logger.info(f"Created new session: {session_id}")

        return self.sessions[session_id]

    def create_session(self) -> SessionMemory:
        """Create a new session."""
        session = SessionMemory()
        self.sessions[session.session_id] = session
        return session

    def remove_session(self, session_id: str):
        """Remove a session."""
        if session_id in self.sessions:
            del self.sessions[session_id]
            logger.info(f"Removed session: {session_id}")

    def cleanup_expired_sessions(self):
        """Remove expired sessions."""
        cutoff = datetime.now() - timedelta(minutes=self.SESSION_TIMEOUT)
        expired = []

        for sid, session in self.sessions.items():
            if session.created_at < cutoff:
                expired.append(sid)

        for sid in expired:
            self.remove_session(sid)

        if expired:
            logger.info(f"Cleaned up {len(expired)} expired sessions")

    def get_active_session_count(self) -> int:
        """Get count of active sessions."""
        return len(self.sessions)


# Global session manager instance
_session_manager: Optional[SessionManager] = None


def get_session_manager() -> SessionManager:
    """Get the global session manager."""
    global _session_manager
    if _session_manager is None:
        _session_manager = SessionManager()
    return _session_manager
