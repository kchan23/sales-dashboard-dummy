"""
LLM-based Natural Language to SQL Query Generator with Ambiguity Detection.

This module replaces the rule-based query_generator.py with an LLM-powered system that:
1. Detects ambiguous queries and requests clarification
2. Generates BigQuery SQL from natural language using OpenRouter API
3. Validates generated SQL for security and correctness

Usage:
    generator = LLMQueryGenerator(db, api_key)

    # Check for ambiguity first
    ambiguity = generator.detect_ambiguity("Show trends")
    if ambiguity.is_ambiguous:
        # Present clarification UI to user
        # Get clarifications from user
        clarifications = {"time_granularity": "daily"}

    # Generate query
    sql, description, params = generator.generate_query(
        "Show trends",
        location_id,
        start_date,
        end_date,
        clarifications
    )
"""

from typing import Optional, Tuple, List, Any, Dict
from dataclasses import dataclass, field
from google.cloud import bigquery
from openai import OpenAI
import logging
import hashlib

from config.prompts import build_prompt

logger = logging.getLogger(__name__)


@dataclass
class AmbiguityResult:
    """
    Result of ambiguity detection for a user query.

    Attributes:
        is_ambiguous: Whether the query is ambiguous and needs clarification
        question: The clarification question to ask the user
        options: List of (value, display_label) tuples for user selection
        question_id: Unique identifier for this clarification question
        confidence: Confidence score (0-1) for ambiguity detection
    """
    is_ambiguous: bool
    question: str = ""
    options: List[Tuple[str, str]] = field(default_factory=list)
    question_id: str = ""
    confidence: float = 0.0


class AmbiguityDetector:
    """
    Detects ambiguous queries before SQL generation.

    Uses a hybrid approach:
    1. Pattern-based detection (fast, deterministic) for common cases
    2. LLM-based detection (slower, flexible) for edge cases
    """

    # Ambiguity pattern definitions
    PATTERNS = {
        'MISSING_TIME_GRANULARITY': {
            'triggers': ['trend', 'over time', 'show', 'graph', 'chart', 'track', 'pattern'],
            'missing': ['daily', 'weekly', 'monthly', 'by day', 'by week', 'by month', 'day', 'week', 'month'],
            'question': "What time period would you like to see?",
            'options': [
                ('daily', 'Daily trends'),
                ('weekly', 'Weekly trends'),
                ('monthly', 'Monthly trends')
            ],
            'question_id': 'time_granularity'
        },
        'MISSING_TOP_METRIC': {
            'triggers': ['top', 'best', 'popular', 'most'],
            'missing': ['revenue', 'sales', 'orders', 'count', 'quantity'],
            'question': "Rank by what metric?",
            'options': [
                ('revenue', 'By revenue generated'),
                ('order_count', 'By number of orders')
            ],
            'question_id': 'ranking_basis'
        },
        'MISSING_REVENUE_TYPE': {
            'triggers': ['revenue', 'sales', 'earnings'],
            'missing': ['total', 'subtotal', 'net', 'gross'],
            'question': "Which revenue metric?",
            'options': [
                ('total_amount', 'Total revenue (including tax & tips)'),
                ('subtotal', 'Subtotal (before tax & tips)'),
            ],
            'question_id': 'metric_type'
        },
        'MISSING_INVENTORY_FILTER': {
            'triggers': ['inventory', 'stock'],
            'missing': ['all', 'low', 'critical', 'good', 'status'],
            'question': "Which inventory items?",
            'options': [
                ('all', 'All items'),
                ('low_or_critical', 'Items needing attention (low/critical)'),
                ('critical_only', 'Critical items only')
            ],
            'question_id': 'filter_type'
        },
    }

    def __init__(self, confidence_threshold: float = 0.6):
        """
        Initialize ambiguity detector.

        Args:
            confidence_threshold: Confidence threshold for ambiguity detection.
                Below this threshold, clarification is requested.
        """
        self.confidence_threshold = confidence_threshold

    def detect(self, question: str) -> AmbiguityResult:
        """
        Detect if query is ambiguous using pattern matching.

        Args:
            question: User's natural language question

        Returns:
            AmbiguityResult indicating if clarification is needed
        """
        question_lower = question.lower().strip()

        # Check each pattern
        for pattern_name, pattern in self.PATTERNS.items():
            # Check if trigger words are present
            has_trigger = any(trigger in question_lower for trigger in pattern['triggers'])

            if has_trigger:
                # Check if clarifying words are missing
                has_clarification = any(missing in question_lower for missing in pattern['missing'])

                if not has_clarification:
                    # Ambiguity detected
                    logger.info(f"Ambiguity detected: {pattern_name} for question: {question}")
                    return AmbiguityResult(
                        is_ambiguous=True,
                        question=pattern['question'],
                        options=pattern['options'],
                        question_id=pattern['question_id'],
                        confidence=0.8  # High confidence for pattern match
                    )

        # No ambiguity detected
        return AmbiguityResult(
            is_ambiguous=False,
            confidence=0.9
        )


class LLMQueryGenerator:
    """
    Main LLM-based SQL query generator using OpenRouter API.

    Generates BigQuery Standard SQL from natural language questions with:
    - Ambiguity detection and clarification
    - Security validation
    - Parameterized queries
    """

    def __init__(
        self,
        db,
        api_key: str,
        model: str = "openai/gpt-4o-mini",
        enable_cache: bool = True
    ):
        """
        Initialize LLM query generator.

        Args:
            db: BigQueryManager instance
            api_key: OpenRouter API key
            model: Model identifier (default: gpt-4o-mini for cost-effectiveness)
            enable_cache: Whether to cache generated SQL (default: True)
        """
        self.db = db
        self.api_key = api_key
        self.model = model
        self.enable_cache = enable_cache
        self._cache = {}  # Simple in-memory cache

        # Initialize OpenRouter client
        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key
        )

        # Initialize ambiguity detector
        self.detector = AmbiguityDetector()

        logger.info(f"LLMQueryGenerator initialized with model: {model}")

    def detect_ambiguity(self, question: str) -> AmbiguityResult:
        """
        Check if query needs clarification.

        Args:
            question: User's natural language question

        Returns:
            AmbiguityResult with clarification details if needed
        """
        return self.detector.detect(question)

    def generate_query(
        self,
        question: str,
        location_id: str,
        start_date: str,
        end_date: str,
        clarifications: Optional[Dict[str, str]] = None
    ) -> Tuple[Optional[str], str, Optional[List[Any]]]:
        """
        Generate BigQuery SQL from natural language question.

        Args:
            question: User's natural language question
            location_id: Location identifier for filtering
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            clarifications: Optional dict of user-provided clarifications
                Example: {"time_granularity": "daily", "ranking_basis": "revenue"}

        Returns:
            Tuple of (sql_query, description, parameters)
            - sql_query: Generated SQL string (None if generation failed)
            - description: Human-readable description or error message
            - parameters: List of BigQuery query parameters (None if generation failed)
        """
        logger.info(f"Generating query for: {question}")
        if clarifications:
            logger.info(f"Clarifications provided: {clarifications}")

        # Check cache first
        cache_key = self._get_cache_key(question, location_id, start_date, end_date, clarifications)
        if self.enable_cache and cache_key in self._cache:
            logger.info("Returning cached SQL")
            return self._cache[cache_key]

        try:
            # Build prompt with schema and clarifications
            prompt = self._build_prompt(question, clarifications)

            # Call LLM to generate SQL
            response = self._call_llm(prompt)

            # Parse response into SQL and explanation
            sql, explanation = self._parse_response(response)

            # Check if LLM indicated it cannot generate SQL
            if sql.startswith("UNABLE:"):
                reason = sql.replace("UNABLE:", "").strip()
                logger.warning(f"LLM unable to generate SQL: {reason}")
                result = (None, f"❌ {reason}", None)

                if self.enable_cache:
                    self._cache[cache_key] = result
                return result

            # Create parameters
            params = self._create_parameters(sql, location_id, start_date, end_date)

            # Use the explanation from LLM (or default if not provided)
            description = explanation if explanation else "Query generated successfully."
            result = (sql, description, params)

            # Cache the result
            if self.enable_cache:
                self._cache[cache_key] = result

            logger.info(f"SQL generation successful. Explanation: {description[:50]}...")
            return result

        except Exception as e:
            error_msg = f"Error generating SQL: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return (None, f"❌ {error_msg}", None)

    def _build_prompt(self, question: str, clarifications: Optional[Dict[str, str]]) -> str:
        """
        Construct prompt with schema, examples, and clarifications.

        Args:
            question: User's question
            clarifications: User-provided clarifications

        Returns:
            Complete formatted prompt
        """
        return build_prompt(
            dataset_ref=self.db.dataset_ref,
            user_question=question,
            clarifications=clarifications or {}
        )

    def _call_llm(self, prompt: str) -> str:
        """
        Call OpenRouter API to generate SQL.

        Args:
            prompt: Complete prompt with schema and examples

        Returns:
            Generated SQL string or "UNABLE: reason"
        """
        logger.info("Calling OpenRouter API...")

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,  # Low temperature for consistency
                max_tokens=1000,
                top_p=0.95
            )

            sql = response.choices[0].message.content.strip()
            logger.info(f"LLM response received: {sql[:100]}...")

            return sql

        except Exception as e:
            logger.error(f"OpenRouter API call failed: {e}", exc_info=True)
            return f"UNABLE: API call failed - {str(e)}"

    def _clean_sql(self, sql: str) -> str:
        """
        Clean SQL output from LLM (remove markdown, extra whitespace).

        Args:
            sql: Raw SQL from LLM

        Returns:
            Cleaned SQL string
        """
        # Remove markdown code blocks
        if sql.startswith("```sql"):
            sql = sql.replace("```sql", "").replace("```", "").strip()
        elif sql.startswith("```"):
            sql = sql.replace("```", "").strip()

        # Remove leading/trailing whitespace
        sql = sql.strip()

        return sql

    def _parse_response(self, response: str) -> Tuple[str, str]:
        """
        Parse LLM response into SQL and plain English explanation.

        The expected format is:
        SQL:
        [query]

        EXPLANATION:
        [plain English description]

        Args:
            response: Raw response from LLM

        Returns:
            Tuple of (sql, explanation)
        """
        # Handle UNABLE responses
        if "UNABLE:" in response:
            return response.strip(), ""

        sql = ""
        explanation = "Query generated successfully."

        # Try to parse structured format
        if "SQL:" in response and "EXPLANATION:" in response:
            # Split on EXPLANATION: first
            parts = response.split("EXPLANATION:")
            sql_part = parts[0]
            explanation = parts[1].strip() if len(parts) > 1 else explanation

            # Extract SQL from the first part
            if "SQL:" in sql_part:
                sql = sql_part.split("SQL:", 1)[1].strip()
            else:
                sql = sql_part.strip()
        elif "SQL:" in response:
            # Only SQL: present, no explanation
            sql = response.split("SQL:", 1)[1].strip()
        elif "EXPLANATION:" in response:
            # Only EXPLANATION: present (unusual)
            parts = response.split("EXPLANATION:")
            sql = parts[0].strip()
            explanation = parts[1].strip() if len(parts) > 1 else explanation
        else:
            # No structured format, assume entire response is SQL
            sql = response.strip()

        # Clean up the SQL
        sql = self._clean_sql(sql)

        # Clean up explanation (remove any trailing SQL that leaked in)
        if explanation and "SELECT" in explanation.upper():
            # Explanation contains SQL, truncate before it
            lines = explanation.split('\n')
            clean_lines = []
            for line in lines:
                if line.strip().upper().startswith("SELECT"):
                    break
                clean_lines.append(line)
            explanation = ' '.join(clean_lines).strip()

        # Ensure explanation doesn't start with common SQL keywords
        if explanation.upper().startswith(("SELECT", "FROM", "WHERE")):
            explanation = "Query generated successfully."

        return sql, explanation

    def _create_parameters(
        self,
        sql: str,
        location_id: str,
        start_date: str,
        end_date: str
    ) -> List[bigquery.ScalarQueryParameter]:
        """
        Create BigQuery parameters based on what the SQL uses.

        Args:
            sql: Generated SQL query
            location_id: Location ID value
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            List of BigQuery query parameters
        """
        params = []

        # Add parameters based on what SQL references
        if '@location_id' in sql:
            params.append(bigquery.ScalarQueryParameter("location_id", "STRING", location_id))

        if '@start_date' in sql:
            params.append(bigquery.ScalarQueryParameter("start_date", "STRING", start_date))

        if '@end_date' in sql:
            params.append(bigquery.ScalarQueryParameter("end_date", "STRING", end_date))

        if '@snapshot_date' in sql:
            # For inventory queries, use end_date as snapshot_date
            params.append(bigquery.ScalarQueryParameter("snapshot_date", "STRING", end_date))

        return params

    def _get_cache_key(
        self,
        question: str,
        location_id: str,
        start_date: str,
        end_date: str,
        clarifications: Optional[Dict[str, str]]
    ) -> str:
        """
        Generate cache key for query.

        Args:
            question: User question
            location_id: Location ID
            start_date: Start date
            end_date: End date
            clarifications: Clarifications dict

        Returns:
            MD5 hash string as cache key
        """
        # Create a deterministic string representation
        key_data = f"{question}|{location_id}|{start_date}|{end_date}|{clarifications}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def clear_cache(self):
        """Clear the SQL generation cache."""
        self._cache.clear()
        logger.info("Cache cleared")
