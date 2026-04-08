"""
SQL validation and security checking for generated queries.

This module provides multi-stage validation for LLM-generated SQL queries:
1. Security checks - Blocks forbidden operations (DROP, DELETE, etc.)
2. Parameter validation - Ensures required parameters are present
3. BigQuery dry-run - Validates syntax and schema references

Usage:
    validator = SQLValidator(bigquery_client)
    is_valid, error_msg = validator.validate(sql_query, parameters)
    if is_valid:
        # Safe to execute
        results = client.query(sql_query, params=parameters)
"""

from typing import Tuple, Optional, List
from google.cloud import bigquery
import re
import logging

logger = logging.getLogger(__name__)


class SQLValidator:
    """Validates generated SQL for security and correctness."""

    FORBIDDEN_KEYWORDS = [
        'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER',
        'CREATE', 'TRUNCATE', 'MERGE', 'GRANT', 'REVOKE',
        'EXEC', 'EXECUTE', 'CALL', 'DECLARE', 'SET'
    ]

    def __init__(self, bq_client: bigquery.Client):
        """
        Initialize validator with BigQuery client.

        Args:
            bq_client: Google Cloud BigQuery client for dry-run validation
        """
        self.client = bq_client

    def validate(self, sql: str, params: List[bigquery.ScalarQueryParameter]) -> Tuple[bool, Optional[str]]:
        """
        Four-stage validation pipeline:
        1. Security check (forbidden operations)
        2. Privacy check (no raw IDs, no PII tables, aggregation required)
        3. Parameter check (required params present)
        4. BigQuery dry-run (syntax validation)

        Args:
            sql: SQL query string to validate
            params: List of BigQuery query parameters

        Returns:
            Tuple of (is_valid: bool, error_message: Optional[str])
            If valid, error_message is None
            If invalid, error_message contains the reason
        """
        logger.info(f"Validating SQL query: {sql[:100]}...")

        # Stage 1: Security check
        valid, error = self._check_security(sql)
        if not valid:
            logger.warning(f"Security validation failed: {error}")
            return False, error

        # Stage 2: Privacy check
        valid, error = self._check_privacy_safety(sql)
        if not valid:
            logger.warning(f"Privacy validation failed: {error}")
            return False, error

        # Stage 3: Parameter check
        valid, error = self._check_parameters(sql, params)
        if not valid:
            logger.warning(f"Parameter validation failed: {error}")
            return False, error

        # Stage 4: BigQuery dry-run
        valid, error = self._dry_run(sql, params)
        if not valid:
            logger.warning(f"Dry-run validation failed: {error}")
            return False, error

        logger.info("SQL validation passed all checks")
        return True, None

    def _check_security(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Check for forbidden SQL operations.

        Args:
            sql: SQL query string

        Returns:
            Tuple of (is_valid, error_message)
        """
        sql_upper = sql.upper()

        # Check for forbidden keywords using word boundaries
        for keyword in self.FORBIDDEN_KEYWORDS:
            # Use word boundary \b to avoid false positives (e.g., "INSERT" in "INSERTED_AT")
            if re.search(rf'\b{keyword}\b', sql_upper):
                return False, f"Security violation: {keyword} operations are not allowed"

        # Must start with SELECT (after optional whitespace)
        if not re.match(r'^\s*SELECT\b', sql_upper):
            return False, "Only SELECT queries are permitted for analytics"

        # Check for semicolons (multi-statement queries)
        if ';' in sql.strip().rstrip(';'):  # Allow trailing semicolon
            return False, "Multiple SQL statements are not allowed"

        return True, None

    def _check_privacy_safety(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Check that the query does not expose raw PII tables or individual-level identifiers.

        Blocks:
        - References to customer_orders or customer_orders_clean (raw PII)
        - order_guid or order_id selected as a raw output column (not inside an aggregate)

        Args:
            sql: SQL query string

        Returns:
            Tuple of (is_valid, error_message)
        """
        sql_upper = sql.upper()

        # Block raw PII tables
        if re.search(r'\bCUSTOMER_ORDERS_CLEAN\b', sql_upper):
            return False, (
                "Privacy violation: customer_orders_clean contains raw PII (names, emails, phones). "
                "Use customer_orders_masked instead."
            )
        # Match customer_orders but not customer_orders_masked
        if re.search(r'\bCUSTOMER_ORDERS\b(?!_MASKED)', sql_upper):
            return False, (
                "Privacy violation: customer_orders contains raw PII (names, emails, phones). "
                "Use customer_orders_masked instead."
            )

        # Block order_guid / order_id as bare SELECT output columns.
        # Extract the outermost SELECT clause (up to the first FROM at the same nesting level).
        select_match = re.search(r'SELECT\s+(DISTINCT\s+)?(.*?)\s+FROM\b', sql_upper, re.DOTALL)
        if select_match:
            select_clause = select_match.group(2)
            for raw_id in ('ORDER_GUID', 'ORDER_ID'):
                if re.search(rf'\b{raw_id}\b', select_clause):
                    # Allow if it only appears inside an aggregate function call
                    if not re.search(
                        rf'(COUNT|MIN|MAX|SUM|AVG)\s*\(\s*(DISTINCT\s+)?[^)]*\b{raw_id}\b',
                        select_clause
                    ):
                        return False, (
                            f"Privacy violation: {raw_id.lower()} cannot appear as a raw output column. "
                            "Use COUNT(DISTINCT order_guid) or another aggregate instead."
                        )

        return True, None

    def _check_parameters(self, sql: str, params: List[bigquery.ScalarQueryParameter]) -> Tuple[bool, Optional[str]]:
        """
        Verify required parameters are present.

        Args:
            sql: SQL query string
            params: List of query parameters

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Extract parameter names from SQL (@param_name)
        param_pattern = r'@(\w+)'
        sql_params = set(re.findall(param_pattern, sql))

        # Get provided parameter names
        provided_params = {p.name for p in params}

        # Check if all SQL parameters are provided
        missing_params = sql_params - provided_params
        if missing_params:
            return False, f"Missing required parameters: {', '.join(missing_params)}"

        # Check for common required parameters
        if '@location_id' in sql:
            if not any(p.name == 'location_id' for p in params):
                return False, "Missing required parameter: location_id"

        if '@start_date' in sql:
            if not any(p.name == 'start_date' for p in params):
                return False, "Missing required parameter: start_date"

        if '@end_date' in sql:
            if not any(p.name == 'end_date' for p in params):
                return False, "Missing required parameter: end_date"

        if '@snapshot_date' in sql:
            if not any(p.name == 'snapshot_date' for p in params):
                return False, "Missing required parameter: snapshot_date"

        return True, None

    def _dry_run(self, sql: str, params: List[bigquery.ScalarQueryParameter]) -> Tuple[bool, Optional[str]]:
        """
        Execute BigQuery dry-run for syntax validation.

        This validates:
        - SQL syntax is correct for BigQuery Standard SQL
        - Table and column references exist in the schema
        - Functions and aggregations are properly formed

        Args:
            sql: SQL query string
            params: List of query parameters

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            job_config = bigquery.QueryJobConfig()
            job_config.query_parameters = params
            job_config.dry_run = True
            job_config.use_query_cache = False

            # Execute dry-run (does not actually run the query)
            query_job = self.client.query(sql, job_config=job_config)

            # If we get here, syntax is valid
            logger.info(f"Dry-run successful. Query would process {query_job.total_bytes_processed} bytes")
            return True, None

        except Exception as e:
            # Parse error message for user-friendly output
            error_msg = str(e)

            # Common error patterns
            if "not found" in error_msg.lower():
                return False, f"Table or column not found in database schema: {error_msg}"
            elif "syntax error" in error_msg.lower():
                return False, f"SQL syntax error: {error_msg}"
            elif "unrecognized name" in error_msg.lower():
                return False, f"Invalid column or alias name: {error_msg}"
            else:
                return False, f"BigQuery validation error: {error_msg}"


def validate_table_references(sql: str, allowed_tables: List[str]) -> Tuple[bool, Optional[str]]:
    """
    Additional validation: Check that SQL only references allowed tables.

    This is an optional extra layer of security.

    Args:
        sql: SQL query string
        allowed_tables: List of table names that are permitted

    Returns:
        Tuple of (is_valid, error_message)
    """
    sql_upper = sql.upper()

    # Extract table references (simplified pattern)
    # Matches: FROM `project.dataset.table` or FROM table_name
    table_pattern = r'FROM\s+[`"]?(?:\w+\.)?(?:\w+\.)?(\w+)[`"]?'
    join_pattern = r'JOIN\s+[`"]?(?:\w+\.)?(?:\w+\.)?(\w+)[`"]?'

    from_tables = re.findall(table_pattern, sql_upper)
    join_tables = re.findall(join_pattern, sql_upper)

    referenced_tables = set(from_tables + join_tables)

    # Check if all referenced tables are in allowed list
    allowed_upper = {t.upper() for t in allowed_tables}
    invalid_tables = referenced_tables - allowed_upper

    if invalid_tables:
        return False, f"SQL references unauthorized tables: {', '.join(invalid_tables)}"

    return True, None
