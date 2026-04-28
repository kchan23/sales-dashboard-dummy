"""Deterministic natural-language query generator for presentation demo mode."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery

from query.llm_generator import AmbiguityDetector, AmbiguityResult


class DemoQueryGenerator:
    """Generate safe, canned aggregate SQL for the local synthetic dataset."""

    def __init__(self, db):
        self.db = db
        self.detector = AmbiguityDetector()

    def detect_ambiguity(self, question: str) -> AmbiguityResult:
        result = self.detector.detect(question)
        question_lower = question.lower()
        if result.question_id == "metric_type":
            return AmbiguityResult(is_ambiguous=False, confidence=0.9)
        if result.question_id == "time_granularity" and not any(
            word in question_lower
            for word in ("trend", "over time", "graph", "chart", "track", "pattern")
        ):
            return AmbiguityResult(is_ambiguous=False, confidence=0.9)
        return result

    def generate_query(
        self,
        question: str,
        location_id: str,
        start_date: str,
        end_date: str,
        clarifications: Optional[Dict[str, str]] = None,
    ) -> Tuple[Optional[str], str, Optional[List[Any]]]:
        clarifications = clarifications or {}
        query_kind = self._classify(question, clarifications)
        sql = self._sql_for(query_kind)
        if sql is None:
            return (
                None,
                "I can answer demo questions about sales trends, menu items, categories, order types, inventory, reviews, and customer aggregates.",
                None,
            )

        params = self._create_parameters(sql, location_id, start_date, end_date)
        return sql, self._description_for(query_kind), params

    def _classify(self, question: str, clarifications: Dict[str, str]) -> str:
        q = question.lower()

        if any(word in q for word in ("inventory", "stock", "reorder")):
            return "inventory_attention"
        if any(word in q for word in ("review", "rating", "sentiment")):
            return "review_sentiment"
        if any(word in q for word in ("customer", "repeat", "loyal")):
            return "customer_summary"
        if any(word in q for word in ("order type", "delivery", "dine", "takeout")):
            return "order_type_mix"
        if "category" in q:
            return "category_performance"
        if any(word in q for word in ("item", "menu", "dish", "popular", "top", "best")):
            if clarifications.get("ranking_basis") == "order_count":
                return "top_items_by_orders"
            return "top_items_by_revenue"
        if any(word in q for word in ("average order", "avg order", "aov")):
            return "average_order_value"
        if any(word in q for word in ("trend", "daily", "over time", "by day")):
            return "daily_revenue"
        if any(word in q for word in ("revenue", "sales", "orders", "tips", "discount")):
            return "sales_summary"
        return "sales_summary"

    def _sql_for(self, query_kind: str) -> Optional[str]:
        queries = {
            "daily_revenue": """
SELECT /* DEMO_QUERY: daily_revenue */
  business_date AS date,
  COUNT(DISTINCT order_guid) AS orders,
  SUM(total_amount) AS revenue,
  AVG(total_amount) AS avg_order_value
FROM demo_local.orders_clean
WHERE location_id = @location_id
  AND business_date BETWEEN @start_date AND @end_date
GROUP BY date
ORDER BY date
""",
            "sales_summary": """
SELECT /* DEMO_QUERY: sales_summary */
  COUNT(DISTINCT order_guid) AS orders,
  SUM(total_amount) AS revenue,
  AVG(total_amount) AS avg_order_value,
  SUM(tip_amount) AS tips,
  SUM(discount_amount) AS discounts
FROM demo_local.orders_clean
WHERE location_id = @location_id
  AND business_date BETWEEN @start_date AND @end_date
""",
            "average_order_value": """
SELECT /* DEMO_QUERY: average_order_value */
  business_date AS date,
  COUNT(DISTINCT order_guid) AS orders,
  SUM(total_amount) AS revenue,
  AVG(total_amount) AS avg_order_value
FROM demo_local.orders_clean
WHERE location_id = @location_id
  AND business_date BETWEEN @start_date AND @end_date
GROUP BY date
ORDER BY date
""",
            "top_items_by_revenue": """
SELECT /* DEMO_QUERY: top_items_by_revenue */
  item_name AS item,
  ANY_VALUE(category) AS category,
  SUM(quantity) AS order_count,
  SUM(total_price) AS revenue,
  SAFE_DIVIDE(SUM(total_price), SUM(quantity)) AS avg_price
FROM demo_local.order_items_clean
WHERE location_id = @location_id
  AND business_date BETWEEN @start_date AND @end_date
GROUP BY item
ORDER BY revenue DESC
LIMIT 10
""",
            "top_items_by_orders": """
SELECT /* DEMO_QUERY: top_items_by_orders */
  item_name AS item,
  ANY_VALUE(category) AS category,
  SUM(quantity) AS order_count,
  SUM(total_price) AS revenue,
  SAFE_DIVIDE(SUM(total_price), SUM(quantity)) AS avg_price
FROM demo_local.order_items_clean
WHERE location_id = @location_id
  AND business_date BETWEEN @start_date AND @end_date
GROUP BY item
ORDER BY order_count DESC
LIMIT 10
""",
            "category_performance": """
SELECT /* DEMO_QUERY: category_performance */
  category,
  SUM(quantity) AS order_count,
  SUM(total_price) AS revenue,
  SAFE_DIVIDE(SUM(total_price), SUM(quantity)) AS avg_price
FROM demo_local.order_items_clean
WHERE location_id = @location_id
  AND business_date BETWEEN @start_date AND @end_date
GROUP BY category
ORDER BY revenue DESC
""",
            "order_type_mix": """
SELECT /* DEMO_QUERY: order_type_mix */
  order_type,
  COUNT(DISTINCT order_guid) AS orders,
  SUM(total_amount) AS revenue,
  AVG(total_amount) AS avg_order_value
FROM demo_local.orders_clean
WHERE location_id = @location_id
  AND business_date BETWEEN @start_date AND @end_date
GROUP BY order_type
ORDER BY revenue DESC
""",
            "inventory_attention": """
SELECT /* DEMO_QUERY: inventory_attention */
  item_name AS item,
  category,
  current_stock AS stock,
  reorder_level,
  status
FROM demo_local.inventory
WHERE location_id = @location_id
  AND snapshot_date = @snapshot_date
  AND status IN ('low', 'critical')
ORDER BY status, item
""",
            "review_sentiment": """
SELECT /* DEMO_QUERY: review_sentiment */
  sentiment,
  COUNT(*) AS review_count,
  AVG(rating) AS avg_rating
FROM demo_local.reviews
WHERE location_id = @location_id
  AND review_date BETWEEN @start_date AND @end_date
GROUP BY sentiment
ORDER BY review_count DESC
""",
            "customer_summary": """
SELECT /* DEMO_QUERY: customer_summary */
  CASE
    WHEN order_count >= 5 THEN '5+ orders'
    WHEN order_count >= 2 THEN '2-4 orders'
    ELSE '1 order'
  END AS customer_segment,
  COUNT(*) AS customers,
  AVG(total_spend) AS avg_total_spend,
  AVG(avg_order) AS avg_order_value
FROM demo_local.customer_orders_masked
WHERE location_id = @location_id
  AND last_visit BETWEEN @start_date AND @end_date
GROUP BY customer_segment
ORDER BY customers DESC
""",
        }
        return queries.get(query_kind)

    def _description_for(self, query_kind: str) -> str:
        descriptions = {
            "daily_revenue": "Daily revenue, order volume, and average order value for the selected date range.",
            "sales_summary": "A summary of revenue, orders, average order value, tips, and discounts.",
            "average_order_value": "Average order value by day, with total revenue and order counts for context.",
            "top_items_by_revenue": "Top menu items ranked by revenue in the selected date range.",
            "top_items_by_orders": "Top menu items ranked by quantity ordered in the selected date range.",
            "category_performance": "Menu category performance by revenue, quantity, and average price.",
            "order_type_mix": "Revenue and order volume split by order type.",
            "inventory_attention": "Inventory items currently marked low or critical in the latest matching snapshot.",
            "review_sentiment": "Review counts and average ratings grouped by sentiment.",
            "customer_summary": "Privacy-safe customer segments grouped by repeat-order behavior.",
        }
        return descriptions.get(query_kind, "Demo query generated successfully.")

    def _create_parameters(
        self, sql: str, location_id: str, start_date: str, end_date: str
    ) -> List[bigquery.ScalarQueryParameter]:
        params = []
        if "@location_id" in sql:
            params.append(bigquery.ScalarQueryParameter("location_id", "STRING", location_id))
        if "@start_date" in sql:
            params.append(bigquery.ScalarQueryParameter("start_date", "STRING", start_date))
        if "@end_date" in sql:
            params.append(bigquery.ScalarQueryParameter("end_date", "STRING", end_date))
        if "@snapshot_date" in sql:
            params.append(bigquery.ScalarQueryParameter("snapshot_date", "STRING", end_date))
        return params
