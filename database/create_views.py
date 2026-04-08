#!/usr/bin/env python3
"""Create or replace BigQuery cleaning views for analytics use.

Transforms raw API data into clean, analytics-ready views:
  - Casts business_date STRING -> DATE
  - Casts timestamp strings -> TIMESTAMP
  - Replaces sentinel strings ('UNKNOWN', '') with NULL
  - Deduplicates on order_guid / (order_guid, item_name, quantity)
  - Adds derived columns: tip_rate, hour_of_day, day_of_week, order_category
  - Adds outlier flags: flag_high_total, flag_negative, flag_zero_amount
  - Aggregates: daily_sales, item_performance

Run after backfill is complete:
    python3 -m database.create_views
"""

import json
import logging
from pathlib import Path
from database.bigquery import BigQueryManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

VIEWS = {
    "orders_clean": """
        WITH deduped AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY order_guid ORDER BY created_at)        AS rn
            FROM `{dataset}.orders`
        )
        SELECT
            order_id,
            location_id,
            PARSE_DATE('%Y%m%d', business_date)                                        AS business_date,
            order_guid,
            COALESCE(
                SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', order_time),
                SAFE.PARSE_TIMESTAMP('%m/%d/%y %I:%M %p',    order_time)
            )                                                                          AS order_time,
            NULLIF(order_type, 'UNKNOWN')                                              AS order_type,
            CASE
                WHEN UPPER(order_type) LIKE '%DELIVERY%'                   THEN 'Delivery'
                WHEN UPPER(order_type) LIKE '%TAKEOUT%'
                  OR UPPER(order_type) LIKE '%TAKE OUT%'
                  OR UPPER(order_type) LIKE '%PICKUP%'
                  OR UPPER(order_type) LIKE '%PICK UP%'                    THEN 'Takeout'
                WHEN UPPER(order_type) LIKE '%DINE%'                       THEN 'Dine-In'
                ELSE 'Other'
            END                                                                        AS order_category,
            total_amount,
            subtotal,
            tax_amount,
            tip_amount,
            discount_amount,
            SAFE_DIVIDE(tip_amount, total_amount)                                      AS tip_rate,
            EXTRACT(HOUR FROM COALESCE(
                SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', order_time),
                SAFE.PARSE_TIMESTAMP('%m/%d/%y %I:%M %p',    order_time)
            ))                                                                         AS hour_of_day,
            FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', business_date))                    AS day_of_week,
            total_amount > 500                                                         AS flag_high_total,
            total_amount < 0                                                           AS flag_negative,
            total_amount = 0                                                           AS flag_zero_amount,
            created_at
        FROM deduped
        WHERE rn = 1
          AND location_id IN ({loc_filter})
    """,
    "order_items_clean": """
        WITH deduped AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY order_guid, item_name, quantity
                    ORDER BY prediscount_total
                )                                                                      AS rn
            FROM `{dataset}.order_items`
        )
        SELECT
            oi.order_guid,
            NULLIF(oi.item_name, '')                                                   AS item_name,
            COALESCE(m.canonical_name, NULLIF(oi.item_name, ''))                       AS canonical_name,
            COALESCE(m.display_name,   NULLIF(oi.item_name, ''))                       AS display_name,
            COALESCE(NULLIF(oi.category, ''), m.category)                              AS category,
            oi.quantity,
            oi.prediscount_total,
            oi.total_price,
            SAFE_DIVIDE(oi.total_price, oi.quantity)                               AS true_unit_price,
            oi.location_id,
            PARSE_DATE('%Y%m%d', oi.business_date)                                     AS business_date
        FROM deduped oi
        LEFT JOIN `{dataset}.menu_canonical_map` m ON oi.item_name = m.item_name
        WHERE oi.rn = 1
          AND oi.location_id IN ({loc_filter})
    """,
    "payments_clean": """
        SELECT
            order_guid,
            NULLIF(payment_method, 'UNKNOWN')                                          AS payment_method,
            amount,
            COALESCE(
                SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', payment_date),
                SAFE.PARSE_TIMESTAMP('%m/%d/%y %I:%M %p',    payment_date)
            )                                                                          AS payment_date,
            location_id,
            PARSE_DATE('%Y%m%d', business_date)                                        AS business_date
        FROM `{dataset}.payments`
        WHERE location_id IN ({loc_filter})
    """,
    "daily_sales": """
        SELECT
            location_id,
            PARSE_DATE('%Y%m%d', business_date)                                        AS business_date,
            COUNT(DISTINCT order_guid)                                                 AS order_count,
            SUM(total_amount)                                                          AS gross_revenue,
            SUM(total_amount - discount_amount)                                        AS net_revenue,
            AVG(total_amount)                                                          AS avg_order_value,
            SUM(tip_amount)                                                            AS total_tips,
            SUM(discount_amount)                                                       AS total_discounts,
            COUNTIF(UPPER(order_type) LIKE '%DELIVERY%')                               AS delivery_orders,
            COUNTIF(UPPER(order_type) LIKE '%DINE%')                                   AS dine_in_orders,
            COUNTIF(
                UPPER(order_type) NOT LIKE '%DELIVERY%'
                AND UPPER(order_type) NOT LIKE '%DINE%'
            )                                                                          AS takeout_orders
        FROM `{dataset}.orders`
        WHERE total_amount >= 0
          AND location_id IN ({loc_filter})
        GROUP BY location_id, business_date
    """,
    "item_performance": """
        SELECT
            oi.location_id,
            COALESCE(m.canonical_name, NULLIF(oi.item_name, ''))                       AS canonical_name,
            COALESCE(m.display_name,   NULLIF(oi.item_name, ''))                       AS display_name,
            COALESCE(NULLIF(oi.category, ''), m.category)                              AS category,
            PARSE_DATE('%Y%m%d', oi.business_date)                                     AS business_date,
            SUM(oi.quantity)                                                           AS total_qty,
            SUM(oi.total_price)                                                        AS total_revenue,
            COUNT(DISTINCT oi.order_guid)                                              AS order_count,
            SAFE_DIVIDE(SUM(oi.total_price), SUM(oi.quantity))                        AS avg_unit_price
        FROM `{dataset}.order_items` oi
        LEFT JOIN `{dataset}.menu_canonical_map` m ON oi.item_name = m.item_name
        WHERE NULLIF(oi.item_name, '') IS NOT NULL
          AND oi.location_id IN ({loc_filter})
        GROUP BY 1, 2, 3, 4, 5
    """,
    # NOTE: customer_orders_clean was intentionally removed.
    # It exposed raw PII (email, phone, first_name, last_name) and is not used
    # by the analytics pipeline. Use customer_orders_masked for all analysis.
    #
    # PII-masked view for analysis: raw email/phone/name replaced by a stable
    # pseudonymous customer_id (SHA256 hex).  Email takes priority; phone is
    # used as fallback when email is absent.  Name fields are dropped entirely.
    "customer_orders_masked": """
        SELECT
            order_guid,
            location_id,
            PARSE_DATE('%Y%m%d', business_date)                                        AS business_date,
            CASE
                WHEN customer_email IS NOT NULL AND TRIM(customer_email) != ''
                    THEN TO_HEX(SHA256(LOWER(TRIM(customer_email))))
                WHEN customer_phone IS NOT NULL AND TRIM(customer_phone) != ''
                    THEN TO_HEX(SHA256(REGEXP_REPLACE(TRIM(customer_phone), r'[^0-9]', '')))
                ELSE NULL
            END                                                                        AS customer_id,
            (customer_email IS NOT NULL AND TRIM(customer_email) != '')                AS has_email,
            (customer_phone IS NOT NULL AND TRIM(customer_phone) != '')                AS has_phone
        FROM `{dataset}.customer_orders`
        WHERE location_id IN ({loc_filter})
    """,
}


def main():
    bq = BigQueryManager()
    dataset = bq.dataset_ref

    # Load valid location GUIDs from the scheduler-maintained cache
    loc_path = Path(__file__).parent.parent / "toast_api" / "location_names.json"
    valid_guids = list(json.loads(loc_path.read_text()).keys())
    loc_filter = ", ".join(f"'{g}'" for g in valid_guids)
    logger.info(f"Filtering views to {len(valid_guids)} location(s): {valid_guids}")

    for view_name, select_sql in VIEWS.items():
        sql = (
            f"CREATE OR REPLACE VIEW `{dataset}.{view_name}` AS\n"
            + select_sql.format(dataset=dataset, loc_filter=loc_filter)
        )
        bq.client.query(sql).result()
        logger.info(f"View {view_name} created")

    logger.info("Done.")


if __name__ == "__main__":
    main()
