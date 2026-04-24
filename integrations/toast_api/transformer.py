"""
Transform Toast API JSON responses into BigQuery row dicts.

Maps the nested Toast API order/menu structures into the flat row format
expected by the BigQuery warehouse tables.
"""

import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def transform_orders(api_orders: List[Dict], location_id: str) -> List[Dict[str, Any]]:
    """Transform Toast API orders into BigQuery `orders` table rows."""
    rows = []
    for order in api_orders:
        try:
            if order.get("voided"):
                continue

            order_guid = order.get("guid")
            if not order_guid:
                continue

            business_date = str(order.get("businessDate", ""))
            opened_date = order.get("openedDate", "")
            order_type = order.get("source", "UNKNOWN")

            total_amount = 0.0
            subtotal = 0.0
            tax_amount = 0.0
            tip_amount = 0.0
            discount_amount = 0.0

            for check in order.get("checks", []):
                if check.get("voided"):
                    continue
                total_amount += check.get("totalAmount", 0.0) or 0.0
                subtotal += check.get("amount", 0.0) or 0.0
                tax_amount += check.get("taxAmount", 0.0) or 0.0

                for payment in check.get("payments", []):
                    tip_amount += payment.get("tipAmount", 0.0) or 0.0

                for discount in check.get("appliedDiscounts", []):
                    discount_amount += discount.get("discountAmount", 0.0) or 0.0

            rows.append({
                "order_id": order_guid,
                "location_id": location_id,
                "business_date": business_date,
                "order_guid": order_guid,
                "order_time": opened_date,
                "order_type": order_type,
                "total_amount": round(total_amount, 2),
                "subtotal": round(subtotal, 2),
                "tax_amount": round(tax_amount, 2),
                "tip_amount": round(tip_amount, 2),
                "discount_amount": round(discount_amount, 2),
            })
        except Exception as exc:
            logger.warning(f"Skipping order {order.get('guid', '?')}: {exc}")
            continue

    return rows


def transform_order_items(api_orders: List[Dict], location_id: str) -> List[Dict[str, Any]]:
    """Transform Toast API order selections into BigQuery `order_items` rows."""
    rows = []
    for order in api_orders:
        try:
            if order.get("voided"):
                continue

            order_guid = order.get("guid")
            if not order_guid:
                continue

            business_date = str(order.get("businessDate", ""))

            for check in order.get("checks", []):
                if check.get("voided"):
                    continue

                for selection in check.get("selections", []):
                    if selection.get("voided"):
                        continue

                    item_name = selection.get("displayName", "")
                    sales_category = selection.get("salesCategory") or {}

                    rows.append({
                        "order_guid": order_guid,
                        "item_name": item_name,
                        "category": sales_category.get("name", "") if isinstance(sales_category, dict) and "name" in sales_category else "",
                        "quantity": int(selection.get("quantity", 1) or 1),
                        "prediscount_total": float(selection.get("preDiscountPrice", 0.0) or 0.0),
                        "total_price": float(selection.get("price", 0.0) or 0.0),
                        "location_id": location_id,
                        "business_date": business_date,
                    })
        except Exception as exc:
            logger.warning(f"Skipping items for order {order.get('guid', '?')}: {exc}")
            continue

    return rows


def transform_payments(api_orders: List[Dict], location_id: str) -> List[Dict[str, Any]]:
    """Transform Toast API order payments into BigQuery `payments` rows."""
    rows = []
    for order in api_orders:
        try:
            if order.get("voided"):
                continue

            order_guid = order.get("guid")
            if not order_guid:
                continue

            business_date = str(order.get("businessDate", ""))

            for check in order.get("checks", []):
                if check.get("voided"):
                    continue

                for payment in check.get("payments", []):
                    rows.append({
                        "order_guid": order_guid,
                        "payment_method": payment.get("type", "UNKNOWN"),
                        "amount": round(float(payment.get("amount", 0.0) or 0.0), 2),
                        "payment_date": payment.get("paidDate", ""),
                        "location_id": location_id,
                        "business_date": business_date,
                    })
        except Exception as exc:
            logger.warning(f"Skipping payments for order {order.get('guid', '?')}: {exc}")
            continue

    return rows


def transform_customer_orders(api_orders: List[Dict], location_id: str) -> List[Dict[str, Any]]:
    """Transform Toast API customer data into BigQuery `customer_orders` rows."""
    rows = []
    for order in api_orders:
        try:
            if order.get("voided"):
                continue

            order_guid = order.get("guid")
            if not order_guid:
                continue

            business_date = str(order.get("businessDate", ""))

            for check in order.get("checks", []):
                if check.get("voided"):
                    continue

                customer = check.get("customer") or {}
                email = (customer.get("email") or "").strip()
                phone = (customer.get("phone") or "").strip()
                first_name = (customer.get("firstName") or "").strip()
                last_name = (customer.get("lastName") or "").strip()

                if not any([email, phone, first_name, last_name]):
                    continue

                rows.append({
                    "order_guid": order_guid,
                    "location_id": location_id,
                    "business_date": business_date,
                    "customer_email": email or None,
                    "customer_phone": phone or None,
                    "first_name": first_name or None,
                    "last_name": last_name or None,
                })
        except Exception as exc:
            logger.warning(f"Skipping customer data for order {order.get('guid', '?')}: {exc}")
            continue

    return rows


def transform_menus(api_menus: List[Dict], location_id: str, snapshot_date: str) -> List[Dict[str, Any]]:
    """Transform Toast API menu data into BigQuery `inventory` rows."""
    rows = []
    for menu_response in api_menus:
        menus = menu_response.get("menus", [menu_response])
        if not isinstance(menus, list):
            menus = [menus]

        for menu in menus:
            for group in menu.get("menuGroups", []):
                group_name = group.get("name", "")

                for item in group.get("menuItems", []):
                    item_name = item.get("name", "")
                    if not item_name:
                        continue

                    price = float(item.get("price", 0.0) or 0.0)

                    rows.append({
                        "location_id": location_id,
                        "item_name": item_name,
                        "category": group_name,
                        "current_stock": 0.0,
                        "reorder_level": 0.0,
                        "unit_cost": round(price, 2),
                        "snapshot_date": snapshot_date,
                        "status": "good",
                    })

    return rows
