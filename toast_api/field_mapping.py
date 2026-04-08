"""
Field mapping between Toast API responses and existing BigQuery schema.

These mappings document how Toast API v2 fields correspond to the columns
in the BigQuery doughzone_analytics dataset. Used by the test pull script
to show side-by-side comparisons.
"""

# Toast orders API -> BigQuery `orders` table
# Source: /orders/v2/ordersBulk response
ORDER_FIELD_MAP = {
    "guid":                                       "order_guid",      # also used as order_id
    "openedDate":                                 "order_time",      # ISO datetime string
    "source":                                     "order_type",      # e.g. "In Store", "API", "DoorDash - Delivery"
    # Amounts are nested inside checks[].
    # These are aggregated across all checks in an order:
    "checks[].totalAmount":                       "total_amount",
    "checks[].amount":                            "subtotal",        # pre-discount subtotal
    "checks[].taxAmount":                         "tax_amount",
    "checks[].payments[].tipAmount":              "tip_amount",      # nested under payments[]
    # Discounts are nested inside checks[].appliedDiscounts[]
    "checks[].appliedDiscounts[].discountAmount": "discount_amount",
}

# Toast order selections -> BigQuery `order_items` table
# Source: orders[].checks[].selections[]
ITEM_FIELD_MAP = {
    "displayName":              "item_name",       # or item.name
    "salesCategory.name":       "category",
    "quantity":                 "quantity",
    "price":                    "total_price",         # actual line total (after discounts)
    "preDiscountPrice":         "prediscount_total",   # pre-discount line total
}

# Toast order payments -> BigQuery `payments` table
# Source: orders[].checks[].payments[]
PAYMENT_FIELD_MAP = {
    "type":                     "payment_method",  # e.g. "CREDIT", "CASH"
    "amount":                   "amount",
    "paidDate":                 "payment_date",
}

# Toast menus API -> BigQuery `inventory` table
# Source: /menus/v2/menus response
# Note: The menus endpoint returns menu structure, not stock levels.
# We map item names and prices for catalog reference.
MENU_FIELD_MAP = {
    "menuGroups[].menuItems[].name":  "item_name",
    "menuGroups[].name":              "category",
    "menuGroups[].menuItems[].price": "unit_cost",
}

# Toast guest PI -> BigQuery `customer_orders` table
# Source: orders[].checks[].customer (requires guest.pi:read scope)
GUEST_FIELD_MAP = {
    "customer.firstName":       "first_name",       # customer_orders.first_name
    "customer.lastName":        "last_name",         # customer_orders.last_name
    "customer.phone":           "customer_phone",    # customer_orders.customer_phone
    "customer.email":           "customer_email",    # customer_orders.customer_email
}

# Future: Toast labor API -> BigQuery `time_entries` table
# Requires labor:read scope (not yet available)
LABOR_FIELD_MAP = {
    "employeeName":             "employee_name",
    "jobTitle":                 "job_title",
    "inDate":                   "clock_in_time",
    "outDate":                  "clock_out_time",
    "regularHours":             "regular_hours",
    "overtimeHours":            "overtime_hours",
    "totalHours":               "total_hours",
    "declaredCashTips":         "cash_tips",
    "nonCashTips":              "non_cash_tips",
    "wage":                     "wage",
}


def _get_nested(obj: dict, dotted_key: str):
    """
    Retrieve a value from a nested dict/list using a dotted path with optional
    bracket notation for list traversal.

    Supports paths like:
      'source'                                      -> obj['source']
      'diningOption.name'                           -> obj['diningOption']['name']
      'checks[].totalAmount'                        -> obj['checks'][0]['totalAmount']
      'checks[].payments[].tipAmount'               -> obj['checks'][0]['payments'][0]['tipAmount']
      'checks[].appliedDiscounts[].discountAmount'  -> obj['checks'][0]['appliedDiscounts'][0]['discountAmount']

    Returns the first non-None value found when traversing lists, or None if the
    path doesn't exist.
    """
    segments = []
    for part in dotted_key.split("."):
        is_list = part.endswith("[]")
        segments.append((part.rstrip("[]"), is_list))

    def _traverse(current, segs):
        if not segs:
            return current
        key, is_list = segs[0]
        rest = segs[1:]
        if isinstance(current, list):
            # Already iterating a list — search each element for the path
            for item in current:
                result = _traverse(item, segs)
                if result is not None:
                    return result
            return None
        if not isinstance(current, dict):
            return None
        val = current.get(key)
        if val is None:
            return None
        if is_list:
            if not isinstance(val, list):
                return None
            for item in val:
                result = _traverse(item, rest)
                if result is not None:
                    return result
            return None
        return _traverse(val, rest)

    return _traverse(obj, segments)


def print_mapping_comparison(endpoint_type: str, sample_data: dict):
    """
    Print a side-by-side comparison of Toast API fields vs BigQuery columns,
    showing actual values from the sample data.

    Args:
        endpoint_type: "orders" or "menus"
        sample_data: A single record from the API response
    """
    if endpoint_type == "orders":
        field_map = ORDER_FIELD_MAP
    elif endpoint_type == "menus":
        field_map = MENU_FIELD_MAP
    elif endpoint_type == "guest":
        field_map = GUEST_FIELD_MAP
    else:
        return

    print(f"\n{'─'*60}")
    print(f"FIELD MAPPING CHECK: {endpoint_type}")
    print(f"{'─'*60}")
    print(f"{'Toast API Field':<45} {'BQ Column':<20} {'Sample Value'}")
    print(f"{'─'*45} {'─'*20} {'─'*30}")

    for api_field, bq_col in field_map.items():
        value = _get_nested(sample_data, api_field)
        value_str = repr(value)[:30] if value is not None else "(missing)"
        print(f"  {api_field:<43} {bq_col:<20} {value_str}")

    print()
