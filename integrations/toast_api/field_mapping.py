"""
Field mapping between Toast API responses and existing BigQuery schema.

These mappings document how Toast API v2 fields correspond to the columns
in the BigQuery restaurant_analytics_demo dataset.
"""

ORDER_FIELD_MAP = {
    "guid": "order_guid",
    "openedDate": "order_time",
    "source": "order_type",
    "checks[].totalAmount": "total_amount",
    "checks[].amount": "subtotal",
    "checks[].taxAmount": "tax_amount",
    "checks[].payments[].tipAmount": "tip_amount",
    "checks[].appliedDiscounts[].discountAmount": "discount_amount",
}

ITEM_FIELD_MAP = {
    "displayName": "item_name",
    "salesCategory.name": "category",
    "quantity": "quantity",
    "price": "total_price",
    "preDiscountPrice": "prediscount_total",
}

PAYMENT_FIELD_MAP = {
    "type": "payment_method",
    "amount": "amount",
    "paidDate": "payment_date",
}

MENU_FIELD_MAP = {
    "menuGroups[].menuItems[].name": "item_name",
    "menuGroups[].name": "category",
    "menuGroups[].menuItems[].price": "unit_cost",
}

GUEST_FIELD_MAP = {
    "customer.firstName": "first_name",
    "customer.lastName": "last_name",
    "customer.phone": "customer_phone",
    "customer.email": "customer_email",
}

LABOR_FIELD_MAP = {
    "employeeName": "employee_name",
    "jobTitle": "job_title",
    "inDate": "clock_in_time",
    "outDate": "clock_out_time",
    "regularHours": "regular_hours",
    "overtimeHours": "overtime_hours",
    "totalHours": "total_hours",
    "declaredCashTips": "cash_tips",
    "nonCashTips": "non_cash_tips",
    "wage": "wage",
}


def _get_nested(obj: dict, dotted_key: str):
    """Retrieve a nested value using dotted paths and [] list traversal."""
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
    """Print a side-by-side comparison of Toast API fields vs BigQuery columns."""
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
