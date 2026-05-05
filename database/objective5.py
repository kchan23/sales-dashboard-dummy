"""Shared helpers for synthetic Objective 5 menu optimization."""

from __future__ import annotations

from typing import Any


OBJ5_ACTIONS = ["Promote", "Bundle", "Re-price", "Rework", "Remove"]


def recommended_action(quadrant: Any, net_revenue: Any = 0, has_bundle: bool = False) -> str:
    """Map a menu quadrant and supporting signals to an operator action."""
    quadrant_text = "" if quadrant is None else str(quadrant)
    try:
        revenue = float(net_revenue or 0)
    except (TypeError, ValueError):
        revenue = 0.0

    if quadrant_text == "Star":
        return "Promote"
    if quadrant_text == "Puzzle":
        return "Bundle" if has_bundle else "Promote"
    if quadrant_text == "Plowhorse":
        return "Re-price"
    if has_bundle:
        return "Bundle"
    if revenue >= 5000:
        return "Rework"
    return "Remove"


def confidence_label(orders_with_item: Any, cost_coverage: Any = 0) -> str:
    """Return a coarse confidence label for action recommendations."""
    try:
        orders = float(orders_with_item or 0)
    except (TypeError, ValueError):
        orders = 0.0
    try:
        coverage = float(cost_coverage or 0)
    except (TypeError, ValueError):
        coverage = 0.0

    if orders >= 100 and coverage >= 0.5:
        return "High"
    if orders >= 25:
        return "Medium"
    return "Low"
