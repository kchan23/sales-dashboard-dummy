"""
DoughZone Analytics Dashboard - Streamlit application.
"""

import streamlit as st
import pandas as pd
import numpy as np
import statsmodels.api as sm
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
import plotly.express as px
import plotly.graph_objects as go
from database.bigquery import get_bq_manager
import json
import logging
import os
import re
import time
from pathlib import Path

try:
    import holidays as holidays_lib
except ImportError:
    holidays_lib = None


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from query.llm_generator import AmbiguityResult

BRAND_BURGUNDY = "#A6192E"
BRAND_GOLD = "#DABA79"
BRAND_IVORY = "#F7F3EB"
BRAND_PAPER = "#FFFDFC"
BRAND_CHARCOAL = "#2F241F"
BRAND_MUTED = "#7A6A61"
BRAND_BORDER = "#E6D8C3"
BRAND_SLATE = "#5C6B73"
STATUS_GOOD = "#6A8F63"
STATUS_WARN = "#C18B2F"
STATUS_BAD = "#A84A3A"
STATUS_NEUTRAL = "#8A7A70"
DRIVER_POSITIVE = "#B22222"
DRIVER_NEGATIVE = "#2F5F8F"

ACTION_BADGE_STYLES = {
    "Promote": {"background": BRAND_BURGUNDY, "color": "#FFFFFF"},
    "Bundle": {"background": BRAND_SLATE, "color": "#FFFFFF"},
    "Re-price": {"background": BRAND_GOLD, "color": BRAND_CHARCOAL},
    "Rework": {"background": STATUS_WARN, "color": BRAND_CHARCOAL},
    "Remove": {"background": STATUS_BAD, "color": "#FFFFFF"},
}


def action_badge_style(action: str) -> str:
    style = ACTION_BADGE_STYLES.get(action)
    if not style:
        return ""
    return (
        f"background-color: {style['background']}; "
        f"color: {style['color']}; "
        "font-weight: 700; "
        "text-align: center; "
        "border-radius: 6px;"
    )

# Page configuration
st.set_page_config(
    page_title="DoughZone Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    :root {
        --dz-burgundy: #A6192E;
        --dz-gold: #DABA79;
        --dz-ivory: #F7F3EB;
        --dz-paper: #FFFDFC;
        --dz-charcoal: #2F241F;
        --dz-muted: #7A6A61;
        --dz-border: #E6D8C3;
        font-size: 16px;
    }
    .stApp {
        background-color: var(--dz-ivory);
        color: var(--dz-charcoal);
    }
    [data-testid="stHeader"] {
        background: rgba(247, 243, 235, 0.88);
    }
    [data-testid="stSidebar"] {
        background-color: #efe5d3;
        border-right: 1px solid var(--dz-border);
    }
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span {
        color: var(--dz-charcoal);
    }
    h1, h2, h3, h4, h5, h6 {
        color: var(--dz-charcoal);
        letter-spacing: 0;
    }
    h1 {
        margin-bottom: 0.35rem;
    }
    .metric-card {
        background-color: var(--dz-paper);
        padding: 20px;
        border-radius: 12px;
        border-left: 4px solid var(--dz-burgundy);
        border: 1px solid var(--dz-border);
    }
    .metric-value {
        font-size: 1.75rem;
        font-weight: bold;
        color: var(--dz-charcoal);
    }
    .metric-label {
        font-size: 0.75rem;
        color: var(--dz-muted);
        margin-bottom: 10px;
    }
    [data-testid="stMetric"] {
        background: var(--dz-paper);
        border: 1px solid var(--dz-border);
        border-radius: 12px;
        padding: 0.9rem 1rem;
    }
    [data-testid="stMetricLabel"],
    [data-testid="stMetricValue"] {
        color: var(--dz-charcoal);
    }
    [data-testid="stAlertContainer"] {
        border-radius: 12px;
    }
    [data-testid="stExpander"] {
        border: 1px solid var(--dz-border);
        border-radius: 12px;
        background: var(--dz-paper);
    }
    /* Sidebar enlargement */
    [data-testid="stSidebarNav"] {
        font-size: 1.2em;
    }
    [data-testid="stSidebarContent"] {
        font-size: 1.2em;
    }
    .stRadio > label > div {
        font-size: 1.2em !important;
    }
    .stSelectbox > div > div > div {
        font-size: 1.2em !important;
    }
    .stSubheader {
        font-size: 1.2em !important;
    }
    /* Remove radio button bullet points */
    .stRadio > div > label > span:first-child {
        display: none !important;
        width: 0 !important;
    }
    /* Hide all SVG elements in radio buttons */
    div[role="radiogroup"] svg,
    .stRadio svg {
        display: none !important;
        width: 0 !important;
        height: 0 !important;
    }
    /* Remove spacing left by hidden radio indicators */
    .stRadio > div > label {
        padding-left: 0 !important;
        gap: 0.75rem !important;
        margin-left: 0 !important;
    }
    /* Target the div that contains the radio circle - first div child only */
    .stRadio > div > label > div:first-of-type {
        display: none !important;
        width: 0 !important;
        margin: 0 !important;
    }
    /* Horizontal tab nav font size */
    .stTabs [data-baseweb="tab"] {
        padding: 8px 18px !important;
        border-radius: 999px !important;
        background: transparent !important;
    }
    .stTabs [data-baseweb="tab"] p,
    .stTabs button[data-baseweb="tab"] p {
        font-size: 1.05rem !important;
        font-weight: 500 !important;
        color: var(--dz-muted) !important;
    }
    .stTabs button[data-baseweb="tab"][aria-selected="true"] {
        background: rgba(166, 25, 46, 0.08) !important;
        border: 1px solid rgba(166, 25, 46, 0.22) !important;
    }
    .stTabs button[data-baseweb="tab"][aria-selected="true"] p {
        color: var(--dz-burgundy) !important;
        font-weight: 600 !important;
    }
    .stButton > button {
        background: var(--dz-burgundy);
        color: #fffaf5;
        border-radius: 999px;
        border: 1px solid var(--dz-burgundy);
        font-weight: 600;
    }
    .stButton > button:hover {
        background: #8f1628;
        border-color: #8f1628;
    }
    .stTextInput input,
    .stDateInput input,
    .stMultiSelect div[data-baseweb="select"],
    .stSelectbox div[data-baseweb="select"] {
        border-radius: 10px !important;
        border-color: var(--dz-border) !important;
        background: var(--dz-paper) !important;
    }
    .stCaption {
        color: var(--dz-muted) !important;
    }
    @media (min-width: 1400px) {
        :root {
            font-size: 17px;
        }
        .block-container {
            max-width: 1500px;
            padding-left: 3rem;
            padding-right: 3rem;
        }
        [data-testid="stSidebar"] {
            min-width: 20rem;
        }
        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] li,
        label,
        .stButton > button,
        .stTextInput input,
        .stDateInput input,
        .stMultiSelect div[data-baseweb="select"],
        .stSelectbox div[data-baseweb="select"] {
            font-size: 1rem !important;
        }
        [data-testid="stMetricValue"] {
            font-size: 2rem !important;
        }
        [data-testid="stMetricLabel"] p {
            font-size: 0.95rem !important;
        }
        .stTabs [data-baseweb="tab"] p,
        .stTabs button[data-baseweb="tab"] p {
            font-size: 1.12rem !important;
        }
    }
    @media (min-width: 1900px) {
        :root {
            font-size: 18px;
        }
        .block-container {
            max-width: 1800px;
            padding-left: 4rem;
            padding-right: 4rem;
        }
        [data-testid="stSidebar"] {
            min-width: 22rem;
        }
        h1 {
            font-size: 2.6rem !important;
        }
        h2 {
            font-size: 2rem !important;
        }
        h3 {
            font-size: 1.55rem !important;
        }
        [data-testid="stMetricValue"] {
            font-size: 2.2rem !important;
        }
        [data-testid="stMetricLabel"] p {
            font-size: 1rem !important;
        }
    }
    /* Mobile: stack columns vertically */
    @media (max-width: 768px) {
        [data-testid="stHorizontalBlock"] {
            flex-direction: column;
        }
        [data-testid="stColumn"] {
            width: 100% !important;
            min-width: 100% !important;
            flex: none !important;
        }
    }
    /* Mobile: wrap and shrink tab bar */
    @media (max-width: 768px) {
        .stTabs [data-baseweb="tab-list"] {
            flex-wrap: wrap;
            gap: 4px;
        }
        .stTabs [data-baseweb="tab"] {
            font-size: 0.9rem !important;
            padding: 6px 10px !important;
        }
    }
    /* Mobile: ensure Plotly charts fill container */
    @media (max-width: 768px) {
        .js-plotly-plot, .plotly {
            width: 100% !important;
        }
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db():
    """Get database connection (cached for entire session)."""
    if is_demo_mode():
        return get_demo_db()
    try:
        db = get_bq_manager()
        # db.create_schema() # Schema should be managed by admin/automation to avoid permission issues for viewers
        db.migrate_schema()
        return db
    except Exception as e:
        st.warning(f"Failed to connect to BigQuery: {e}")
        st.info("Falling back to local synthetic demo data.")
        try:
            return get_demo_db()
        except Exception as demo_error:
            st.error(f"Failed to load demo data fallback: {demo_error}")
            return None


def get_demo_db():
    """Load the local synthetic demo data manager."""
    from database.demo_db import DemoDBManager
    return DemoDBManager()


def get_query_tools(db):
    """Build the query generator and validator for the active data source."""
    from query.validator import SQLValidator

    if getattr(db, "is_demo", False):
        from query.demo_generator import DemoQueryGenerator
        return DemoQueryGenerator(db), SQLValidator(None)

    from query.llm_generator import LLMQueryGenerator

    api_key = st.secrets.get("OPENROUTER_API_KEY") if "OPENROUTER_API_KEY" in st.secrets else os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENROUTER_API_KEY not configured. Please add it to .env file or Streamlit secrets."
        )
    return LLMQueryGenerator(db, api_key), SQLValidator(getattr(db, "client", None))


def format_currency(value):
    """Format value as currency."""
    return f"${value:,.2f}" if value else "$0.00"


def format_number(value):
    """Format value as number."""
    return f"{value:,.0f}" if value else "0"


def with_alpha(color: str, alpha: float) -> str:
    if not isinstance(color, str):
        return color
    if color.startswith("#") and len(color) == 7:
        hex_color = color.lstrip("#")
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        return f"rgba({r},{g},{b},{alpha})"
    return color


def format_compact_number(value, currency: bool = False) -> str:
    amount = float(value or 0)
    abs_amount = abs(amount)
    prefix = "$" if currency else ""
    suffix = ""
    scaled = amount
    if abs_amount >= 1_000_000_000:
        scaled = amount / 1_000_000_000
        suffix = "B"
    elif abs_amount >= 1_000_000:
        scaled = amount / 1_000_000
        suffix = "M"
    elif abs_amount >= 1_000:
        scaled = amount / 1_000
        suffix = "K"
    if suffix:
        return f"{prefix}{scaled:,.2f}{suffix}" if currency else f"{scaled:,.1f}{suffix}"
    return format_currency(amount) if currency else format_number(amount)


MENU_RECOMMENDATION_COLUMNS = [
    "canonical_name", "display_name", "category", "quadrant", "popularity",
    "net_revenue", "total_qty", "orders_with_item", "avg_unit_price",
    "avg_net_rev_per_unit", "avg_est_margin_per_unit", "est_margin_pct",
    "discount_rate", "cost_coverage", "margin_source",
    "recommended_action", "confidence",
]

BUNDLE_OPPORTUNITY_COLUMNS = [
    "item_a", "item_b", "display_a", "display_b", "category_a", "category_b",
    "pair_type", "pair_count", "support", "orders_a", "orders_b",
    "confidence_b_given_a", "confidence_a_given_b", "lift",
]

PROMO_OPPORTUNITY_COLUMNS = [
    "canonical_name", "display_name", "category", "store_days",
    "discount_days", "avg_qty_discounted_days", "avg_qty_regular_days",
    "qty_lift_on_discount_days", "unique_prices", "min_price", "max_price",
    "price_range", "opportunity_type",
]


def _empty_df(columns):
    return pd.DataFrame(columns=columns)


def _fallback_action(quadrant, revenue):
    if quadrant == "Star":
        return "Promote"
    if quadrant == "Puzzle":
        return "Promote"
    if quadrant == "Plowhorse":
        return "Re-price"
    return "Rework" if revenue >= 5000 else "Remove"


def _fallback_confidence(orders_with_item):
    if orders_with_item >= 100:
        return "High"
    if orders_with_item >= 25:
        return "Medium"
    return "Low"


def _build_menu_recommendations_fallback(menu_data: pd.DataFrame) -> pd.DataFrame:
    """Derive dummy menu optimization rows when the active demo DB lacks Obj5 methods."""
    if menu_data.empty:
        return _empty_df(MENU_RECOMMENDATION_COLUMNS)

    recs = menu_data.copy()
    total_orders = recs["order_count"].sum()
    recs["canonical_name"] = recs["item"]
    recs["display_name"] = recs["item"]
    recs["orders_with_item"] = recs["order_count"]
    recs["total_qty"] = recs["order_count"]
    recs["net_revenue"] = recs["revenue"]
    recs["avg_unit_price"] = recs["avg_price"]
    recs["popularity"] = recs["orders_with_item"] / total_orders if total_orders else 0
    recs["avg_net_rev_per_unit"] = recs["net_revenue"] / recs["total_qty"].replace(0, np.nan)
    recs["avg_est_margin_per_unit"] = recs["avg_net_rev_per_unit"] * 0.62
    recs["est_margin_pct"] = 0.62
    recs["discount_rate"] = 0.0
    recs["cost_coverage"] = 1.0
    recs["margin_source"] = "Synthetic cost proxy"

    pop_threshold = recs["popularity"].median()
    margin_threshold = recs["avg_est_margin_per_unit"].median()
    recs["quadrant"] = np.select(
        [
            (recs["popularity"] >= pop_threshold) & (recs["avg_est_margin_per_unit"] >= margin_threshold),
            (recs["popularity"] >= pop_threshold) & (recs["avg_est_margin_per_unit"] < margin_threshold),
            (recs["popularity"] < pop_threshold) & (recs["avg_est_margin_per_unit"] >= margin_threshold),
        ],
        ["Star", "Plowhorse", "Puzzle"],
        default="Dog",
    )
    recs["recommended_action"] = recs.apply(
        lambda row: _fallback_action(row["quadrant"], row["net_revenue"]),
        axis=1,
    )
    recs["confidence"] = recs["orders_with_item"].apply(_fallback_confidence)
    return recs[MENU_RECOMMENDATION_COLUMNS].sort_values("net_revenue", ascending=False)


def _call_optional_df(db, method_name: str, fallback, *args) -> pd.DataFrame:
    """Call an optional DB method, falling back for older dummy DB instances."""
    method = getattr(db, method_name, None)
    if method is None:
        return fallback() if callable(fallback) else fallback.copy()
    try:
        return method(*args)
    except AttributeError:
        return fallback() if callable(fallback) else fallback.copy()


def is_demo_mode() -> bool:
    """Default to demo mode unless explicitly disabled."""
    return os.getenv("DEMO_MODE", "true").lower() != "false"


def format_date_display(date_str):
    """Convert YYYYMMDD to MM/DD/YYYY format."""
    if not date_str or len(date_str) != 8:
        return date_str
    try:
        return f"{date_str[4:6]}/{date_str[6:8]}/{date_str[0:4]}"
    except:
        return date_str


def apply_plotly_theme(fig):
    """Apply a consistent brand theme to Plotly figures."""
    fig.update_layout(
        paper_bgcolor=BRAND_PAPER,
        plot_bgcolor=BRAND_PAPER,
        font=dict(color=BRAND_CHARCOAL, size=15),
        title_font=dict(color=BRAND_CHARCOAL, size=22),
        legend=dict(
            bgcolor="rgba(255, 253, 252, 0.92)",
            bordercolor=BRAND_BORDER,
            borderwidth=1,
        ),
        margin=dict(t=64, r=24, b=24, l=24),
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor=BRAND_BORDER,
        zeroline=False,
        linecolor=BRAND_BORDER,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=BRAND_BORDER,
        zeroline=False,
        linecolor=BRAND_BORDER,
    )


def _apply_small_n_suppression(df: pd.DataFrame, threshold: int = 5) -> tuple:
    """
    Remove rows where any count-like column falls below the suppression threshold.

    Returns (filtered_df, suppressed_count) so the caller can warn the user.
    A "count-like" column is an integer column whose name contains 'count',
    'orders', or starts/ends with 'n'.
    """
    count_cols = [
        c for c in df.columns
        if re.search(r'\b(count|orders?)\b|^n$|_n$', c.lower())
        and pd.api.types.is_numeric_dtype(df[c])
    ]
    if not count_cols:
        return df, 0

    keep_mask = pd.Series([True] * len(df), index=df.index)
    for col in count_cols:
        keep_mask &= df[col] >= threshold

    suppressed = int((~keep_mask).sum())
    return df[keep_mask].copy(), suppressed


def _display_query_visualization(df: pd.DataFrame, description: str):
    """Create visualizations for query results."""
    if df.empty:
        return

    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    string_cols = df.select_dtypes(include=['object']).columns.tolist()

    # Single-row result: show metric cards instead of a chart
    if len(df) == 1:
        cols = st.columns(min(len(numeric_cols), 4)) if numeric_cols else []
        for col, metric in zip(cols, numeric_cols[:4]):
            label = metric.replace("_", " ").title()
            val = df[metric].iloc[0]
            formatted = f"${val:,.2f}" if any(k in metric for k in ("revenue", "amount", "spend", "total", "avg", "tip", "discount")) else f"{val:,.0f}" if val == int(val) else f"{val:,.2f}"
            col.metric(label, formatted)
        return

    # Timeline/Trend charts
    if 'date' in df.columns and len(numeric_cols) > 0:
        col1, col2 = st.columns(2) if len(numeric_cols) > 1 else (st.columns(1)[0], None)

        with col1:
            metric = numeric_cols[0]
            fig = px.line(
                df,
                x='date',
                y=metric,
                markers=True,
                title=f"{metric.title()} Over Time"
            )
            fig.update_layout(height=300, hovermode='x unified')
            fig.update_traces(line_color=BRAND_BURGUNDY, marker_color=BRAND_BURGUNDY)
            apply_plotly_theme(fig)
            st.plotly_chart(fig, width='stretch')

        if col2 and len(numeric_cols) > 1:
            with col2:
                metric = numeric_cols[1]
                fig = px.bar(
                    df,
                    x='date',
                    y=metric,
                    title=f"{metric.title()} Over Time"
                )
                fig.update_layout(height=300)
                fig.update_traces(marker_color=BRAND_GOLD)
                apply_plotly_theme(fig)
                st.plotly_chart(fig, width='stretch')

    # Category comparisons
    elif string_cols and numeric_cols:
        for metric in numeric_cols[:2]:
            fig = px.bar(
                df.sort_values(metric, ascending=False),
                y=string_cols[0],
                x=metric,
                orientation='h',
                title=f"{string_cols[0].title()} by {metric.title()}"
            )
            fig.update_layout(height=400)
            fig.update_traces(marker_color=BRAND_BURGUNDY)
            apply_plotly_theme(fig)
            st.plotly_chart(fig, width='stretch')


def _render_alerts(sales_data: pd.DataFrame, inventory_data: pd.DataFrame, labor_data: pd.DataFrame):
    """Render actionable alerts from already-loaded DataFrames, capped at 5 per category."""
    OT_THRESHOLD = 8  # total overtime hours across all staff in a single day
    MAX_VISIBLE = 5

    def _fmt_date(date):
        d = pd.to_datetime(str(date), format="%Y%m%d", errors="coerce")
        return d.strftime("%m/%d") if not pd.isna(d) else str(date)

    st.subheader("Alerts")
    any_alerts = False

    # --- Inventory alerts ---
    if not inventory_data.empty:
        critical_items = inventory_data[inventory_data['status'] == 'critical']['item'].tolist()
        low_items = inventory_data[inventory_data['status'] == 'low']['item'].tolist()

        if critical_items:
            any_alerts = True
            visible, hidden = critical_items[:MAX_VISIBLE], critical_items[MAX_VISIBLE:]
            st.error(f"Inventory critical: {', '.join(visible)}" + (f" (+{len(hidden)} more)" if hidden else ""))
            if hidden:
                with st.expander(f"Show all {len(critical_items)} critical items"):
                    for item in hidden:
                        st.write(f"• {item}")
            st.caption("Inventory detail view is not currently shown in the app.")

        if low_items:
            any_alerts = True
            visible, hidden = low_items[:MAX_VISIBLE], low_items[MAX_VISIBLE:]
            st.warning(f"Inventory low: {', '.join(visible)}" + (f" (+{len(hidden)} more)" if hidden else ""))
            if hidden:
                with st.expander(f"Show all {len(low_items)} low-stock items"):
                    for item in hidden:
                        st.write(f"• {item}")
            st.caption("Inventory detail view is not currently shown in the app.")

    # --- Overtime alerts ---
    if not labor_data.empty and 'overtime_hours' in labor_data.columns:
        daily_ot = labor_data.groupby('date')['overtime_hours'].sum().sort_values(ascending=False)
        flagged = daily_ot[daily_ot > OT_THRESHOLD]
        if not flagged.empty:
            any_alerts = True
            visible_ot, hidden_ot = flagged.iloc[:MAX_VISIBLE], flagged.iloc[MAX_VISIBLE:]
            for date, hrs in visible_ot.items():
                st.warning(f"Overtime spike on {_fmt_date(date)}: {hrs:.1f} hrs total")
            if not hidden_ot.empty:
                with st.expander(f"Show {len(hidden_ot)} more overtime days"):
                    for date, hrs in hidden_ot.items():
                        st.write(f"• {_fmt_date(date)}: {hrs:.1f} hrs")
            st.caption("Labor analytics are not currently shown in the app.")

    if not any_alerts:
        st.success("No alerts for this period")


def _render_tip_breakdown(sales_data: pd.DataFrame):
    """Render channel-aware tip percentages without treating API orders as service alerts."""
    st.subheader("Tip Breakdown")

    required = {
        "dine_in_orders", "dine_in_subtotal", "dine_in_tips", "dine_in_zero_tip_orders",
        "api_orders", "api_subtotal", "api_tips", "api_zero_tip_orders",
    }
    if sales_data.empty or not required.issubset(sales_data.columns):
        st.info("Tip breakdown is not available for this data source.")
        return

    def _sum(col):
        return float(sales_data[col].fillna(0).sum()) if col in sales_data.columns else 0.0

    def _rate(num, den):
        return num / den if den else 0.0

    rows = []
    for label, prefix in [
        ("Dine-in-like", "dine_in"),
        ("API", "api"),
        ("Delivery", "delivery"),
        ("Takeout", "takeout"),
    ]:
        orders = _sum(f"{prefix}_orders")
        subtotal = _sum(f"{prefix}_subtotal")
        tips = _sum(f"{prefix}_tips")
        zero_tips = _sum(f"{prefix}_zero_tip_orders")
        if orders <= 0 and subtotal <= 0:
            continue
        rows.append({
            "Channel": label,
            "Orders": int(orders),
            "Tip % of Subtotal": _rate(tips, subtotal) * 100,
            "Zero-Tip Orders": _rate(zero_tips, orders) * 100,
            "Tips": tips,
        })

    if not rows:
        st.info("No tip-eligible channel data available for the selected period.")
        return

    dine_row = next((r for r in rows if r["Channel"] == "Dine-in-like"), None)
    if dine_row:
        dine_rate = dine_row["Tip % of Subtotal"]
        zero_tip_rate = dine_row["Zero-Tip Orders"]
        if dine_rate < 13 or zero_tip_rate >= 25:
            st.error(
                f"Dine-in-like tip rate: {dine_rate:.1f}% of subtotal; "
                f"{zero_tip_rate:.1f}% of orders have no recorded tip."
            )
        elif dine_rate < 15 or zero_tip_rate >= 18:
            st.warning(
                f"Dine-in-like tip rate: {dine_rate:.1f}% of subtotal; "
                f"{zero_tip_rate:.1f}% of orders have no recorded tip."
            )
        else:
            st.success(
                f"Dine-in-like tip rate: {dine_rate:.1f}% of subtotal; "
                f"{zero_tip_rate:.1f}% zero-tip orders."
            )

    display = pd.DataFrame(rows)
    st.dataframe(
        display,
        hide_index=True,
        width="stretch",
        column_config={
            "Channel": "Channel",
            "Orders": st.column_config.NumberColumn("Orders", format="%d"),
            "Tip % of Subtotal": st.column_config.NumberColumn("Tip % of Subtotal", format="%.1f%%"),
            "Zero-Tip Orders": st.column_config.NumberColumn("Zero-Tip Orders", format="%.1f%%"),
            "Tips": st.column_config.NumberColumn("Tips", format="$%.2f"),
        },
    )
    st.caption("API and delivery/takeout channels are context; dine-in-like is the service-focused signal.")


def show_clarification_ui(ambiguity_result: "AmbiguityResult"):
    """
    Display clarification options and collect user selection.

    Uses on_click callbacks to reliably capture button clicks in Streamlit's
    rerun model. The callback sets session state BEFORE the rerun happens.

    Args:
        ambiguity_result: AmbiguityResult with question and options

    Returns:
        dict with {question_id: selected_value} if user confirmed,
        "rephrase" if user wants to rephrase,
        None if no action taken yet
    """
    # Check if user already confirmed in a previous callback
    if st.session_state.get("clarification_confirmed"):
        result = st.session_state.clarification_confirmed
        st.session_state.clarification_confirmed = None  # Clear for next time
        return result

    st.warning(f"🤔 {ambiguity_result.question}")

    question_id = ambiguity_result.question_id
    radio_key = f"clarify_{question_id}"

    # Create options dict for easy lookup
    options_dict = {opt[0]: opt[1] for opt in ambiguity_result.options}

    selected = st.radio(
        "Please select:",
        options=[opt[0] for opt in ambiguity_result.options],
        format_func=lambda x: options_dict.get(x, x),
        key=radio_key,
        horizontal=True  # Show options horizontally for better visibility
    )

    # Show visual confirmation of current selection
    selected_label = options_dict.get(selected, selected)
    st.info(f"✅ **Selected:** {selected_label}")

    # Callback functions - these run BEFORE the rerun
    def on_confirm():
        # Read the selected value from session state (radio widget stores it there)
        selected_value = st.session_state.get(radio_key)
        st.session_state.clarification_confirmed = {question_id: selected_value}

    def on_rephrase():
        st.session_state.clarification_confirmed = "rephrase"

    col1, col2 = st.columns([1, 4])
    with col1:
        st.button("✓ Confirm", width='stretch', key="confirm_clarification", on_click=on_confirm, type="primary")
    with col2:
        st.button("Rephrase query instead", width='stretch', key="rephrase_query", on_click=on_rephrase)

    return None


def _short_location_name(full_name: str) -> str:
    """Extract city name from a full address string.

    "123 Main Street, Ste 1, Downtown, CA" → "Downtown"
    Falls back to the original string if it doesn't look like an address.
    """
    parts = [p.strip() for p in full_name.split(",")]
    if len(parts) >= 2:
        return parts[-2]
    return full_name


def _friendly_location_label(location_id: str, index: int) -> str:
    """Return a presentation-safe label for demo and unknown locations."""
    if location_id.startswith("demo_"):
        return location_id.replace("demo_", "").replace("_", " ").title()
    return f"Location {index}"


def load_location_map(db, toast_client=None) -> dict:
    """Return {uuid: display_name} for all known locations.

    Priority: cached JSON file → live Toast API → anonymized fallback.
    Never exposes raw UUIDs to the UI.
    """
    known_ids = sorted(db.get_locations())
    if not known_ids:
        return {}

    fallback_map = {
        uid: _friendly_location_label(uid, i + 1)
        for i, uid in enumerate(known_ids)
    }
    cache_path = Path("integrations/toast_api/location_names.json")
    if cache_path.exists():
        try:
            with open(cache_path) as f:
                cached = json.load(f)
            filtered = {
                k: _short_location_name(v)
                for k, v in cached.items()
                if k in fallback_map and v
            }
            if len(filtered) == len(known_ids):
                return filtered
            if filtered:
                merged = fallback_map.copy()
                merged.update(filtered)
                return merged
        except Exception:
            pass

    if toast_client:
        try:
            restaurants = toast_client.discover_restaurants()
            api_map = {
                r.get("restaurantGuid", ""): _short_location_name(
                    r.get("locationName", r.get("restaurantName", ""))
                )
                for r in restaurants
                if r.get("restaurantGuid") in fallback_map
            }
            if api_map:
                merged = fallback_map.copy()
                merged.update(api_map)
                return merged
        except Exception:
            pass

    return fallback_map


# ---------------------------------------------------------------------------
# Engagement Analysis helpers
# ---------------------------------------------------------------------------

def _empty_instagram_media() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "id", "date", "caption", "media_type", "permalink", "likes",
            "comments_count", "views", "reach", "saved", "shares",
            "total_interactions",
        ]
    )


def _normalize_instagram_media(media: pd.DataFrame) -> pd.DataFrame:
    if media is None or media.empty:
        return _empty_instagram_media()

    media = media.copy()
    if "date" not in media.columns:
        for candidate in ["posted_date", "posted_date_utc", "uploaded_date_utc", "timestamp"]:
            if candidate in media.columns:
                media["date"] = media[candidate]
                break
    media["date"] = pd.to_datetime(media.get("date"), errors="coerce").dt.normalize()
    media = media.dropna(subset=["date"])

    if "id" not in media.columns and "media_id" in media.columns:
        media["id"] = media["media_id"]
    media["caption"] = media.get("caption", "").fillna("").astype(str)
    for col in ["likes", "comments_count", "views", "reach", "saved", "shares"]:
        media[col] = pd.to_numeric(media[col] if col in media.columns else 0, errors="coerce")
    media["total_interactions"] = pd.to_numeric(
        media["total_interactions"] if "total_interactions" in media.columns else np.nan,
        errors="coerce",
    )

    media[["likes", "comments_count", "views", "saved", "shares"]] = (
        media[["likes", "comments_count", "views", "saved", "shares"]].fillna(0)
    )
    media["total_interactions"] = media["total_interactions"].fillna(
        media["likes"] + media["comments_count"] + media["shares"] + media["saved"]
    )
    median_interactions = media["total_interactions"].median()
    median_views = media.loc[media["views"] > 0, "views"].median() if (media["views"] > 0).any() else 0
    views_scale = float(median_interactions / median_views) if median_views and median_views > 0 else 0.0
    media["_views_scale"] = views_scale
    media["engagement_score"] = media["total_interactions"] + views_scale * media["views"]
    return media


@st.cache_data(show_spinner=False)
def _fit_bertopic_on_captions(captions_tuple: tuple) -> dict | None:
    try:
        from bertopic import BERTopic
        from hdbscan import HDBSCAN
        from sklearn.feature_extraction.text import CountVectorizer
        from umap import UMAP
    except ImportError:
        return None

    docs = [
        re.sub(r"#\w+", "", caption).strip()
        for caption in captions_tuple
        if isinstance(caption, str) and caption.strip()
    ]
    if len(docs) < 5:
        return None

    try:
        vectorizer_model = CountVectorizer(ngram_range=(1, 3), stop_words="english", min_df=2)
        n_docs = len(docs)
        topic_model = BERTopic(
            embedding_model="all-MiniLM-L6-v2",
            umap_model=UMAP(
                n_neighbors=min(12, max(2, n_docs // 8)),
                n_components=min(5, max(2, n_docs // 18)),
                min_dist=0.0,
                metric="cosine",
                random_state=42,
            ),
            hdbscan_model=HDBSCAN(
                min_cluster_size=3,
                min_samples=1,
                metric="euclidean",
                cluster_selection_method="eom",
                prediction_data=True,
            ),
            vectorizer_model=vectorizer_model,
            calculate_probabilities=True,
            verbose=False,
        )
        topics, probs = topic_model.fit_transform(docs)
        try:
            topics = topic_model.reduce_outliers(docs, topics, probabilities=probs, strategy="probabilities")
        except Exception:
            pass
    except Exception:
        return None

    topic_info = topic_model.get_topic_info()
    topic_words: dict[int, list[str]] = {}
    for tid in topic_info["Topic"].tolist():
        if tid == -1:
            continue
        words = topic_model.get_topic(tid) or []
        topic_words[tid] = [word for word, _ in words[:8]]

    if not topic_words:
        return None

    valid_idx = [
        i for i, caption in enumerate(captions_tuple)
        if isinstance(caption, str) and re.sub(r"#\w+", "", caption).strip()
    ]
    return {
        "valid_idx": valid_idx,
        "topics": list(topics),
        "topic_words": topic_words,
        "topic_info": topic_info.to_dict("records"),
        "num_outliers": list(topics).count(-1),
    }


def _classify_social_topics(media: pd.DataFrame, allow_bertopic: bool = True) -> tuple[pd.DataFrame, dict]:
    media = media.copy()
    if media.empty:
        for col in ["promo_topic", "product_topic", "event_topic"]:
            media[col] = pd.Series(dtype=int)
        return media, {"success": False, "num_outliers": 0, "topics": []}

    promo_kw = [
        "discount", "deal", "offer", "promo", "promotion", "special", "save",
        "gift card", "free", "giveaway", "coupon", "bundle", "rewards",
        "limited", "win", "lunch special", "happy hour",
    ]
    product_kw = [
        "dumpling", "dumplings", "xlb", "xiao long bao", "bao", "bun",
        "noodle", "noodles", "fried rice", "wonton", "potsticker",
        "scallion pancake", "chili oil", "dan dan", "beef", "shrimp",
        "pork", "chicken", "milk tea", "menu",
    ]
    event_kw = [
        "grand opening", "soft opening", "now open", "new location", "holiday",
        "christmas", "new year", "lunar", "valentine", "mother's day",
        "thanksgiving", "halloween", "anniversary", "event", "festival",
        "community", "catering", "collab",
    ]

    def kw_match(text: str, keywords: list[str]) -> int:
        low = text.lower()
        return int(any(keyword in low for keyword in keywords))

    bertopic_meta = (
        _fit_bertopic_on_captions(tuple(media["caption"].tolist()))
        if allow_bertopic
        else None
    )

    if bertopic_meta:
        promo_seeds = {"discount", "deal", "offer", "promo", "promotion", "special", "free", "bundle", "rewards"}
        product_seeds = {"dumpling", "dumplings", "xlb", "bao", "noodle", "noodles", "rice", "wonton", "chicken", "pork"}
        event_seeds = {"opening", "holiday", "christmas", "lunar", "event", "festival", "community", "catering"}
        topic_category = {}
        for tid, words in bertopic_meta["topic_words"].items():
            word_set = {word.lower() for word in words}
            scores = {
                "Promotions": len(word_set & promo_seeds),
                "Product Features": len(word_set & product_seeds),
                "Events & Holidays": len(word_set & event_seeds),
            }
            best = max(scores, key=scores.get)
            topic_category[tid] = best if scores[best] > 0 else "Other"

        media["_bt_topic"] = -1
        for doc_idx, media_idx in enumerate(bertopic_meta["valid_idx"]):
            if media_idx < len(media):
                media.iat[media_idx, media.columns.get_loc("_bt_topic")] = bertopic_meta["topics"][doc_idx]
        media["promo_topic"] = media["_bt_topic"].map(topic_category).eq("Promotions").astype(int)
        media["product_topic"] = media["_bt_topic"].map(topic_category).eq("Product Features").astype(int)
        media["event_topic"] = media["_bt_topic"].map(topic_category).eq("Events & Holidays").astype(int)
        unclassified = (media[["promo_topic", "product_topic", "event_topic"]].sum(axis=1) == 0)
        if unclassified.any():
            media.loc[unclassified, "promo_topic"] = media.loc[unclassified, "caption"].map(lambda t: kw_match(t, promo_kw))
            media.loc[unclassified, "product_topic"] = media.loc[unclassified, "caption"].map(lambda t: kw_match(t, product_kw))
            media.loc[unclassified, "event_topic"] = media.loc[unclassified, "caption"].map(lambda t: kw_match(t, event_kw))
        bertopic_meta = {
            "success": True,
            "num_outliers": bertopic_meta["num_outliers"],
            "topics": [
                {
                    "topic_id": rec["Topic"],
                    "label": ", ".join(bertopic_meta["topic_words"].get(rec["Topic"], ["(no keywords)"])[:5]),
                    "category": topic_category.get(rec["Topic"], "Other"),
                    "count": rec.get("Count", 0),
                }
                for rec in bertopic_meta["topic_info"]
                if rec["Topic"] != -1
            ],
        }
    else:
        media["promo_topic"] = media["caption"].map(lambda t: kw_match(t, promo_kw))
        media["product_topic"] = media["caption"].map(lambda t: kw_match(t, product_kw))
        media["event_topic"] = media["caption"].map(lambda t: kw_match(t, event_kw))
        bertopic_meta = {"success": False, "num_outliers": 0, "topics": []}

    return media, bertopic_meta


def build_social_daily(media: pd.DataFrame, allow_bertopic: bool = True) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    media = _normalize_instagram_media(media)
    media, bertopic_meta = _classify_social_topics(media, allow_bertopic=allow_bertopic)
    if media.empty:
        columns = [
            "date", "post_count", "total_likes", "total_views", "total_post_comments",
            "comment_count", "promo_topic_share", "product_topic_share", "event_topic_share",
            "weighted_promo_signal", "weighted_product_signal", "weighted_event_signal",
            "total_engagement", "engagement_index",
        ]
        return pd.DataFrame(columns=columns), media, bertopic_meta

    media["_wt_promo"] = media["engagement_score"] * media["promo_topic"]
    media["_wt_product"] = media["engagement_score"] * media["product_topic"]
    media["_wt_event"] = media["engagement_score"] * media["event_topic"]

    social_daily = (
        media.groupby("date", as_index=False)
        .agg(
            post_count=("id", "count"),
            total_likes=("likes", "sum"),
            total_views=("views", "sum"),
            total_post_comments=("comments_count", "sum"),
            comment_count=("comments_count", "sum"),
            promo_topic_share=("promo_topic", "mean"),
            product_topic_share=("product_topic", "mean"),
            event_topic_share=("event_topic", "mean"),
            weighted_promo_signal=("_wt_promo", "sum"),
            weighted_product_signal=("_wt_product", "sum"),
            weighted_event_signal=("_wt_event", "sum"),
            total_engagement=("engagement_score", "sum"),
        )
    )

    components = []
    for col in ["total_engagement", "comment_count", "post_count"]:
        col_min = social_daily[col].min()
        col_max = social_daily[col].max()
        if pd.isna(col_min) or pd.isna(col_max) or col_max == col_min:
            components.append(pd.Series(0, index=social_daily.index))
        else:
            components.append((social_daily[col] - col_min) / (col_max - col_min))
    social_daily["engagement_index"] = sum(components) * 100 / 3
    return social_daily.sort_values("date"), media, bertopic_meta


def build_engagement_features(
    db,
    selected_locations: list[str],
    location_map: dict,
    start_date: str,
    end_date: str,
    allow_bertopic: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    try:
        sales_daily = db.get_daily_drivers_data(start_date, end_date)
    except Exception as exc:
        logger.warning("Unable to load daily driver data for engagement analysis: %s", exc)
        sales_daily = pd.DataFrame()
    try:
        media = db.get_instagram_media(start_date, end_date)
    except Exception as exc:
        logger.warning("Unable to load Instagram media for engagement analysis: %s", exc)
        media = _empty_instagram_media()

    social_daily, media, bertopic_meta = build_social_daily(media, allow_bertopic=allow_bertopic)
    if sales_daily.empty:
        return pd.DataFrame(), media, bertopic_meta

    sales_daily = sales_daily.copy()
    sales_daily["date"] = pd.to_datetime(sales_daily["business_date"], errors="coerce").dt.normalize()
    selected_set = set(selected_locations)
    if selected_set:
        sales_daily = sales_daily[sales_daily["location_id"].astype(str).isin(selected_set)]

    if sales_daily.empty:
        return pd.DataFrame(), media, bertopic_meta

    sales_daily["revenue"] = pd.to_numeric(sales_daily.get("gross_revenue", 0), errors="coerce").fillna(0)
    sales_daily["transactions"] = pd.to_numeric(sales_daily.get("order_count", 0), errors="coerce").fillna(0)
    sales_daily["avg_ticket"] = np.where(
        sales_daily["transactions"] > 0,
        sales_daily["revenue"] / sales_daily["transactions"],
        0,
    )
    sales_daily["discount_amount"] = pd.to_numeric(sales_daily.get("total_discounts", 0), errors="coerce").fillna(0)
    sales_daily["promo_flag"] = sales_daily["discount_amount"] > 0

    if holidays_lib is not None and not sales_daily.empty:
        min_year = int(sales_daily["date"].dt.year.min())
        max_year = int(sales_daily["date"].dt.year.max())
        us_holidays = holidays_lib.US(years=range(min_year, max_year + 1))
        sales_daily["holiday_flag"] = sales_daily["date"].dt.date.map(lambda d: d in us_holidays)
        sales_daily["holiday_name"] = sales_daily["date"].dt.date.map(lambda d: us_holidays.get(d))
    else:
        sales_daily["holiday_flag"] = False
        sales_daily["holiday_name"] = None

    feature_cols = [
        "location_id", "date", "revenue", "transactions", "avg_ticket",
        "promo_flag", "discount_amount", "holiday_flag", "holiday_name",
    ]
    sales_features = sales_daily[feature_cols].copy()

    all_locations = (
        sales_features.groupby("date", as_index=False)
        .agg(
            revenue=("revenue", "sum"),
            transactions=("transactions", "sum"),
            promo_flag=("promo_flag", "max"),
            discount_amount=("discount_amount", "sum"),
            holiday_flag=("holiday_flag", "max"),
            holiday_name=("holiday_name", "first"),
        )
        .assign(location_id="ALL")
    )
    all_locations["avg_ticket"] = np.where(
        all_locations["transactions"] > 0,
        all_locations["revenue"] / all_locations["transactions"],
        0,
    )

    features = pd.concat([sales_features, all_locations[feature_cols]], ignore_index=True)
    features["location_label"] = features["location_id"].map(location_map).fillna(features["location_id"])
    features.loc[features["location_id"] == "ALL", "location_label"] = "All Selected Locations"
    features = features.merge(social_daily, on="date", how="left")

    fill_zero_cols = [
        "post_count", "total_likes", "total_views", "total_post_comments", "comment_count",
        "promo_topic_share", "product_topic_share", "event_topic_share",
        "weighted_promo_signal", "weighted_product_signal", "weighted_event_signal",
        "total_engagement", "engagement_index",
    ]
    for col in fill_zero_cols:
        if col not in features.columns:
            features[col] = 0
        features[col] = pd.to_numeric(features[col], errors="coerce").fillna(0)

    return features.sort_values(["location_id", "date"]).reset_index(drop=True), media, bertopic_meta


def _pick_peak_lag(series: pd.Series, outcome: pd.Series, max_lag: int = 14) -> int:
    best_abs_lag, best_abs_r = 1, 0.0
    best_pos_lag, best_pos_r = None, -np.inf
    s = pd.to_numeric(series, errors="coerce")
    y = pd.to_numeric(outcome, errors="coerce")
    for lag in range(1, max_lag + 1):
        shifted = s.shift(lag)
        mask = shifted.notna() & y.notna()
        if mask.sum() < 10:
            continue
        corr = np.corrcoef(shifted[mask], y[mask])[0, 1]
        if pd.isna(corr):
            continue
        if corr > best_pos_r:
            best_pos_r, best_pos_lag = corr, lag
        if abs(corr) > abs(best_abs_r):
            best_abs_r, best_abs_lag = corr, lag
    return int(best_pos_lag if best_pos_lag is not None and best_pos_r > 0 else best_abs_lag)


def _gaussian_lag_kernel(peak_lag: int, max_lag: int = 14) -> np.ndarray:
    peak_lag = int(max(1, min(max_lag, peak_lag)))
    lags = np.arange(1, max_lag + 1, dtype=float)
    sigma = max(1.5, peak_lag / 2.5)
    weights = np.exp(-0.5 * ((lags - peak_lag) / sigma) ** 2)
    total = weights.sum()
    return weights / total if total > 0 else np.repeat(1 / max_lag, max_lag)


def _distributed_lag_transform(series: pd.Series, kernel: np.ndarray) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce").fillna(0).to_numpy(dtype=float)
    result = np.zeros(len(values), dtype=float)
    for lag_idx, weight in enumerate(kernel, start=1):
        shifted = np.roll(values, lag_idx)
        shifted[:lag_idx] = 0.0
        result += weight * shifted
    return pd.Series(result, index=series.index)


def fit_dynamic_panel_model(
    features: pd.DataFrame,
    outcome_col: str = "revenue",
    location_id: str = "ALL",
    max_lag: int = 14,
) -> dict:
    result = {
        "weights": {},
        "pvalues": {},
        "conf_int": {},
        "r_squared": np.nan,
        "n_obs": 0,
        "optimal_lags": {},
        "n_locations": 0,
        "cumulative_pct": {},
        "response_curves": {},
        "social_signals": [],
        "error": None,
    }
    try:
        import statsmodels.formula.api as smf
    except ImportError:
        result["error"] = "statsmodels formula API not installed"
        return result

    if features.empty:
        result["error"] = "No data available."
        return result

    panel_df = features.copy()
    panel_df["date"] = pd.to_datetime(panel_df["date"], errors="coerce").dt.normalize()
    panel_df = panel_df[panel_df["location_id"] != "ALL"] if location_id == "ALL" else panel_df[panel_df["location_id"] == location_id]
    if panel_df.empty:
        result["error"] = "No data available after filtering."
        return result

    panel_df = panel_df.sort_values(["location_id", "date"]).reset_index(drop=True)
    result["n_locations"] = int(panel_df["location_id"].nunique())
    weighted_signals = ["weighted_promo_signal", "weighted_product_signal", "weighted_event_signal"]
    use_weighted = any(pd.to_numeric(panel_df[col], errors="coerce").fillna(0).sum() > 0 for col in weighted_signals)
    social_signals = ["engagement_index"] + (
        weighted_signals if use_weighted else ["promo_topic_share", "product_topic_share", "event_topic_share"]
    )
    result["social_signals"] = social_signals

    daily_source = (
        panel_df.groupby("date", as_index=False)
        .agg(
            outcome=(outcome_col, "sum"),
            engagement_index=("engagement_index", "mean"),
            weighted_promo_signal=("weighted_promo_signal", "mean"),
            weighted_product_signal=("weighted_product_signal", "mean"),
            weighted_event_signal=("weighted_event_signal", "mean"),
            promo_topic_share=("promo_topic_share", "mean"),
            product_topic_share=("product_topic_share", "mean"),
            event_topic_share=("event_topic_share", "mean"),
        )
        .sort_values("date")
    )

    distributed_daily = daily_source[["date"]].copy()
    kernels = {}
    for signal in social_signals:
        peak_lag = _pick_peak_lag(daily_source[signal], daily_source["outcome"], max_lag=max_lag)
        kernel = _gaussian_lag_kernel(peak_lag, max_lag=max_lag)
        result["optimal_lags"][signal] = peak_lag
        kernels[signal] = kernel
        distributed_daily[f"dl__{signal}"] = _distributed_lag_transform(daily_source[signal], kernel)

    panel_df = panel_df.merge(distributed_daily, on="date", how="left")
    outcome_raw = pd.to_numeric(panel_df[outcome_col], errors="coerce").fillna(0).clip(lower=0)
    panel_df["outcome_log"] = np.log1p(outcome_raw)
    panel_df["lag_outcome_1"] = panel_df.groupby("location_id")["outcome_log"].shift(1)
    panel_df["lag_outcome_7"] = panel_df.groupby("location_id")["outcome_log"].shift(7)
    panel_df["trend_idx"] = (panel_df["date"] - panel_df["date"].min()).dt.days.astype(float)
    panel_df["dow"] = panel_df["date"].dt.day_name().str[:3]
    panel_df["month_num"] = panel_df["date"].dt.month.astype(str)
    panel_df["holiday_flag"] = panel_df["holiday_flag"].astype(float)
    panel_df["promo_flag"] = panel_df["promo_flag"].astype(float)

    dl_cols = [f"dl__{signal}" for signal in social_signals]
    baseline_cols = ["lag_outcome_1", "lag_outcome_7", "trend_idx"]
    standardized_cols = baseline_cols + dl_cols + ["holiday_flag", "promo_flag"]
    reg_df = panel_df[["date", "location_id", "outcome_log", "dow", "month_num"] + standardized_cols].dropna().copy()
    if len(reg_df) < 45:
        result["error"] = f"Too few complete observations after dynamic controls ({len(reg_df)}); need at least 45."
        return result

    for col in standardized_cols:
        std = reg_df[col].std()
        reg_df[f"{col}_z"] = 0.0 if pd.isna(std) or std <= 0 else (reg_df[col] - reg_df[col].mean()) / std

    formula_terms = [f"{col}_z" for col in baseline_cols + dl_cols + ["holiday_flag", "promo_flag"]]
    formula_terms += ["C(dow)"]
    if reg_df["month_num"].nunique() > 1:
        formula_terms += ["C(month_num)"]
    if reg_df["location_id"].nunique() > 1:
        formula_terms += ["C(location_id)"]

    try:
        fitted = smf.ols("outcome_log ~ " + " + ".join(formula_terms), data=reg_df).fit()
    except Exception as exc:
        result["error"] = f"Dynamic panel model failed: {exc}"
        return result

    result["n_obs"] = int(len(reg_df))
    result["r_squared"] = float(getattr(fitted, "rsquared", np.nan))
    ci = fitted.conf_int()
    predictor_map = {signal: f"dl__{signal}_z" for signal in social_signals}
    predictor_map.update({"holiday_flag": "holiday_flag_z", "promo_flag": "promo_flag_z"})
    for predictor, model_col in predictor_map.items():
        beta = float(fitted.params.get(model_col, np.nan))
        pval = float(fitted.pvalues.get(model_col, np.nan))
        result["weights"][predictor] = beta
        result["pvalues"][predictor] = pval
        if model_col in ci.index:
            result["conf_int"][predictor] = (float(ci.loc[model_col, 0]), float(ci.loc[model_col, 1]))
        if predictor in social_signals and not np.isnan(beta):
            result["cumulative_pct"][predictor] = float((np.exp(beta) - 1) * 100)
            result["response_curves"][predictor] = [
                {"lag": lag_idx, "effect_pct": float((np.exp(beta * weight) - 1) * 100)}
                for lag_idx, weight in enumerate(kernels[predictor], start=1)
            ]
    return result


def compute_lag_correlations(features: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return pd.DataFrame(columns=["location_id", "outcome", "signal", "lag", "correlation"])

    signals = [
        "engagement_index", "promo_topic_share", "product_topic_share", "event_topic_share",
        "weighted_promo_signal", "weighted_product_signal", "weighted_event_signal",
    ]
    outcomes = ["revenue", "transactions"]
    rows = []
    for location_id, loc_df in features.groupby("location_id"):
        loc_df = loc_df.sort_values("date").reset_index(drop=True)
        numeric = {col: pd.to_numeric(loc_df.get(col, 0), errors="coerce") for col in signals + outcomes}
        for signal in signals:
            shifted = pd.concat([numeric[signal].shift(lag).rename(lag) for lag in range(1, 15)], axis=1)
            for outcome in outcomes:
                correlations = shifted.corrwith(numeric[outcome])
                for lag, corr_value in correlations.items():
                    rows.append({
                        "location_id": location_id,
                        "outcome": outcome,
                        "signal": signal,
                        "lag": int(lag),
                        "correlation": float(corr_value) if pd.notna(corr_value) else np.nan,
                    })
    return pd.DataFrame(rows)


def friendly_label_map() -> dict[str, str]:
    return {
        "engagement_index": "Instagram Engagement",
        "promo_topic_share": "Promotions",
        "product_topic_share": "Product Features",
        "event_topic_share": "Events and Holidays",
        "weighted_promo_signal": "Promo Content x Engagement",
        "weighted_product_signal": "Product Content x Engagement",
        "weighted_event_signal": "Event Content x Engagement",
        "holiday_flag": "Public Holiday",
        "promo_flag": "In-Store Promotion",
    }


def _confidence_band(abs_corr: float, pvalue: float, n_obs: int) -> str:
    abs_corr = 0.0 if pd.isna(abs_corr) else float(abs_corr)
    if pd.isna(pvalue):
        return "Moderate" if abs_corr >= 0.25 and n_obs >= 90 else "Directional" if abs_corr >= 0.15 else "Low"
    if pvalue < 0.10 and abs_corr >= 0.20 and n_obs >= 90:
        return "High"
    if pvalue < 0.20 and abs_corr >= 0.15 and n_obs >= 60:
        return "Moderate"
    if abs_corr >= 0.10 or pvalue < 0.20:
        return "Directional"
    return "Low"


def _build_predictive_summary(
    lag_filtered: pd.DataFrame,
    model: dict,
    signal_labels: dict[str, str],
    n_days: int,
) -> pd.DataFrame:
    lag_lookup = {
        signal: grp.dropna(subset=["correlation"]).sort_values("lag")
        for signal, grp in lag_filtered.groupby("signal")
    }
    rows = []
    for signal in list(dict.fromkeys(["engagement_index"] + list(model.get("weights", {}).keys()))):
        signal_lags = lag_lookup.get(signal, pd.DataFrame())
        chosen_lag = model.get("optimal_lags", {}).get(signal, np.nan)
        chosen_corr = np.nan
        if not signal_lags.empty:
            if pd.notna(chosen_lag):
                lag_match = signal_lags[signal_lags["lag"] == int(chosen_lag)]
                if not lag_match.empty:
                    chosen_corr = float(lag_match["correlation"].iloc[0])
            if pd.isna(chosen_corr):
                positive = signal_lags[signal_lags["correlation"] > 0]
                ref = positive.loc[positive["correlation"].idxmax()] if not positive.empty else signal_lags.loc[signal_lags["correlation"].abs().idxmax()]
                chosen_lag = int(ref["lag"])
                chosen_corr = float(ref["correlation"])

        beta = float(model.get("weights", {}).get(signal, np.nan))
        pvalue = float(model.get("pvalues", {}).get(signal, np.nan))
        if np.isnan(beta) and pd.isna(chosen_corr):
            continue
        direction_source = beta if not np.isnan(beta) else chosen_corr
        rows.append({
            "signal": signal,
            "Predictor": signal_labels.get(signal, signal),
            "Lead Time": chosen_lag,
            "Direction": "Positive" if direction_source > 0 else "Negative" if direction_source < 0 else "Neutral",
            "Peak r": chosen_corr,
            "Std. beta": beta,
            "pvalue": pvalue,
            "Confidence": _confidence_band(abs(chosen_corr) if pd.notna(chosen_corr) else np.nan, pvalue, n_days),
            "_rank": abs(beta) if not np.isnan(beta) else abs(chosen_corr) if pd.notna(chosen_corr) else 0,
        })
    if not rows:
        return pd.DataFrame(columns=["signal", "Predictor", "Lead Time", "Direction", "Peak r", "Std. beta", "Confidence"])
    return pd.DataFrame(rows).sort_values("_rank", ascending=False).drop(columns="_rank").reset_index(drop=True)


def _render_summary_card(title: str, value: str, delta: str | None = None, body: str | None = None) -> None:
    delta_html = f"<div style='margin-top:8px;color:{STATUS_GOOD};font-weight:700;font-size:0.82rem;'>{delta}</div>" if delta else ""
    body_html = f"<div style='margin-top:8px;color:{BRAND_MUTED};font-size:0.82rem;line-height:1.45;'>{body}</div>" if body else ""
    st.html(
        f"<div style='background:{BRAND_PAPER};border:1px solid {BRAND_BORDER};border-radius:8px;padding:14px;min-height:150px;'>"
        f"<div style='color:{BRAND_MUTED};font-size:0.84rem;font-weight:700;'>{title}</div>"
        f"<div style='color:{BRAND_CHARCOAL};font-size:1.7rem;font-weight:800;line-height:1.1;margin-top:8px;overflow-wrap:anywhere;'>{value}</div>"
        f"{delta_html}"
        f"{body_html}"
        f"</div>"
    )


def render_engagement_analysis_tab(
    db,
    selected_locations: list[str],
    location_map: dict,
    start_date: str,
    end_date: str,
    allow_bertopic: bool = True,
):
    st.subheader("Engagement Analysis")
    st.caption(
        "Brand-level Instagram activity is compared with selected-location sales. "
        "Use this as a planning signal, not proof of causation."
    )

    features, media_df, bertopic_meta = build_engagement_features(
        db, selected_locations, location_map, start_date, end_date, allow_bertopic=allow_bertopic
    )
    if features.empty:
        st.info("No sales driver data is available for engagement analysis in this date range.")
        return

    labels = friendly_label_map()
    scope_options = {"All Selected Locations": "ALL"}
    for loc_id in selected_locations:
        if loc_id in set(features["location_id"].astype(str)):
            scope_options[location_map.get(loc_id, loc_id)] = loc_id

    ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 2])
    with ctrl1:
        scope_label = st.selectbox("Sales Scope", list(scope_options.keys()), key="engagement_scope")
    with ctrl2:
        outcome_label = st.selectbox("Sales Outcome", ["Revenue", "Transactions"], key="engagement_outcome")
    with ctrl3:
        smoothing = st.toggle("7-day rolling average", value=True, key="engagement_smoothing")

    scope_value = scope_options[scope_label]
    outcome_col = "revenue" if outcome_label == "Revenue" else "transactions"
    filtered = features[features["location_id"] == scope_value].copy().sort_values("date")
    if filtered.empty:
        st.info("No data is available for the selected engagement filters.")
        return

    n_days = len(filtered)
    plot_df = filtered.copy()
    if smoothing:
        for col in [outcome_col, "engagement_index", "total_engagement"]:
            plot_df[col] = plot_df[col].rolling(7, min_periods=1).mean()

    total_revenue = filtered["revenue"].sum()
    total_transactions = filtered["transactions"].sum()
    total_posts = int(filtered["post_count"].sum())
    total_views = int(filtered["total_views"].sum())
    avg_ticket = total_revenue / total_transactions if total_transactions else 0
    avg_likes = int(filtered["total_likes"].sum() / total_posts) if total_posts else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Revenue", format_compact_number(total_revenue, currency=True), help=format_currency(total_revenue))
    k2.metric("Transactions", format_compact_number(total_transactions), help=format_number(total_transactions))
    k3.metric("Avg Ticket", format_compact_number(avg_ticket, currency=True), help=format_currency(avg_ticket))
    k4.metric("Posts", format_number(total_posts))
    k5.metric("Avg Likes/Post", format_number(avg_likes))

    if media_df.empty:
        st.info("No Instagram media rows were found for this period. Sales metrics are shown, but social signals are zero.")
    elif bertopic_meta.get("success"):
        st.caption(f"Topic classification used BERTopic with {len(bertopic_meta.get('topics', []))} discovered topics.")
    else:
        st.caption("Topic classification used the keyword fallback because BERTopic is unavailable or not useful for this sample.")

    lag_correlations = compute_lag_correlations(features)
    lag_filtered = lag_correlations[
        (lag_correlations["location_id"] == scope_value)
        & (lag_correlations["outcome"] == outcome_col)
    ].copy()
    lag_filtered["signal_label"] = lag_filtered["signal"].map(labels)
    model = fit_dynamic_panel_model(features, outcome_col=outcome_col, location_id=scope_value)
    summary = _build_predictive_summary(lag_filtered, model, labels, n_days)

    st.divider()
    st.markdown("### Predictive Impact Summary")
    model_r2 = model.get("r_squared", np.nan)
    confidence = "High" if model_r2 >= 0.35 and n_days >= 120 else "Moderate" if model_r2 >= 0.20 and n_days >= 75 else "Directional"
    top_row = summary.iloc[0] if not summary.empty else None
    c1, c2, c3 = st.columns(3)
    with c1:
        if top_row is not None:
            _render_summary_card(
                "Top Signal",
                top_row["Predictor"],
                delta=f"{int(top_row['Lead Time'])}d lead" if pd.notna(top_row["Lead Time"]) else None,
                body=f"{top_row['Direction']} relationship; confidence {top_row['Confidence']}.",
            )
        else:
            _render_summary_card("Top Signal", "No clear signal")
    with c2:
        _render_summary_card(
            "Evidence Level",
            confidence,
            delta=f"R2 {model_r2:.2f}" if not np.isnan(model_r2) else None,
            body=f"{n_days} days in the selected scope.",
        )
    with c3:
        _render_summary_card(
            "Instagram Coverage",
            format_number(total_posts),
            delta=f"{format_compact_number(total_views)} views" if total_views else None,
            body="Posts are brand-level and shared across stores.",
        )

    if model.get("error"):
        st.warning(f"Dynamic model could not be fit: {model['error']}")

    if not summary.empty:
        display_summary = summary.copy()
        display_summary["Lead Time"] = display_summary["Lead Time"].map(lambda v: f"{int(v)}d" if pd.notna(v) else "")
        display_summary["Peak r"] = display_summary["Peak r"].map(lambda v: f"{v:+.2f}" if pd.notna(v) else "")
        display_summary["Std. beta"] = display_summary["Std. beta"].map(lambda v: f"{v:+.2f}" if not np.isnan(v) else "")
        st.dataframe(
            display_summary[["Predictor", "Lead Time", "Direction", "Peak r", "Std. beta", "Confidence"]],
            hide_index=True,
            width="stretch",
        )

    st.divider()
    st.markdown("### Sales and Instagram Activity")
    fig_ts = go.Figure()
    fig_ts.add_trace(go.Scatter(
        x=plot_df["date"],
        y=plot_df[outcome_col],
        mode="lines+markers",
        name=outcome_label,
        line=dict(color=BRAND_BURGUNDY, width=3),
        yaxis="y",
    ))
    fig_ts.add_trace(go.Bar(
        x=plot_df["date"],
        y=plot_df["engagement_index"],
        name="Instagram Engagement Index",
        marker_color=with_alpha(BRAND_SLATE, 0.35),
        yaxis="y2",
    ))
    fig_ts.update_layout(
        title=f"{outcome_label} vs Instagram Engagement",
        height=430,
        hovermode="x unified",
        yaxis=dict(title=outcome_label, tickprefix="$" if outcome_col == "revenue" else ""),
        yaxis2=dict(title="Engagement Index", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.1, x=0),
    )
    apply_plotly_theme(fig_ts)
    st.plotly_chart(fig_ts, width="stretch")

    st.markdown("### Lead-Lag Correlation")
    heatmap_source = (
        lag_filtered.dropna(subset=["correlation"])
        .pivot(index="signal_label", columns="lag", values="correlation")
        .reindex([labels[s] for s in [
            "engagement_index", "promo_topic_share", "product_topic_share", "event_topic_share",
            "weighted_promo_signal", "weighted_product_signal", "weighted_event_signal",
        ]])
    )
    if heatmap_source.dropna(how="all").empty:
        st.info("No lag-correlation signal is available for this date range.")
    else:
        fig_hm = px.imshow(
            heatmap_source,
            labels=dict(x="Days After Instagram Activity", y="Signal", color="Correlation"),
            color_continuous_scale="RdBu",
            zmin=-1,
            zmax=1,
            aspect="auto",
            title=f"Instagram to {outcome_label}: Correlation at Each Lag",
            text_auto=".2f",
        )
        fig_hm.update_layout(height=520, margin=dict(l=180, r=90, t=70, b=60))
        apply_plotly_theme(fig_hm)
        st.plotly_chart(fig_hm, width="stretch")


def main():
    # Header
    st.title("DoughZone Analytics Dashboard")

    # Database connection
    db = get_db()

    if db is None:
        st.error("Failed to initialize the database connection. The app cannot continue.")
        return

    _demo_mode = bool(getattr(db, "is_demo", False))
    if _demo_mode:
        st.info("**Demo Mode** — All data shown is synthetic and safe for presentation. No live systems are queried.")

    # Sidebar - Location and Date Selection
    st.sidebar.header("Dashboard Controls")

    # Load location name map (file cache → API → anonymized fallback)
    try:
        location_map = load_location_map(db)  # {uuid: display_name}
        if not location_map:
            st.warning("No locations found in the active dataset.")
            return
    except Exception as e:
        st.error(f"Error loading locations: {e}")
        return

    name_to_id = {v: k for k, v in location_map.items()}  # {display_name: uuid}
    all_names = sorted(name_to_id.keys())

    # Location multi-selector with type-to-search (built into st.multiselect)
    selected_names = st.sidebar.multiselect(
        "Select Location(s)",
        options=all_names,
        default=all_names,
        placeholder="Type to search...",
    )

    if not selected_names:
        st.warning("Select at least one location to view data.")
        st.stop()

    selected_locations = [name_to_id[n] for n in selected_names]

    # Get available dates for selected locations
    try:
        available_dates = db.get_available_dates(selected_locations)
        if not available_dates:
            st.warning(f"No data available for the selected location(s).")
            return
    except Exception as e:
        st.error(f"Error loading dates: {e}")
        return

    # Convert available_dates (YYYYMMDD strings, DESC order) to date objects for bounds
    _date_objs = [datetime.strptime(d, "%Y%m%d").date() for d in available_dates]
    _min_date = _date_objs[-1]   # oldest
    _max_date = _date_objs[0]    # newest
    _default_start = _date_objs[min(29, len(_date_objs) - 1)]
    _default_end = _date_objs[0]

    # Date range selector with calendar popup
    st.sidebar.subheader("Date Range")
    date_range = st.sidebar.date_input(
        "Select date range",
        value=(_default_start, _default_end),
        min_value=_min_date,
        max_value=_max_date,
        format="MM/DD/YYYY",
        key="date_range",
    )

    if len(date_range) < 2:
        st.sidebar.info("Select an end date to complete the range.")
        st.stop()

    start_date = date_range[0].strftime("%Y%m%d")
    end_date = date_range[1].strftime("%Y%m%d")

    if start_date > end_date:
        st.sidebar.error("Start date must be before end date.")
        st.stop()

    if not any(start_date <= d <= end_date for d in available_dates):
        st.sidebar.warning("No data found in the selected range. Try expanding your date range.")

    if not _demo_mode:
        # BigQuery Architecture panel
        st.sidebar.divider()
        with st.sidebar.expander("Warehouse Architecture", expanded=False):
            st.markdown("""
**Production** connects to a cloud data warehouse via a read-only service account.

The generated SQL shown in the Q&A section matches the production query pattern.
            """)

        # Data Sync Section
        st.sidebar.divider()
        with st.sidebar.expander("Data Sync", expanded=False):
            st.caption("Import pending files from cloud storage")

            if 'last_sync_time' not in st.session_state:
                st.session_state.last_sync_time = 0

            current_time = time.time()
            time_since_last_sync = current_time - st.session_state.last_sync_time
            cooldown_seconds = 60

            if time_since_last_sync < cooldown_seconds:
                remaining = int(cooldown_seconds - time_since_last_sync)
                st.button(f"Wait {remaining}s", disabled=True, width='stretch', key="sync_btn_disabled")
            else:
                if st.button("Sync Now", width='stretch', key="sync_btn_active"):
                    st.session_state.last_sync_time = current_time
                    with st.spinner("Syncing data..."):
                        try:
                            from automation.gcs_import_worker import GCSImportWorker, load_credentials_from_env

                            creds = load_credentials_from_env()
                            if not creds:
                                st.error("Missing or invalid credentials")
                            else:
                                worker = GCSImportWorker(creds['bucket_name'], creds['credentials_path'])
                                stats = worker.process_new_files()

                                if stats['status'] == 'success':
                                    if stats['files_found'] > 0:
                                        st.success(stats['message'])
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.info("No new files found")
                                else:
                                    st.error(f"Sync failed: {stats['message']}")
                        except Exception as e:
                            st.error(f"Error: {e}")

        with st.sidebar.expander("Source API Sync", expanded=False):
            st.caption("Pull latest data directly from the source API")

            if 'last_toast_sync_time' not in st.session_state:
                st.session_state.last_toast_sync_time = 0

            toast_current_time = time.time()
            toast_time_since = toast_current_time - st.session_state.last_toast_sync_time
            toast_cooldown = 120

            if toast_time_since < toast_cooldown:
                remaining = int(toast_cooldown - toast_time_since)
                st.button(f"Wait {remaining}s", disabled=True, width='stretch', key="toast_btn_disabled")
            else:
                if st.button("Pull from Source API", width='stretch', key="toast_btn_active"):
                    st.session_state.last_toast_sync_time = toast_current_time
                    with st.spinner("Pulling from source API... this may take a few minutes."):
                        try:
                            from integrations.toast_api.client import ToastAPIClient
                            from integrations.toast_api.scheduler import pull_restaurant

                            toast_client = ToastAPIClient()
                            restaurants = toast_client.discover_restaurants()
                            if not restaurants:
                                st.warning("No accessible locations found.")
                            else:
                                total_orders = 0
                                for r in restaurants:
                                    guid = r.get("restaurantGuid", r.get("guid", ""))
                                    name = r.get("restaurantName", r.get("name", guid))
                                    result = pull_restaurant(
                                        client=toast_client,
                                        bq=db,
                                        restaurant_guid=guid,
                                        restaurant_name=name,
                                        interval_days=30,
                                    )
                                    total_orders += result.get("orders", 0)
                                if total_orders > 0:
                                    st.success(f"Pulled {total_orders:,} orders")
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.info("Already up to date.")
                        except Exception as e:
                            st.error(f"Source API error: {e}")

    # Initialize session state for clarification workflow
    if "clarification_pending" not in st.session_state:
        st.session_state.clarification_pending = None
    if "clarification_selections" not in st.session_state:
        st.session_state.clarification_selections = {}
    if "original_query" not in st.session_state:
        st.session_state.original_query = None
    if "clarification_confirmed" not in st.session_state:
        st.session_state.clarification_confirmed = None

    # Load data
    try:
        sales_data = db.get_sales_summary(selected_locations, start_date, end_date)
        menu_data = db.get_menu_performance(selected_locations, start_date, end_date)
        # inventory_data stub disabled — Toast API has no real stock levels (all zeros).
        # Kept as empty DataFrame so _render_alerts still works without erroring.
        inventory_data = pd.DataFrame()
        # reviews_data stub disabled — not yet rendered in the dashboard.
        # Kept as empty DataFrame to avoid NameError if referenced elsewhere.
        reviews_data = pd.DataFrame()
        # reviews_data = db.get_reviews(selected_locations, start_date, end_date)
        # labor_data stub disabled — labor data access not yet available.
        # Kept as empty DataFrame so _render_alerts still works without erroring.
        labor_data = pd.DataFrame()
        # labor_data = db.get_labor_analytics(selected_locations, start_date, end_date)
        drivers_data = db.get_daily_drivers_data(start_date, end_date)
        customer_data = db.get_customer_analytics(selected_locations, start_date, end_date)
        menu_args = (selected_locations, start_date, end_date)
        menu_recommendations = _call_optional_df(
            db,
            "get_menu_recommendations",
            lambda: _build_menu_recommendations_fallback(menu_data),
            *menu_args,
        )
        bundle_data = _call_optional_df(
            db,
            "get_bundle_opportunities",
            lambda: _empty_df(BUNDLE_OPPORTUNITY_COLUMNS),
            *menu_args,
        )
        promo_data = _call_optional_df(
            db,
            "get_promo_opportunities",
            lambda: _empty_df(PROMO_OPPORTUNITY_COLUMNS),
            *menu_args,
        )
        price_margin_data = _call_optional_df(
            db,
            "get_price_margin_candidates",
            lambda: menu_recommendations[
                menu_recommendations["recommended_action"].isin(["Re-price", "Rework", "Remove"])
            ].sort_values(["recommended_action", "net_revenue"], ascending=[True, False]),
            *menu_args,
        )
        dow_data = _call_optional_df(
            db,
            "get_day_of_week_index",
            lambda: _empty_df(["day", "index", "avg_revenue"]),
            *menu_args,
        )
        rfm_data = _call_optional_df(
            db,
            "get_rfm_segments",
            lambda: _empty_df(["segment", "n_customers", "pct", "avg_total_spend", "avg_visits", "avg_recency_days"]),
            *menu_args,
        )
    except Exception as e:
        st.error(f"Error loading data: {e}")
        logger.error(f"Data load error: {e}", exc_info=True)
        return

    # Horizontal tab navigation (replaces sidebar buttons)
    tab_overview, tab_sales, tab_menu, tab_menu_opt, tab_drivers, tab_customers, tab_engagement = st.tabs([
        "Overview",
        "Sales Analytics",
        "Menu Performance",
        "Menu Optimization",
        "Revenue Drivers",
        "Customer Analytics",
        "Engagement Analysis",
    ])

    # TAB 1: OVERVIEW (with LLM Ask feature at top)
    with tab_overview:
        # Part 1: LLM "Ask a Question" section
        st.subheader("Data Exploration Q&A Tool")
        if _demo_mode:
            st.info("Demo mode uses a local query generator for common aggregate questions. No live systems are queried.")
        else:
            st.markdown("Ask your question about the data in plain English, and we'll generate the SQL query to answer it.")

        user_question = ""
        ask_triggered = False
        user_question = st.text_input(
            "Ask a question about your data (e.g., 'What are the top 10 items by revenue?')",
            placeholder="e.g., 'Show me daily revenue trends', 'What's the average order value?'",
            key="user_question",
            on_change=lambda: st.session_state.update(ask_triggered=True) if st.session_state.get("user_question") else None
        )

        ask_button = st.button("Ask", width='stretch')
        ask_triggered = ask_button or st.session_state.get("ask_triggered", False)
        if ask_triggered:
            st.session_state.ask_triggered = False

        # Check for pending clarification FIRST (before ask_triggered check)
        if st.session_state.clarification_pending:
            clarification = show_clarification_ui(
                st.session_state.clarification_pending
            )
            if clarification == "rephrase":
                st.session_state.clarification_pending = None
                st.session_state.original_query = None
                st.session_state.clarification_selections = {}
                st.rerun()
            elif clarification:
                st.session_state.clarification_selections.update(clarification)
                st.session_state.clarification_pending = None

                query_to_process = st.session_state.original_query
                if query_to_process:
                    with st.spinner("🔄 Generating query with your clarification..."):
                        try:
                            query_gen, validator = get_query_tools(db)

                            query, description, params = query_gen.generate_query(
                                query_to_process,
                                selected_locations[0],
                                start_date,
                                end_date,
                                st.session_state.clarification_selections
                            )

                            if query:
                                valid, error = validator.validate(query, params)
                                if not valid:
                                    st.error(f"❌ Invalid query generated: {error}")
                                else:
                                    query_job = db.execute(query, params)
                                    result_df = query_job.to_dataframe()
                                    result_df, suppressed = _apply_small_n_suppression(result_df)

                                    st.success("✅ Query generated successfully")
                                    st.info(f"📊 **What this shows:** {description}")
                                    if suppressed > 0:
                                        st.warning(
                                            f"⚠️ {suppressed} row(s) with fewer than 5 records were "
                                            "suppressed to protect privacy (insufficient data)."
                                        )

                                    st.dataframe(result_df, width='stretch', hide_index=True)
                                    if len(result_df) > 0:
                                        _display_query_visualization(result_df, description)
                                    with st.expander("📝 View Generated SQL"):
                                        st.code(query, language="sql")
                            else:
                                st.warning(description)

                            st.session_state.original_query = None
                            st.session_state.clarification_selections = {}

                        except Exception as e:
                            st.error(f"❌ Error: {e}")
                            logger.error(f"Query error: {e}", exc_info=True)

        # Process NEW natural language query
        elif ask_triggered and user_question:
            _q = user_question.strip()
            _q_upper = _q.upper()
            _SQL_KEYWORDS = ("SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "WITH", "FROM", "WHERE")
            if any(re.match(rf'^\s*{kw}\b', _q_upper) for kw in _SQL_KEYWORDS) or \
               sum(kw in _q_upper for kw in ("SELECT", "FROM", "WHERE", "JOIN", "GROUP BY")) >= 2:
                st.error("Please ask your question in plain English. SQL queries cannot be entered directly.")
                st.stop()

            with st.spinner("🔄 Understanding your question..."):
                try:
                    query_gen, validator = get_query_tools(db)

                    ambiguity_result = query_gen.detect_ambiguity(user_question)

                    if ambiguity_result.is_ambiguous:
                        st.session_state.clarification_pending = ambiguity_result
                        st.session_state.original_query = user_question
                        st.rerun()
                    else:
                        query, description, params = query_gen.generate_query(
                            user_question,
                            selected_locations[0],
                            start_date,
                            end_date,
                            st.session_state.clarification_selections
                        )

                        if query:
                            valid, error = validator.validate(query, params)

                            if not valid:
                                st.error(f"❌ Invalid query generated: {error}")
                                logger.error(f"SQL validation failed: {error}")
                            else:
                                query_job = db.execute(query, params)
                                result_df = query_job.to_dataframe()
                                result_df, suppressed = _apply_small_n_suppression(result_df)

                                st.success("✅ Query generated successfully")
                                st.info(f"📊 **What this shows:** {description}")
                                if suppressed > 0:
                                    st.warning(
                                        f"⚠️ {suppressed} row(s) with fewer than 5 records were "
                                        "suppressed to protect privacy (insufficient data)."
                                    )

                                st.dataframe(result_df, width='stretch', hide_index=True)

                                if len(result_df) > 0:
                                    _display_query_visualization(result_df, description)

                                with st.expander("📝 View Generated SQL"):
                                    st.code(query, language="sql")

                                st.session_state.clarification_selections = {}
                        else:
                            st.warning(description)

                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    logger.error(f"Query error: {e}", exc_info=True)

        st.divider()

        # Part 2: Summary Metrics section
        st.subheader("Summary Metrics")

        if not sales_data.empty:
            col1, col2, col3, col4 = st.columns(4)

            total_revenue = sales_data['revenue'].sum()
            total_orders = sales_data['orders'].sum()
            avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
            avg_tip = sales_data['tips'].sum() / total_orders if total_orders > 0 else 0

            with col1:
                st.metric(
                    "Total Revenue",
                    format_currency(total_revenue),
                    delta=f"{((sales_data['revenue'].iloc[-7:].sum() / sales_data['revenue'].iloc[-14:-7].sum() - 1) * 100):.1f}% vs prev week" if len(sales_data) >= 14 else None
                )

            with col2:
                st.metric(
                    "Total Orders",
                    format_number(total_orders),
                    delta=f"{((sales_data['orders'].iloc[-7:].sum() / sales_data['orders'].iloc[-14:-7].sum() - 1) * 100):.1f}% vs prev week" if len(sales_data) >= 14 else None
                )

            with col3:
                st.metric("Avg Order Value", format_currency(avg_order_value))

            with col4:
                st.metric("Avg Tip", format_currency(avg_tip))

            # Revenue trend chart
            sales_data_overview = sales_data.copy()
            sales_data_overview['date'] = pd.to_datetime(sales_data_overview['date'].astype(str), format="%Y%m%d", errors="coerce")
            sales_data_overview = sales_data_overview.sort_values("date")

            st.subheader("Revenue Trend")
            fig_revenue = px.line(
                sales_data_overview,
                x='date',
                y='revenue',
                markers=True,
                title="Daily Revenue",
                labels={'revenue': 'Revenue ($)', 'date': 'Date'}
            )
            fig_revenue.update_layout(hovermode='x unified', height=400, xaxis_tickformat="%m/%d/%Y")
            fig_revenue.update_xaxes(type="date")
            fig_revenue.update_yaxes(tickprefix="$", tickformat=",.0f")
            fig_revenue.update_traces(line_color=BRAND_BURGUNDY, marker_color=BRAND_BURGUNDY)
            apply_plotly_theme(fig_revenue)

            st.plotly_chart(fig_revenue, width='stretch')

            # Order type distribution
            col1, col2 = st.columns(2)

            with col1:
                fig_orders = go.Figure(go.Pie(
                    labels=['Dine-in-like', 'API', 'Delivery', 'Takeout / Pickup', 'Other'],
                    values=[
                        sales_data_overview['dine_in_orders'].sum(),
                        sales_data_overview.get('api_orders', pd.Series(dtype=float)).sum(),
                        sales_data_overview['delivery_orders'].sum(),
                        sales_data_overview['takeout_orders'].sum(),
                        sales_data_overview.get('other_orders', pd.Series(dtype=float)).sum(),
                    ],
                    textinfo='percent+label',
                ))
                fig_orders.update_layout(title="Order Type Distribution", height=350)
                fig_orders.update_traces(
                    marker=dict(colors=[BRAND_BURGUNDY, BRAND_SLATE, BRAND_GOLD, STATUS_NEUTRAL, BRAND_MUTED])
                )
                apply_plotly_theme(fig_orders)
                st.plotly_chart(fig_orders, width='stretch')

            with col2:
                _render_alerts(sales_data_overview, inventory_data, labor_data)
                st.divider()
                _render_tip_breakdown(sales_data_overview)

    # TAB 2: SALES ANALYTICS
    with tab_sales:
        st.subheader("Sales Trends")

        sales_data_sales = sales_data.copy()
        if not pd.api.types.is_datetime64_any_dtype(sales_data_sales["date"]):
            sales_data_sales["date"] = pd.to_datetime(sales_data_sales["date"].astype(str), format="%Y%m%d", errors="coerce")

        sales_data_sales = sales_data_sales.dropna(subset=["date"]).sort_values("date")

        if not sales_data_sales.empty:
            # Revenue vs Orders comparison
            fig_comparison = go.Figure()
            fig_comparison.add_trace(go.Bar(
                x=sales_data_sales['date'],
                y=sales_data_sales['revenue'],
                name='Revenue ($)',
                marker_color=BRAND_BURGUNDY,
                yaxis='y'
            ))
            fig_comparison.add_trace(go.Scatter(
                x=sales_data_sales['date'],
                y=sales_data_sales['orders'],
                name='Orders',
                marker_color=BRAND_SLATE,
                line_color=BRAND_SLATE,
                yaxis='y2',
                mode='lines+markers'
            ))
            fig_comparison.update_layout(
                title="Revenue vs Orders",
                xaxis=dict(title='Date', type="date", tickformat="%m/%d/%Y"),
                yaxis=dict(
                    title='Revenue ($)',
                    title_font=dict(color=BRAND_BURGUNDY),
                    tickfont=dict(color=BRAND_BURGUNDY),
                    tickprefix="$",
                    tickformat=",.0f",
                ),
                yaxis2=dict(title='Orders', title_font=dict(color=BRAND_SLATE), tickfont=dict(color=BRAND_SLATE), overlaying='y', side='right'),
                hovermode='x unified',
                height=400
            )
            apply_plotly_theme(fig_comparison)
            st.plotly_chart(fig_comparison, width='stretch')

            # Average order value trend
            fig_aov = px.line(
                sales_data_sales,
                x='date',
                y='avg_order_value',
                markers=True,
                title="Average Order Value Trend",
                labels={'avg_order_value': 'AOV ($)', 'date': 'Date'}
            )
            fig_aov.update_layout(hovermode='x unified', height=400, xaxis_tickformat="%m/%d/%Y")
            fig_aov.update_xaxes(type="date")
            fig_aov.update_traces(line_color=BRAND_GOLD, marker_color=BRAND_GOLD)
            apply_plotly_theme(fig_aov)

            st.plotly_chart(fig_aov, width='stretch')

            # Tips and Discounts
            col1, col2 = st.columns(2)

            with col1:
                fig_tips = px.bar(
                    sales_data_sales,
                    x='date',
                    y='tips',
                    title="Daily Tips",
                    labels={'tips': 'Tips ($)', 'date': 'Date'}
                )
                fig_tips.update_layout(xaxis_tickformat="%m/%d/%Y")
                fig_tips.update_xaxes(type="date")
                fig_tips.update_yaxes(tickprefix="$", tickformat=",.0f")
                fig_tips.update_traces(marker_color=BRAND_GOLD)
                apply_plotly_theme(fig_tips)

                st.plotly_chart(fig_tips, width='stretch')

            with col2:
                fig_discounts = px.bar(
                    sales_data_sales,
                    x='date',
                    y='discounts',
                    title="Daily Discounts",
                    labels={'discounts': 'Discounts ($)', 'date': 'Date'}
                )
                fig_discounts.update_layout(xaxis_tickformat="%m/%d/%Y")
                fig_discounts.update_xaxes(type="date")
                fig_discounts.update_yaxes(tickprefix="$", tickformat=",.0f")
                fig_discounts.update_traces(marker_color=STATUS_BAD)
                apply_plotly_theme(fig_discounts)

                st.plotly_chart(fig_discounts, width='stretch')

            if not dow_data.empty:
                st.subheader("Day-of-Week Sales Pattern")
                dow_colors = [
                    BRAND_BURGUNDY if idx >= 1.0 else BRAND_SLATE
                    for idx in dow_data["index"]
                ]
                fig_dow = go.Figure(go.Bar(
                    x=dow_data["day"],
                    y=dow_data["index"],
                    marker_color=dow_colors,
                    text=dow_data["index"].map(lambda v: f"{v:.2f}x"),
                    textposition="outside",
                ))
                fig_dow.add_hline(
                    y=1.0,
                    line_dash="dash",
                    line_color=STATUS_NEUTRAL,
                    annotation_text="Average (1.0x)",
                    annotation_position="right",
                )
                fig_dow.update_layout(
                    title="Revenue Index by Day of Week",
                    xaxis_title="Day",
                    yaxis_title="Revenue Index (1.0 = avg day)",
                    height=380,
                    showlegend=False,
                )
                apply_plotly_theme(fig_dow)
                st.plotly_chart(fig_dow, width="stretch")
                st.caption(
                    "Index = average daily revenue for that day divided by overall daily average. "
                    "Burgundy bars are above-average days; slate bars are below."
                )

    # TAB 3: MENU PERFORMANCE
    with tab_menu:
        st.subheader("Top Performing Items")

        if not menu_data.empty:
            col1, col2 = st.columns([2, 1])

            with col1:
                # Revenue by menu item
                menu_sorted = menu_data.sort_values('revenue', ascending=True).tail(15)
                fig_menu = px.bar(
                    menu_sorted,
                    y='item',
                    x='revenue',
                    orientation='h',
                    title="Top 15 Items by Revenue",
                    labels={'revenue': 'Revenue ($)', 'item': 'Item'}
                )
                fig_menu.update_xaxes(tickprefix="$", tickformat=",.0f")
                fig_menu.update_traces(marker_color=BRAND_BURGUNDY)
                apply_plotly_theme(fig_menu)
                st.plotly_chart(fig_menu, width='stretch')

            with col2:
                # Order count by item
                menu_by_orders = menu_data.sort_values('order_count', ascending=True).tail(15)
                fig_orders = px.bar(
                    menu_by_orders,
                    y='item',
                    x='order_count',
                    orientation='h',
                    title="Top 15 Items by Order Count",
                    labels={'order_count': 'Orders', 'item': 'Item'},
                    color_discrete_sequence=[BRAND_SLATE],
                )
                apply_plotly_theme(fig_orders)
                st.plotly_chart(fig_orders, width='stretch')

            # Menu items table
            st.subheader("All Menu Items")
            display_menu = menu_data.copy()
            display_menu = display_menu.sort_values('revenue', ascending=False)
            st.dataframe(
                display_menu,
                width='stretch',
                hide_index=True,
                column_config={
                    "revenue": st.column_config.NumberColumn(
                        "Revenue",
                        format="$%.2f"
                    ),
                    "avg_price": st.column_config.NumberColumn(
                        "Avg Price",
                        format="$%.2f"
                    ),
                    "order_count": st.column_config.NumberColumn(
                        "Order Count",
                        format="%d"
                    )
                }
            )
        else:
            st.info("No menu data available for selected date range")

    # TAB 4: REVENUE DRIVERS
    # NOTE: Inventory stub hidden here — Toast API returns no real stock levels (all zeros).
    # Re-enable by restoring inventory_data load (db.get_inventory_status) and the block below
    # once real inventory data is available.
    #
    # --- INVENTORY STUB (hidden) ---
    # with tab_inventory:
    #     st.subheader(f"Inventory Status as of {end_date}")
    #     if not inventory_data.empty:
    #         col1, col2, col3 = st.columns(3)
    #         good_count = len(inventory_data[inventory_data['status'] == 'good'])
    #         low_count = len(inventory_data[inventory_data['status'] == 'low'])
    #         critical_count = len(inventory_data[inventory_data['status'] == 'critical'])
    #         with col1: st.metric("✅ Good", good_count)
    #         with col2: st.metric("⚠️ Low", low_count)
    #         with col3: st.metric("🚨 Critical", critical_count)
    #         st.subheader("Inventory Items")
    #         display_inventory = inventory_data.copy().sort_values('stock', ascending=True)
    #         st.dataframe(display_inventory, width='stretch', hide_index=True,
    #             column_config={"item":"Item","category":"Category",
    #                 "stock": st.column_config.NumberColumn("Stock", format="%.1f"),
    #                 "reorder_level": st.column_config.NumberColumn("Reorder Level", format="%.1f"),
    #                 "unit_cost": st.column_config.NumberColumn("Unit Cost", format="$%.2f"),
    #                 "last_ordered": "Last Ordered", "status": "Status"})
    #         if critical_count > 0 or low_count > 0:
    #             st.warning(f"⚠️ {critical_count + low_count} items need attention!")
    #             needs_attention = inventory_data[inventory_data['status'].isin(['critical', 'low'])]
    #             st.dataframe(needs_attention[['item','stock','reorder_level','status']], width='stretch', hide_index=True)
    #     else:
    #         st.info("No inventory data available for selected date")
    # --- END INVENTORY STUB ---

    with tab_menu_opt:
        st.subheader("Menu Optimization")
        st.caption(
            "Objective 5 recommendations from synthetic item sales, discount exposure, inventory cost proxies, "
            "and basket co-occurrence. Discount exposure uses order-level discounts as a promotion proxy."
        )

        if menu_recommendations.empty:
            st.info("No menu optimization data available for the selected date range.")
        else:
            action_order = ["Promote", "Bundle", "Re-price", "Rework", "Remove"]
            action_counts = menu_recommendations["recommended_action"].value_counts()
            metric_cols = st.columns(len(action_order))
            for col, action in zip(metric_cols, action_order):
                col.metric(action, format_number(action_counts.get(action, 0)))

            st.divider()

            rec_plot = menu_recommendations.copy()
            rec_plot["margin_axis"] = rec_plot["avg_est_margin_per_unit"].fillna(
                rec_plot["avg_net_rev_per_unit"]
            )
            rec_plot["margin_label"] = rec_plot["margin_source"].fillna("Revenue/unit proxy")
            rec_plot = rec_plot.dropna(subset=["popularity", "margin_axis", "net_revenue"])

            if not rec_plot.empty:
                quadrant_colors = {
                    "Star": BRAND_BURGUNDY,
                    "Plowhorse": BRAND_GOLD,
                    "Puzzle": BRAND_SLATE,
                    "Dog": STATUS_BAD,
                }
                fig_opt = px.scatter(
                    rec_plot,
                    x="popularity",
                    y="margin_axis",
                    size="net_revenue",
                    color="quadrant",
                    color_discrete_map=quadrant_colors,
                    hover_name="display_name",
                    hover_data={
                        "recommended_action": True,
                        "confidence": True,
                        "net_revenue": ":$,.0f",
                        "orders_with_item": ":,",
                        "margin_label": True,
                        "popularity": ":.1%",
                        "margin_axis": ":$.2f",
                    },
                    title="Menu Engineering Matrix",
                    labels={
                        "popularity": "Popularity (% orders with item)",
                        "margin_axis": "Estimated Margin / Unit or Revenue Proxy",
                    },
                )
                fig_opt.update_layout(height=500)
                fig_opt.update_xaxes(tickformat=".1%")
                fig_opt.update_yaxes(tickprefix="$", tickformat=",.2f")
                apply_plotly_theme(fig_opt)
                st.plotly_chart(fig_opt, width="stretch")

            st.subheader("Action Recommendations")
            selected_actions = st.multiselect(
                "Filter actions",
                options=action_order,
                default=action_order,
                key="obj5_action_filter",
            )
            rec_display = menu_recommendations[
                menu_recommendations["recommended_action"].isin(selected_actions)
            ].copy()
            rec_display = rec_display.sort_values(["recommended_action", "net_revenue"], ascending=[True, False])
            rec_display["popularity_pct"] = rec_display["popularity"] * 100
            rec_display["discount_rate_pct"] = rec_display["discount_rate"] * 100
            rec_display["cost_coverage_pct"] = rec_display["cost_coverage"] * 100
            action_recommendation_display = rec_display[
                [
                    "display_name", "recommended_action", "category", "quadrant", "confidence",
                    "net_revenue", "orders_with_item", "popularity_pct", "avg_unit_price",
                    "avg_est_margin_per_unit", "avg_net_rev_per_unit", "discount_rate_pct",
                    "cost_coverage_pct", "margin_source",
                ]
            ]
            styled_action_recommendations = action_recommendation_display.style.map(
                action_badge_style,
                subset=["recommended_action"],
            )
            st.dataframe(
                styled_action_recommendations,
                hide_index=True,
                width="stretch",
                column_config={
                    "display_name": "Item",
                    "category": "Category",
                    "quadrant": "Quadrant",
                    "recommended_action": "Action",
                    "confidence": "Confidence",
                    "net_revenue": st.column_config.NumberColumn("Net Revenue", format="$%.2f"),
                    "orders_with_item": st.column_config.NumberColumn("Orders With Item", format="%d"),
                    "popularity_pct": st.column_config.NumberColumn("Popularity", format="%.2f%%"),
                    "avg_unit_price": st.column_config.NumberColumn("Avg Price", format="$%.2f"),
                    "avg_est_margin_per_unit": st.column_config.NumberColumn("Est Margin / Unit", format="$%.2f"),
                    "avg_net_rev_per_unit": st.column_config.NumberColumn("Net Rev / Unit", format="$%.2f"),
                    "discount_rate_pct": st.column_config.NumberColumn("Discount Rate", format="%.2f%%"),
                    "cost_coverage_pct": st.column_config.NumberColumn("Cost Coverage", format="%.2f%%"),
                    "margin_source": "Margin Source",
                },
            )

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Bundle Opportunities")
                if bundle_data.empty:
                    st.info("No bundle pairs met the minimum threshold of 50 orders and 1.2 lift.")
                else:
                    bundle_display = bundle_data.head(25).copy()
                    bundle_display["support_pct"] = bundle_display["support"] * 100
                    bundle_display["confidence_b_given_a_pct"] = bundle_display["confidence_b_given_a"] * 100
                    bundle_display["confidence_a_given_b_pct"] = bundle_display["confidence_a_given_b"] * 100
                    st.dataframe(
                        bundle_display[
                            [
                                "display_a", "display_b", "pair_type", "pair_count", "support_pct",
                                "confidence_b_given_a_pct", "confidence_a_given_b_pct", "lift",
                            ]
                        ],
                        hide_index=True,
                        width="stretch",
                        column_config={
                            "display_a": "Item A",
                            "display_b": "Item B",
                            "pair_type": "Pair Type",
                            "pair_count": st.column_config.NumberColumn("Pair Orders", format="%d"),
                            "support_pct": st.column_config.NumberColumn("Support", format="%.2f%%"),
                            "confidence_b_given_a_pct": st.column_config.NumberColumn("Conf B|A", format="%.2f%%"),
                            "confidence_a_given_b_pct": st.column_config.NumberColumn("Conf A|B", format="%.2f%%"),
                            "lift": st.column_config.NumberColumn("Lift", format="%.2f"),
                        },
                    )

            with col2:
                st.subheader("Promo & Price Signals")
                if promo_data.empty:
                    st.info("No promo or price candidates met the minimum data threshold.")
                else:
                    promo_display = promo_data.head(25).copy()
                    st.dataframe(
                        promo_display[
                            [
                                "display_name", "category", "opportunity_type", "store_days",
                                "discount_days", "qty_lift_on_discount_days", "unique_prices",
                                "price_range",
                            ]
                        ],
                        hide_index=True,
                        width="stretch",
                        column_config={
                            "display_name": "Item",
                            "category": "Category",
                            "opportunity_type": "Opportunity",
                            "store_days": st.column_config.NumberColumn("Store-Days", format="%d"),
                            "discount_days": st.column_config.NumberColumn("Discount Days", format="%d"),
                            "qty_lift_on_discount_days": st.column_config.NumberColumn("Qty Lift", format="%.1f"),
                            "unique_prices": st.column_config.NumberColumn("Unique Prices", format="%d"),
                            "price_range": st.column_config.NumberColumn("Price Range", format="$%.2f"),
                        },
                    )

            if not price_margin_data.empty:
                with st.expander("Margin and removal candidates"):
                    price_margin_display = price_margin_data.copy()
                    price_margin_display["est_margin_pct_display"] = price_margin_display["est_margin_pct"] * 100
                    price_margin_display["cost_coverage_pct"] = price_margin_display["cost_coverage"] * 100
                    st.dataframe(
                        price_margin_display[
                            [
                                "display_name", "category", "quadrant", "recommended_action", "confidence",
                                "net_revenue", "avg_est_margin_per_unit", "est_margin_pct_display",
                                "cost_coverage_pct",
                            ]
                        ],
                        hide_index=True,
                        width="stretch",
                        column_config={
                            "display_name": "Item",
                            "category": "Category",
                            "quadrant": "Quadrant",
                            "recommended_action": "Action",
                            "confidence": "Confidence",
                            "net_revenue": st.column_config.NumberColumn("Net Revenue", format="$%.2f"),
                            "avg_est_margin_per_unit": st.column_config.NumberColumn("Est Margin / Unit", format="$%.2f"),
                            "est_margin_pct_display": st.column_config.NumberColumn("Est Margin %", format="%.2f%%"),
                            "cost_coverage_pct": st.column_config.NumberColumn("Cost Coverage", format="%.2f%%"),
                        },
                    )

            st.caption(
                "Margin estimates use synthetic inventory unit cost when available; otherwise the matrix falls back to "
                "net revenue per unit as a profit proxy. Promo signals are directional because explicit campaign "
                "tags are not available."
            )

    with tab_drivers:
        st.subheader("Revenue Drivers")
        st.caption(
            "OLS regression (HC3 robust SEs) on daily net revenue. "
            "All features are standardized (mean=0, std=1) so coefficients are directly comparable. "
            "Uses all locations across the selected date range."
        )

        _FEATURE_LABELS = {
            "order_count":     "Order Count",
            "avg_order_value": "Avg Order Value",
            "dine_in_mix":     "Dine-In Mix",
            "delivery_mix":    "Delivery Mix",
            "discount_rate":   "Discount Rate",
            "tip_rate":        "Tip Rate",
            "is_weekend":      "Weekend",
            "store_b":         "Store B (vs A)",
        }

        if drivers_data.empty or len(drivers_data) < 20:
            st.info("Not enough data for driver analysis in this date range (minimum 20 store-days).")
        else:
            feat = drivers_data.copy()
            feat["delivery_mix"]  = feat["delivery_orders"]  / feat["order_count"].replace(0, np.nan)
            feat["dine_in_mix"]   = feat["dine_in_orders"]   / feat["order_count"].replace(0, np.nan)
            feat["discount_rate"] = feat["total_discounts"]  / feat["gross_revenue"].replace(0, np.nan)
            feat["tip_rate"]      = feat["total_tips"]        / feat["gross_revenue"].replace(0, np.nan)
            feat["is_weekend"]    = pd.to_datetime(feat["business_date"]).dt.dayofweek.isin([5, 6]).astype(int)

            unique_locs = feat["location_id"].dropna().unique()
            features = ["order_count", "avg_order_value", "discount_rate",
                        "dine_in_mix", "delivery_mix", "tip_rate", "is_weekend"]
            if len(unique_locs) > 1:
                loc_b = sorted(unique_locs)[1]
                feat["store_b"] = (feat["location_id"] == loc_b).astype(int)
                features = features + ["store_b"]

            feat = feat.dropna(subset=features + ["net_revenue"])

            if len(feat) < 20:
                st.info("Not enough complete rows after dropping NaNs.")
            else:
                scaler = StandardScaler()
                X_scaled = pd.DataFrame(
                    scaler.fit_transform(feat[features]),
                    columns=features,
                    index=feat.index,
                )
                X_ols = sm.add_constant(X_scaled)
                model = sm.OLS(feat["net_revenue"].values, X_ols.values).fit(cov_type="HC3")

                ci = model.conf_int()
                coef_df = pd.DataFrame({
                    "feature": features,
                    "beta":    model.params[1:],
                    "lower":   ci[1:, 0],
                    "upper":   ci[1:, 1],
                    "pvalue":  model.pvalues[1:],
                }).sort_values("beta").reset_index(drop=True)

                def _stars(p):
                    if p < 0.001: return "***"
                    if p < 0.01:  return "**"
                    if p < 0.05:  return "*"
                    return "ns"

                coef_df["sig"] = coef_df["pvalue"].apply(_stars)
                coef_df["label"] = coef_df["feature"].map(_FEATURE_LABELS)
                top_driver = coef_df.loc[coef_df["beta"].abs().idxmax(), "label"]

                col1, col2, col3 = st.columns(3)
                col1.metric("R² (in-sample)", f"{model.rsquared:.3f}")
                col2.metric("Store-days", len(feat))
                col3.metric("Strongest driver", top_driver)

                fig = go.Figure(go.Bar(
                    x=coef_df["beta"],
                    y=coef_df["label"],
                    orientation="h",
                    marker_color=[DRIVER_POSITIVE if b > 0 else DRIVER_NEGATIVE for b in coef_df["beta"]],
                    text=[f"β={b:.3f} {s}" for b, s in zip(coef_df["beta"], coef_df["sig"])],
                    textposition="outside",
                ))
                fig.update_layout(
                    title=f"Standardized Revenue Drivers  (R²={model.rsquared:.3f}, n={len(feat)})",
                    xaxis_title="Standardized β  (effect on daily net revenue)",
                    yaxis_title="",
                    height=420,
                    margin=dict(l=10, r=120, t=50, b=40),
                )
                apply_plotly_theme(fig)
                st.plotly_chart(fig, width="stretch")

                with st.expander("Full coefficient table"):
                    display_coef = coef_df[["label", "beta", "lower", "upper", "pvalue", "sig"]].copy()
                    display_coef.columns = ["Feature", "β", "CI lower", "CI upper", "p-value", "Sig"]
                    st.dataframe(
                        display_coef,
                        hide_index=True,
                        column_config={
                            "β":        st.column_config.NumberColumn(format="%.4f"),
                            "CI lower": st.column_config.NumberColumn(format="%.4f"),
                            "CI upper": st.column_config.NumberColumn(format="%.4f"),
                            "p-value":  st.column_config.NumberColumn(format="%.4f"),
                        },
                    )

    # TAB 5: CUSTOMER ANALYTICS
    with tab_customers:
        st.subheader("Customer Analytics")
        st.caption(
            "PII-masked data — email/phone hashed to a stable pseudonymous customer_id via SHA256. "
            "Coverage is limited to orders where guests shared contact info (online, delivery, loyalty)."
        )

        CUSTOMER_MIN_N = 10  # suppress analytics when fewer customers than this threshold
        if customer_data.empty:
            st.info("No customer data available for the selected date range.")
        elif len(customer_data) < CUSTOMER_MIN_N:
            st.info(
                f"Insufficient data — fewer than {CUSTOMER_MIN_N} identified customers in this period. "
                "Widen the date range to see customer analytics."
            )
        else:
            total_custs = len(customer_data)
            repeat_custs = int((customer_data["visit_days"] > 1).sum())
            repeat_rate = repeat_custs / total_custs if total_custs > 0 else 0
            rev_repeat = customer_data.loc[customer_data["visit_days"] > 1, "total_spend"].sum()
            rev_one_time = customer_data.loc[customer_data["visit_days"] <= 1, "total_spend"].sum()

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Identified Customers", format_number(total_custs))
            col2.metric("Repeat Rate", f"{repeat_rate * 100:.1f}%")
            col3.metric("Avg Orders / Customer", f"{customer_data['order_count'].mean():.1f}")
            col4.metric("Avg Spend / Customer", format_currency(customer_data["total_spend"].mean()))

            st.divider()

            col1, col2 = st.columns(2)

            with col1:
                visit_days = customer_data["visit_days"].fillna(0).astype(int)
                visit_bins = [
                    (1, 1, "1 day"),
                    (2, 2, "2 days"),
                    (3, 4, "3-4 days"),
                    (5, 7, "5-7 days"),
                    (8, 14, "8-14 days"),
                    (15, np.inf, "15+ days"),
                ]
                visit_hist = pd.DataFrame([
                    {
                        "visit_days": label,
                        "customers": int(((visit_days >= lower) & (visit_days <= upper)).sum()),
                        "sort_order": sort_order,
                    }
                    for sort_order, (lower, upper, label) in enumerate(visit_bins)
                ])
                fig_visits = px.bar(
                    visit_hist,
                    x="visit_days",
                    y="customers",
                    title="Visit Frequency Distribution (binned)",
                    labels={"visit_days": "Distinct Visit Days", "customers": "Customers"},
                    text="customers",
                )
                fig_visits.update_layout(height=350, bargap=0.2)
                fig_visits.update_traces(
                    marker_color=BRAND_BURGUNDY,
                    hovertemplate="Visit days: %{x}<br>Customers: %{y:,}<extra></extra>",
                    texttemplate="%{text:,}",
                    textposition="outside",
                    cliponaxis=False,
                )
                fig_visits.update_xaxes(
                    type="category",
                    categoryorder="array",
                    categoryarray=visit_hist.sort_values("sort_order")["visit_days"].tolist(),
                )
                fig_visits.update_yaxes(rangemode="tozero")
                apply_plotly_theme(fig_visits)
                st.plotly_chart(fig_visits, width="stretch")

            with col2:
                rev_df = pd.DataFrame({
                    "Segment": ["Repeat (>1 visit)", "One-Time"],
                    "Revenue": [rev_repeat, rev_one_time],
                })
                fig_rev = px.pie(
                    rev_df,
                    values="Revenue",
                    names="Segment",
                    title="Revenue Share: Repeat vs One-Time",
                    color_discrete_sequence=[BRAND_BURGUNDY, BRAND_SLATE],
                )
                fig_rev.update_traces(textinfo="percent+label")
                fig_rev.update_layout(height=350)
                apply_plotly_theme(fig_rev)
                st.plotly_chart(fig_rev, width="stretch")

            if not rfm_data.empty:
                st.subheader("Customer Segments")
                rfm_sorted = rfm_data.sort_values("avg_total_spend", ascending=True)
                fig_rfm = go.Figure(go.Bar(
                    x=rfm_sorted["pct"],
                    y=rfm_sorted["segment"],
                    orientation="h",
                    marker=dict(
                        color=rfm_sorted["avg_total_spend"],
                        colorscale=[[0.0, BRAND_GOLD], [0.5, BRAND_BURGUNDY], [1.0, BRAND_CHARCOAL]],
                        showscale=True,
                        colorbar=dict(title="Avg Spend ($)"),
                    ),
                    text=rfm_sorted.apply(
                        lambda r: f"{r['pct']:.1f}%  -  ${r['avg_total_spend']:.0f} avg", axis=1
                    ),
                    textposition="outside",
                    hovertemplate=(
                        "<b>%{y}</b><br>"
                        "Share: %{x:.1f}%<br>"
                        "Avg spend: $%{marker.color:.2f}<br>"
                        "Avg visits: %{customdata[0]:.1f}<br>"
                        "Avg recency: %{customdata[1]:.1f} days<extra></extra>"
                    ),
                    customdata=rfm_sorted[["avg_visits", "avg_recency_days"]].values,
                ))
                fig_rfm.update_layout(
                    xaxis_title="% of Customers",
                    yaxis_title="",
                    height=max(300, len(rfm_sorted) * 45),
                    margin=dict(l=160),
                )
                apply_plotly_theme(fig_rfm)
                st.plotly_chart(fig_rfm, width="stretch")
                st.caption(
                    "Segments ranked by avg spend. Color = average spend per customer. "
                    "Recency = days since last visit."
                )

            p95 = customer_data["total_spend"].quantile(0.95)
            fig_spend = px.histogram(
                customer_data["total_spend"].clip(upper=p95),
                nbins=30,
                title=f"Customer Total Spend Distribution (capped at 95th percentile: ${p95:,.0f})",
                color_discrete_sequence=[BRAND_BURGUNDY],
            )
            fig_spend.update_layout(
                showlegend=False,
                xaxis_title="Total Spend ($)",
                yaxis_title="Customers",
                height=350,
            )
            fig_spend.update_xaxes(tickprefix="$", tickformat=",.0f")
            apply_plotly_theme(fig_spend)
            st.plotly_chart(fig_spend, width="stretch")

            total_orders_in_period = customer_data["order_count"].sum()
            st.caption(
                f"Coverage note: {total_custs:,} customers identified across "
                f"{total_orders_in_period:,.0f} orders in this period. "
                "Dine-in and in-store orders typically have no customer data."
            )

    with tab_engagement:
        render_engagement_analysis_tab(
            db=db,
            selected_locations=selected_locations,
            location_map=location_map,
            start_date=start_date,
            end_date=end_date,
            allow_bertopic=True,
        )

    # TAB 7: LABOR ANALYTICS
    # NOTE: Labor Analytics hidden here — labor data access not yet available.
    # Re-enable by restoring labor_data load (db.get_labor_analytics), adding tab_labor
    # back to st.tabs(), and uncommenting the block below once labor data is available.
    #
    # --- LABOR ANALYTICS STUB (hidden) ---
    # with tab_labor:
    #     st.subheader("Labor Analytics")
    #
    #     if not labor_data.empty:
    #         labor_display = labor_data.copy()
    #         if not pd.api.types.is_datetime64_any_dtype(labor_display["date"]):
    #             labor_display["date"] = pd.to_datetime(
    #                 labor_display["date"].astype(str), format="%Y%m%d", errors="coerce"
    #             )
    #         labor_display = labor_display.dropna(subset=["date"]).sort_values("date")
    #
    #         if not labor_display.empty:
    #             # Summary metrics
    #             total_hours = labor_display['payable_hours'].sum()
    #             total_employees = labor_display['employee_name'].nunique()
    #             total_tips = labor_display['total_tips'].sum()
    #             total_overtime = labor_display['overtime_hours'].sum()
    #
    #             col1, col2, col3, col4 = st.columns(4)
    #             with col1:
    #                 st.metric("Total Payable Hours", f"{total_hours:,.1f}")
    #             with col2:
    #                 st.metric("Unique Employees", int(total_employees))
    #             with col3:
    #                 st.metric("Total Tips", format_currency(total_tips))
    #             with col4:
    #                 st.metric("Total Overtime Hours", f"{total_overtime:,.1f}")
    #
    #             st.divider()
    #
    #             # Row 1: Daily hours trend + Daily tips
    #             col1, col2 = st.columns(2)
    #
    #             with col1:
    #                 daily_hours = labor_display.groupby('date').agg(
    #                     payable_hours=('payable_hours', 'sum')
    #                 ).reset_index()
    #                 fig_hours = px.line(
    #                     daily_hours, x='date', y='payable_hours', markers=True,
    #                     title="Daily Payable Hours",
    #                     labels={'payable_hours': 'Hours', 'date': 'Date'}
    #                 )
    #                 fig_hours.update_layout(hovermode='x unified', height=400, xaxis_tickformat="%m/%d/%Y")
    #                 fig_hours.update_xaxes(type="date")
    #                 st.plotly_chart(fig_hours, width="stretch")
    #
    #             with col2:
    #                 daily_tips = labor_display.groupby('date').agg(
    #                     total_tips=('total_tips', 'sum')
    #                 ).reset_index()
    #                 fig_tips = px.bar(
    #                     daily_tips, x='date', y='total_tips',
    #                     title="Daily Tips",
    #                     labels={'total_tips': 'Tips ($)', 'date': 'Date'}
    #                 )
    #                 fig_tips.update_layout(height=400, xaxis_tickformat="%m/%d/%Y")
    #                 fig_tips.update_xaxes(type="date")
    #                 st.plotly_chart(fig_tips, width="stretch")
    #
    #             # Row 2: Top employees by hours + Top employees by tips
    #             col1, col2 = st.columns(2)
    #
    #             with col1:
    #                 emp_hours = labor_display.groupby('employee_name').agg(
    #                     payable_hours=('payable_hours', 'sum')
    #                 ).reset_index().sort_values('payable_hours', ascending=True).tail(10)
    #                 fig_emp_hours = px.bar(
    #                     emp_hours, y='employee_name', x='payable_hours', orientation='h',
    #                     title="Top Employees by Hours",
    #                     labels={'payable_hours': 'Hours', 'employee_name': 'Employee'}
    #                 )
    #                 fig_emp_hours.update_layout(height=400)
    #                 st.plotly_chart(fig_emp_hours, width="stretch")
    #
    #             with col2:
    #                 emp_tips = labor_display.groupby('employee_name').agg(
    #                     total_tips=('total_tips', 'sum')
    #                 ).reset_index().sort_values('total_tips', ascending=True).tail(10)
    #                 fig_emp_tips = px.bar(
    #                     emp_tips, y='employee_name', x='total_tips', orientation='h',
    #                     title="Top Employees by Tips",
    #                     labels={'total_tips': 'Tips ($)', 'employee_name': 'Employee'}
    #                 )
    #                 fig_emp_tips.update_layout(height=400)
    #                 st.plotly_chart(fig_emp_tips, width="stretch")
    #
    #             # Row 3: Overtime breakdown
    #             daily_ot = labor_display.groupby('date').agg(
    #                 regular_hours=('regular_hours', 'sum'),
    #                 overtime_hours=('overtime_hours', 'sum')
    #             ).reset_index()
    #             fig_ot = px.bar(
    #                 daily_ot, x='date', y=['regular_hours', 'overtime_hours'],
    #                 title="Regular vs Overtime Hours",
    #                 labels={'value': 'Hours', 'date': 'Date', 'variable': 'Type'},
    #                 barmode='stack'
    #             )
    #             fig_ot.update_layout(height=400, xaxis_tickformat="%m/%d/%Y")
    #             fig_ot.update_xaxes(type="date")
    #             st.plotly_chart(fig_ot, width="stretch")
    #
    #             # Detail table
    #             st.subheader("Employee Time Entries")
    #             table_data = labor_display.sort_values(['date', 'employee_name'], ascending=[False, True])
    #             st.dataframe(
    #                 table_data,
    #                 width="stretch",
    #                 hide_index=True,
    #                 column_config={
    #                     "date": st.column_config.DateColumn("Date", format="MM/DD/YYYY"),
    #                     "employee_name": "Employee",
    #                     "job_title": "Job Title",
    #                     "clock_in_time": "Clock In",
    #                     "clock_out_time": "Clock Out",
    #                     "total_hours": st.column_config.NumberColumn("Total Hours", format="%.2f"),
    #                     "payable_hours": st.column_config.NumberColumn("Payable Hours", format="%.2f"),
    #                     "regular_hours": st.column_config.NumberColumn("Regular Hours", format="%.2f"),
    #                     "overtime_hours": st.column_config.NumberColumn("Overtime Hours", format="%.2f"),
    #                     "total_tips": st.column_config.NumberColumn("Total Tips", format="$%.2f"),
    #                     "cash_tips": st.column_config.NumberColumn("Cash Tips", format="$%.2f"),
    #                     "non_cash_tips": st.column_config.NumberColumn("Non-Cash Tips", format="$%.2f"),
    #                     "total_gratuity": st.column_config.NumberColumn("Gratuity", format="$%.2f"),
    #                     "wage": st.column_config.NumberColumn("Wage", format="$%.2f"),
    #                 }
    #             )
    #         else:
    #             st.info("No labor data available for selected date range")
    #     else:
    #         st.info("No labor data available for selected date range")

    # Footer
    st.divider()
    footer_source = "Synthetic demo dataset" if _demo_mode else "Cloud warehouse"
    st.markdown("""
        <div style='text-align: center; color: """ + BRAND_MUTED + """; font-size: 12px;'>
            Data Source: """ + footer_source + """ | Last Updated: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """
        </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
