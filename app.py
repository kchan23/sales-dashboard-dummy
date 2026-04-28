"""
Restaurant Analytics Demo - Streamlit application.
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


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from query.llm_generator import AmbiguityResult

# Page configuration
st.set_page_config(
    page_title="Restaurant Analytics Demo",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #ff6b35;
    }
    .metric-value {
        font-size: 28px;
        font-weight: bold;
        color: #262730;
    }
    .metric-label {
        font-size: 12px;
        color: #808080;
        margin-bottom: 10px;
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
    }
    .stTabs [data-baseweb="tab"] p,
    .stTabs button[data-baseweb="tab"] p {
        font-size: 1.15rem !important;
        font-weight: 500 !important;
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


def format_currency(value):
    """Format value as currency."""
    return f"${value:,.2f}" if value else "$0.00"


def format_number(value):
    """Format value as number."""
    return f"{value:,.0f}" if value else "0"


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
            st.plotly_chart(fig, width='stretch')


def _render_alerts(sales_data: pd.DataFrame, inventory_data: pd.DataFrame, labor_data: pd.DataFrame):
    """Render actionable alerts from already-loaded DataFrames, capped at 5 per category."""
    OT_THRESHOLD = 8  # total overtime hours across all staff in a single day
    MAX_VISIBLE = 5

    def _fmt_date(date):
        d = pd.to_datetime(str(date), format="%Y%m%d", errors="coerce")
        return d.strftime("%m/%d") if not pd.isna(d) else str(date)

    st.subheader("⚠️ Alerts")
    any_alerts = False

    # --- Inventory alerts ---
    if not inventory_data.empty:
        critical_items = inventory_data[inventory_data['status'] == 'critical']['item'].tolist()
        low_items = inventory_data[inventory_data['status'] == 'low']['item'].tolist()

        if critical_items:
            any_alerts = True
            visible, hidden = critical_items[:MAX_VISIBLE], critical_items[MAX_VISIBLE:]
            st.error(f"🚨 Inventory critical: {', '.join(visible)}" + (f" (+{len(hidden)} more)" if hidden else ""))
            if hidden:
                with st.expander(f"Show all {len(critical_items)} critical items"):
                    for item in hidden:
                        st.write(f"• {item}")
            st.caption("See 📦 Inventory tab for details")

        if low_items:
            any_alerts = True
            visible, hidden = low_items[:MAX_VISIBLE], low_items[MAX_VISIBLE:]
            st.warning(f"⚠️ Inventory low: {', '.join(visible)}" + (f" (+{len(hidden)} more)" if hidden else ""))
            if hidden:
                with st.expander(f"Show all {len(low_items)} low-stock items"):
                    for item in hidden:
                        st.write(f"• {item}")
            st.caption("See 📦 Inventory tab for details")

    # --- Overtime alerts ---
    if not labor_data.empty and 'overtime_hours' in labor_data.columns:
        daily_ot = labor_data.groupby('date')['overtime_hours'].sum().sort_values(ascending=False)
        flagged = daily_ot[daily_ot > OT_THRESHOLD]
        if not flagged.empty:
            any_alerts = True
            visible_ot, hidden_ot = flagged.iloc[:MAX_VISIBLE], flagged.iloc[MAX_VISIBLE:]
            for date, hrs in visible_ot.items():
                st.warning(f"⏱️ Overtime spike on {_fmt_date(date)}: {hrs:.1f} hrs total")
            if not hidden_ot.empty:
                with st.expander(f"Show {len(hidden_ot)} more overtime days"):
                    for date, hrs in hidden_ot.items():
                        st.write(f"• {_fmt_date(date)}: {hrs:.1f} hrs")
            st.caption("See 👥 Labor Analytics tab for details")

    # --- Tip rate alerts ---
    if not sales_data.empty and len(sales_data) >= 3 and 'tips' in sales_data.columns:
        revenue = sales_data['revenue'].replace(0, pd.NA)
        tip_rate = sales_data['tips'] / revenue
        mean_rate = tip_rate.mean()
        std_rate = tip_rate.std()
        if std_rate and std_rate > 0:
            flagged = sales_data[tip_rate < (mean_rate - std_rate)].copy()
            flagged['_tip_rate'] = tip_rate[flagged.index]
            flagged = flagged.sort_values('_tip_rate')  # worst first
            if not flagged.empty:
                any_alerts = True
                visible_tips, hidden_tips = flagged.iloc[:MAX_VISIBLE], flagged.iloc[MAX_VISIBLE:]
                for _, row in visible_tips.iterrows():
                    label = _fmt_date(row['date'])
                    actual = (row['tips'] / row['revenue'] * 100) if row['revenue'] else 0
                    st.warning(f"📉 Low tip rate on {label}: {actual:.1f}% (avg {mean_rate*100:.1f}%)")
                if not hidden_tips.empty:
                    with st.expander(f"Show {len(hidden_tips)} more tip-rate days"):
                        for _, row in hidden_tips.iterrows():
                            actual = (row['tips'] / row['revenue'] * 100) if row['revenue'] else 0
                            st.write(f"• {_fmt_date(row['date'])}: {actual:.1f}%")
                st.caption("See 💰 Sales Analytics tab for details")

    if not any_alerts:
        st.success("No alerts for this period")


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


def main():
    # Header
    st.title("Restaurant Analytics Demo")

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
    except Exception as e:
        st.error(f"Error loading data: {e}")
        logger.error(f"Data load error: {e}", exc_info=True)
        return

    # Horizontal tab navigation (replaces sidebar buttons)
    tab_overview, tab_sales, tab_menu, tab_inventory, tab_customers = st.tabs([
        "📈 Overview",
        "💰 Sales Analytics",
        "🍔 Menu Performance",
        "📊 Revenue Drivers",
        "👤 Customer Analytics",
    ])

    # TAB 1: OVERVIEW (with LLM Ask feature at top)
    with tab_overview:
        # Part 1: LLM "Ask a Question" section
        st.subheader("Data Exploration Q&A Tool")
        if _demo_mode:
            st.info("The Q&A tool is disabled in the public demo build.")
        else:
            st.markdown("Ask your question about the data in plain English, and we'll generate the SQL query to answer it.")

        user_question = ""
        ask_triggered = False
        if not _demo_mode:
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
                            from query.llm_generator import LLMQueryGenerator
                            from query.validator import SQLValidator

                            api_key = st.secrets.get("OPENROUTER_API_KEY") if "OPENROUTER_API_KEY" in st.secrets else os.getenv("OPENROUTER_API_KEY")

                            if not api_key:
                                st.error("⚠️ OPENROUTER_API_KEY not configured.")
                                st.stop()

                            query_gen = LLMQueryGenerator(db, api_key)
                            validator = SQLValidator(getattr(db, "client", None))

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
                    from query.llm_generator import LLMQueryGenerator
                    from query.validator import SQLValidator

                    api_key = st.secrets.get("OPENROUTER_API_KEY") if "OPENROUTER_API_KEY" in st.secrets else os.getenv("OPENROUTER_API_KEY")

                    if not api_key:
                        st.error("⚠️ OPENROUTER_API_KEY not configured. Please add it to .env file or Streamlit secrets.")
                        st.info("Get your API key from: https://openrouter.ai/keys")
                        st.stop()

                    query_gen = LLMQueryGenerator(db, api_key)
                    validator = SQLValidator(getattr(db, "client", None))

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
        st.subheader("📊 Summary Metrics")

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

            st.plotly_chart(fig_revenue, width='stretch')

            # Order type distribution
            col1, col2 = st.columns(2)

            with col1:
                fig_orders = go.Figure(go.Pie(
                    labels=['Takeout / In-Store', 'Delivery', 'Dine-In'],
                    values=[
                        sales_data_overview['takeout_orders'].sum(),
                        sales_data_overview['delivery_orders'].sum(),
                        sales_data_overview['dine_in_orders'].sum(),
                    ],
                    textinfo='percent+label',
                ))
                fig_orders.update_layout(title="Order Type Distribution", height=350)
                st.plotly_chart(fig_orders, width='stretch')

            with col2:
                _render_alerts(sales_data_overview, inventory_data, labor_data)

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
                marker_color='#ff6b35',
                yaxis='y'
            ))
            fig_comparison.add_trace(go.Scatter(
                x=sales_data_sales['date'],
                y=sales_data_sales['orders'],
                name='Orders',
                marker_color='#004e89',
                yaxis='y2',
                mode='lines+markers'
            ))
            fig_comparison.update_layout(
                title="Revenue vs Orders",
                xaxis=dict(title='Date', type="date", tickformat="%m/%d/%Y"),
                yaxis=dict(title='Revenue ($)', title_font=dict(color='#ff6b35'), tickfont=dict(color='#ff6b35')),
                yaxis2=dict(title='Orders', title_font=dict(color='#004e89'), tickfont=dict(color='#004e89'), overlaying='y', side='right'),
                hovermode='x unified',
                height=400
            )
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

                st.plotly_chart(fig_discounts, width='stretch')

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
                st.plotly_chart(fig_menu, width='stretch')

            with col1:
                # Order count by item
                menu_by_orders = menu_data.sort_values('order_count', ascending=True).tail(15)
                fig_orders = px.bar(
                    menu_by_orders,
                    y='item',
                    x='order_count',
                    orientation='h',
                    title="Top 15 Items by Order Count",
                    labels={'order_count': 'Orders', 'item': 'Item'}
                )
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

    with tab_inventory:
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
                    marker_color=["#b22222" if b > 0 else "#4682b4" for b in coef_df["beta"]],
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
                cap = min(15, int(customer_data["visit_days"].max()))
                visit_hist = (
                    customer_data["visit_days"]
                    .clip(upper=cap)
                    .value_counts()
                    .sort_index()
                    .reset_index()
                )
                visit_hist.columns = ["visit_days", "customers"]
                fig_visits = px.bar(
                    visit_hist,
                    x="visit_days",
                    y="customers",
                    title="Visit Frequency Distribution (capped at 15)",
                    labels={"visit_days": "Distinct Visit Days", "customers": "Customers"},
                )
                fig_visits.update_layout(height=350, bargap=0.15)
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
                    color_discrete_sequence=["#ff6b35", "#004e89"],
                )
                fig_rev.update_traces(textinfo="percent+label")
                fig_rev.update_layout(height=350)
                st.plotly_chart(fig_rev, width="stretch")

            p95 = customer_data["total_spend"].quantile(0.95)
            fig_spend = px.histogram(
                customer_data["total_spend"].clip(upper=p95),
                nbins=30,
                title="Customer Total Spend Distribution (capped at 95th percentile)",
                color_discrete_sequence=["#ff6b35"],
            )
            fig_spend.update_layout(
                showlegend=False,
                xaxis_title="Total Spend ($)",
                yaxis_title="Customers",
                height=350,
            )
            st.plotly_chart(fig_spend, width="stretch")

            total_orders_in_period = customer_data["order_count"].sum()
            st.caption(
                f"Coverage note: {total_custs:,} customers identified across "
                f"{total_orders_in_period:,.0f} orders in this period. "
                "Dine-in and in-store orders typically have no customer data."
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
        <div style='text-align: center; color: gray; font-size: 12px;'>
            Data Source: """ + footer_source + """ | Last Updated: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """
        </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
