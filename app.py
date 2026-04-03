"""
CallRail Dashboard — Streamlit app
Run with:  streamlit run app.py
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date

from db import (
    init_db,
    load_companies,
    load_monthly_contacts,
    load_monthly_tagged,
    load_monthly_by_tag,
    load_source_breakdown,
    load_scorecard_totals,
    load_all_tags,
    load_call_duration_by_month,
    load_duration_scorecard,
    load_conversion_quality_totals,
    load_conversion_quality_by_month,
)
from config import DATE_FROM

# ── Constants ──────────────────────────────────────────────────────────────────

TODAY = date.today().isoformat()

ALL_KNOWN_TAGS = [
    "appointment sat",
    "closed/lost",
    "closed/won",
    "discovery call booked",
    "happy customer",
    "lead",
    "mql",
    "opportunity",
    "project work",
    "proposal presented",
    "qualified - but not a fit",
    "qualified company",
    "qualified lead",
    "residential",
    "spam/bot",
    "too small",
    "unsure how to score",
]

WINS_FOCUS_TAGS = [
    "qualified lead",
    "qualified company",
    "appointment sat",
    "discovery call booked",
    "closed/won",
    "proposal presented",
    "qualified - but not a fit",
    "opportunity",
    "mql",
    "lead",
    "closed/lost",
    "too small",
    "project work",
]

PIPELINE_TAGS = WINS_FOCUS_TAGS  # same set defines "Qualified Pipeline"

EXCLUDED_IN_WINS = [
    "spam/bot",
    "residential",
    "unsure how to score",
]

# Chart colors
DARK_GREEN  = "#166534"
GREEN       = "#22c55e"
LIGHT_GREEN = "#86efac"
YELLOW      = "#eab308"
RED         = "#ef4444"
BLUE        = "#3b82f6"
PURPLE      = "#a855f7"
ORANGE      = "#f97316"
TEAL        = "#14b8a6"
PINK        = "#ec4899"

TAG_COLORS = {
    "qualified lead":         GREEN,
    "qualified company":      DARK_GREEN,
    "appointment sat":        TEAL,
    "discovery call booked":  BLUE,
    "closed/won":             "#15803d",
    "closed/lost":            RED,
    "spam/bot":               "#6b7280",
    "too small":              "#9ca3af",
    "residential":            YELLOW,
    "happy customer":         LIGHT_GREEN,
    "qualified - but not a fit": ORANGE,
    "unsure how to score":    "#6b7280",
}

SOURCE_COLORS = [GREEN, BLUE, ORANGE, PURPLE, TEAL, YELLOW, PINK, "#f43f5e", "#06b6d4", "#d97706"]

# Fixed color per source — keeps donuts comparable across time periods
SOURCE_COLOR_MAP = {
    "Website (Direct)":   GREEN,       # #22c55e
    "Google My Business": BLUE,        # #3b82f6
    "Google Ads":         PURPLE,      # #a855f7
    "Referral":           ORANGE,      # #f97316
    "Bing":               YELLOW,      # #eab308
    "Google Organic":     TEAL,        # #14b8a6
    "Offline Marketing":  "#f43f5e",   # rose — offline tracking numbers, print, SDR
    "Social Media":       PINK,        # #ec4899
    "Email":              LIGHT_GREEN, # #86efac
    "Other":              "#6b7280",   # gray
}

# ── Page config ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="CallRail Dashboard",
    page_icon="📞",
    layout="wide",
)

# Dark theme CSS matching PRT Visualizer
st.markdown("""
<style>
/* ── Dark base ── */
html, body, [data-testid="stAppViewContainer"],
[data-testid="stMain"], [data-testid="block-container"] {
    background-color: #0f172a !important;
    color: #f1f5f9 !important;
}
[data-testid="stSidebar"] {
    background-color: #1e293b !important;
}
[data-testid="stSidebar"] * { color: #f1f5f9 !important; }

/* Metric cards */
[data-testid="stMetric"] {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 10px;
    padding: 14px 18px;
}
[data-testid="stMetricLabel"]  { color: #94a3b8 !important; font-size: 0.82rem; }
[data-testid="stMetricValue"]  { color: #f1f5f9 !important; }
[data-testid="stMetricDelta"]  { font-size: 0.8rem; }

/* Buttons */
.stButton > button {
    background-color: #22c55e !important;
    color: #0f172a !important;
    border: none !important;
    font-weight: 600 !important;
    border-radius: 8px !important;
}
.stButton > button:hover {
    background-color: #16a34a !important;
}

/* Dividers */
hr { border-color: #334155 !important; }

/* Toggle / checkbox */
[data-testid="stCheckbox"] label { font-size: 0.9rem; }

/* Multiselect */
[data-testid="stMultiSelect"] > div {
    background: #1e293b !important;
    border-color: #334155 !important;
}

/* Section headers */
h2, h3 { color: #f1f5f9 !important; }

/* Plotly charts transparent background */
.js-plotly-plot .plotly, .plot-container { background: transparent !important; }
</style>
""", unsafe_allow_html=True)

init_db()


# ── Cached loaders ─────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _monthly_contacts(date_from, date_to):
    return load_monthly_contacts(date_from, date_to)

@st.cache_data(show_spinner=False)
def _monthly_tagged(date_from, date_to, tags_tuple):
    return load_monthly_tagged(date_from, date_to, list(tags_tuple))

@st.cache_data(show_spinner=False)
def _monthly_by_tag(date_from, date_to, tags_tuple):
    return load_monthly_by_tag(date_from, date_to, list(tags_tuple))

@st.cache_data(show_spinner=False)
def _source_breakdown(date_from, date_to, tags_tuple):
    return load_source_breakdown(date_from, date_to, list(tags_tuple) if tags_tuple else None)

@st.cache_data(show_spinner=False)
def _scorecard(date_from, date_to, pipeline_tags_tuple):
    return load_scorecard_totals(date_from, date_to, list(pipeline_tags_tuple))

@st.cache_data(show_spinner=False)
def _all_tags():
    return load_all_tags()


@st.cache_data(show_spinner=False)
def _conversion_quality_totals(date_from, date_to, pipeline_tags_tuple):
    return load_conversion_quality_totals(date_from, date_to, list(pipeline_tags_tuple))

@st.cache_data(show_spinner=False)
def _conversion_quality_by_month(date_from, date_to, pipeline_tags_tuple):
    return load_conversion_quality_by_month(date_from, date_to, list(pipeline_tags_tuple))

@st.cache_data(show_spinner=False)
def _duration_by_month(date_from, date_to, pipeline_tags_tuple):
    return load_call_duration_by_month(date_from, date_to, list(pipeline_tags_tuple))

@st.cache_data(show_spinner=False)
def _duration_scorecard(date_from, date_to, pipeline_tags_tuple):
    return load_duration_scorecard(date_from, date_to, list(pipeline_tags_tuple))


def clear_cache():
    _monthly_contacts.clear()
    _monthly_tagged.clear()
    _monthly_by_tag.clear()
    _source_breakdown.clear()
    _scorecard.clear()
    _all_tags.clear()
    _conversion_quality_totals.clear()
    _conversion_quality_by_month.clear()
    _duration_by_month.clear()
    _duration_scorecard.clear()


# ── Plotly dark layout defaults ────────────────────────────────────────────────

PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#1e293b",
    font=dict(color="#f1f5f9", size=12),
    xaxis=dict(
        gridcolor="#334155",
        linecolor="#475569",
        tickfont=dict(color="#94a3b8"),
    ),
    yaxis=dict(
        gridcolor="#334155",
        linecolor="#475569",
        tickfont=dict(color="#94a3b8"),
    ),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        font=dict(color="#f1f5f9"),
        orientation="h",
        yanchor="bottom",
        y=1.02,
        x=0,
    ),
    margin=dict(t=60, b=40, l=40, r=20),
    height=380,
)


def apply_dark(fig: go.Figure, **overrides) -> go.Figure:
    layout = {**PLOT_LAYOUT, **overrides}
    fig.update_layout(**layout)
    return fig


# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 📞 CallRail Dashboard")
    st.markdown("---")

    # Wins Focus toggle
    wins_focus = st.toggle("Wins Focus", value=True,
                           help="Focus on qualified pipeline signals only")

    st.markdown("---")

    # Tag filter
    db_tags   = _all_tags()
    all_tags  = sorted(set(ALL_KNOWN_TAGS) | set(db_tags))

    if wins_focus:
        default_selected = WINS_FOCUS_TAGS
    else:
        default_selected = [t for t in all_tags if t not in EXCLUDED_IN_WINS or not wins_focus]

    st.markdown("**Tag Filter**")
    selected_tags = st.multiselect(
        label="Include tags",
        options=all_tags,
        default=[t for t in WINS_FOCUS_TAGS if t in all_tags] if wins_focus
                else [t for t in all_tags if t not in EXCLUDED_IN_WINS],
        label_visibility="collapsed",
    )

    st.markdown("---")

    # Refresh Data button
    if st.button("🔄 Refresh Data", use_container_width=True):
        with st.spinner("Syncing from CallRail API…"):
            try:
                from sync import sync_all
                messages = []
                def _cb(msg):
                    messages.append(msg)
                failures = sync_all(progress_callback=_cb)
                clear_cache()
                st.cache_data.clear()
                if failures:
                    st.error(f"{len(failures)} error(s) during sync.")
                    for f in failures[:5]:
                        st.warning(f)
                else:
                    st.success("Sync complete!")
                if messages:
                    with st.expander("Sync log (last 60 lines)"):
                        st.text("\n".join(messages[-60:]))
            except ValueError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.error(f"Sync failed: {exc}")
        st.rerun()

    st.markdown("---")
    companies = load_companies()
    st.caption(f"**{len(companies)} companies** in cache")
    if companies:
        with st.expander("Companies"):
            for c in companies:
                st.caption(f"• {c['name']}")


# ── Main content ───────────────────────────────────────────────────────────────

mode_label = "Wins Focus" if wins_focus else "Full Picture"
st.markdown(f"# CallRail Portfolio Dashboard")
st.markdown(f"*Mode: **{mode_label}** · Jun 2024 – Today · {len(companies)} companies*")

selected_tags_tuple = tuple(sorted(selected_tags))
pipeline_tags_tuple = tuple(sorted(t for t in PIPELINE_TAGS if t in selected_tags))

no_data = len(companies) == 0

if no_data:
    st.info("No data yet. Click **Refresh Data** in the sidebar to pull from CallRail.")
    st.stop()

if not selected_tags:
    st.warning("No tags selected. Choose at least one tag in the sidebar to see filtered metrics.")

# ── Scorecards ─────────────────────────────────────────────────────────────────

st.markdown("## Portfolio Scorecards")

scorecard = _scorecard(DATE_FROM, TODAY, pipeline_tags_tuple)

total_pipeline  = scorecard.get("total_pipeline",   0)
total_closed    = scorecard.get("total_closed_won", 0)
total_contacts  = scorecard.get("total_contacts",   0)
pct_change      = scorecard.get("pct_change",       0.0)
early_period    = scorecard.get("early_period",     0)
recent_period   = scorecard.get("recent_period",    0)

pct_pipeline = (total_pipeline / total_contacts * 100) if total_contacts else 0
delta_label  = f"{pct_change:+.1f}% vs first half" if early_period else "—"

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        label="Qualified Pipeline",
        value=f"{total_pipeline:,}",
        delta=delta_label,
        help="qualified lead + qualified company + appointment sat + discovery call booked + closed/won",
    )
with col2:
    st.metric(
        label="Closed / Won",
        value=f"{total_closed:,}",
        help="Total closed/won contacts since Feb 2024",
    )
with col3:
    st.metric(
        label="Total Contacts",
        value=f"{total_contacts:,}",
        help="All calls + form submissions",
    )
with col4:
    st.metric(
        label="Pipeline Rate",
        value=f"{pct_pipeline:.1f}%",
        help="Qualified Pipeline ÷ Total Contacts",
    )

st.markdown("---")

# ── Qualified Lead Velocity ────────────────────────────────────────────────────

st.markdown("## Qualified Lead Velocity")

# Always use full pipeline tag set for velocity so the chart is stable
_vel_tags = tuple(sorted(PIPELINE_TAGS))
_vel_rows = _monthly_tagged(DATE_FROM, TODAY, _vel_tags)

if _vel_rows:
    _df_vel   = pd.DataFrame(_vel_rows)
    _df_qual  = (_df_vel[_df_vel["category"] == "tagged"]
                 .copy()
                 .sort_values("month")
                 .set_index("month"))
    _vel_months = _df_qual.index.tolist()
    _vel_counts = _df_qual["cnt"].tolist()
    _vel_series = pd.Series(_vel_counts, index=_vel_months)

    # Rolling 3-month average (min_periods=1 so early months still show a line)
    _rolling_avg = _vel_series.rolling(window=3, min_periods=1).mean().round(1)

    # YoY: last 3 calendar months vs same 3 months one year prior
    _yoy_label = ""
    if len(_vel_months) >= 3:
        _recent_months   = _vel_months[-3:]
        _recent_total    = _vel_series.iloc[-3:].sum()
        _ly_months       = [f"{int(m[:4])-1}-{m[5:]}" for m in _recent_months]
        _ly_total        = sum(_vel_series[m] if m in _vel_series.index else 0 for m in _ly_months)
        if _ly_total > 0:
            _yoy_pct   = (_recent_total - _ly_total) / _ly_total * 100
            _yoy_arrow = "↑" if _yoy_pct > 0 else "↓"
            _yoy_label = f"{_yoy_arrow} {abs(_yoy_pct):.0f}% vs same period last year"
        else:
            _yoy_label = "No data from same period last year to compare"

    if _yoy_label:
        _color_yoy = GREEN if "↑" in _yoy_label else RED
        st.markdown(
            f'<span style="color:{_color_yoy};font-size:1.1rem;font-weight:600">{_yoy_label}</span>',
            unsafe_allow_html=True,
        )

    fig_vel = go.Figure()
    fig_vel.add_trace(go.Bar(
        name="Monthly Qualified Leads",
        x=_vel_months,
        y=_vel_counts,
        marker_color="rgba(100,116,139,0.40)",
        hovertemplate="Qualified leads: %{y}<extra></extra>",
    ))
    fig_vel.add_trace(go.Scatter(
        name="3-Month Rolling Avg",
        x=_vel_months,
        y=_rolling_avg.tolist(),
        mode="lines+markers",
        line=dict(color=GREEN, width=3),
        marker=dict(size=7, color=GREEN),
        hovertemplate="3-mo avg: %{y:.1f}<extra></extra>",
    ))
    fig_vel.update_layout(
        title="Qualified Lead Velocity — 3-Month Rolling Average",
        xaxis_title="Month",
        yaxis_title="Qualified Leads",
        **{k: v for k, v in PLOT_LAYOUT.items() if k not in ("xaxis", "yaxis", "height", "legend")},
        xaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
        yaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
        height=380,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0,
                    bgcolor="rgba(0,0,0,0)", font=dict(color="#f1f5f9")),
    )
    st.plotly_chart(fig_vel, use_container_width=True)
else:
    st.info("No qualified lead data. Sync data first.")

st.markdown("---")

# ── Headline: Qualified Leads Over Time (stacked bar) ─────────────────────────

st.markdown("## Qualified Leads Over Time")

tagged_rows  = _monthly_tagged(DATE_FROM, TODAY, selected_tags_tuple)
contact_rows = _monthly_contacts(DATE_FROM, TODAY)

if tagged_rows:
    df_tagged = pd.DataFrame(tagged_rows)
    df_pivot  = df_tagged.pivot_table(index="month", columns="category", values="cnt", aggfunc="sum", fill_value=0)
    for col in ["tagged", "other"]:
        if col not in df_pivot.columns:
            df_pivot[col] = 0
    df_pivot = df_pivot.sort_index()
    months   = df_pivot.index.tolist()

    # Build total contacts per month for the secondary line
    total_by_month: dict[str, int] = {}
    if contact_rows:
        df_ct = pd.DataFrame(contact_rows)
        for m, grp in df_ct.groupby("month"):
            total_by_month[m] = int(grp["cnt"].sum())

    qualified_label = "Qualified Leads" if wins_focus else "Tagged Contacts"

    # Qualified leads as prominent bars on primary axis;
    # total contacts as a muted line on secondary axis so scale doesn't crush the bars.
    fig_qual = go.Figure()
    fig_qual.add_trace(go.Bar(
        name=qualified_label,
        x=months,
        y=df_pivot["tagged"],
        marker_color=DARK_GREEN,
        yaxis="y",
        hovertemplate=f"{qualified_label}: %{{y}}<extra></extra>",
    ))
    if total_by_month:
        fig_qual.add_trace(go.Scatter(
            name="Total Contacts",
            x=months,
            y=[total_by_month.get(m, 0) for m in months],
            mode="lines",
            line=dict(color=LIGHT_GREEN, width=2, dash="dot"),
            yaxis="y2",
            hovertemplate="Total Contacts: %{y}<extra></extra>",
        ))
    fig_qual.update_layout(
        title="Qualified Leads per Month (total contacts as reference)",
        xaxis_title="Month",
        yaxis=dict(
            title=qualified_label,
            gridcolor="#334155",
            linecolor="#475569",
            tickfont=dict(color="#94a3b8"),
        ),
        yaxis2=dict(
            title="Total Contacts",
            overlaying="y",
            side="right",
            showgrid=False,
            tickfont=dict(color="#64748b"),
            title_font=dict(color="#64748b"),
        ),
        **{k: v for k, v in PLOT_LAYOUT.items() if k not in ("xaxis", "yaxis", "height", "legend")},
        xaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0,
                    bgcolor="rgba(0,0,0,0)", font=dict(color="#f1f5f9")),
    )
    st.plotly_chart(fig_qual, use_container_width=True)
else:
    st.info("No tagged contact data. Sync data first.")

st.markdown("---")

# ── Secondary: Tag breakdown + Total Contacts ─────────────────────────────────

col_left, col_right = st.columns(2)

with col_left:
    st.markdown("### Contacts by Tag per Month")
    tag_rows = _monthly_by_tag(DATE_FROM, TODAY, selected_tags_tuple)
    if tag_rows:
        df_by_tag = pd.DataFrame(tag_rows)
        pivot_tag = (
            df_by_tag.pivot_table(index="month", columns="tag", values="cnt", aggfunc="sum", fill_value=0)
            .sort_index()
        )
        fig_tags = go.Figure()
        for j, tag in enumerate(pivot_tag.columns):
            color = TAG_COLORS.get(tag, SOURCE_COLORS[j % len(SOURCE_COLORS)])
            fig_tags.add_trace(go.Bar(
                name=tag.title(),
                x=pivot_tag.index.tolist(),
                y=pivot_tag[tag],
                marker_color=color,
                hovertemplate=f"{tag}: %{{y}}<extra></extra>",
            ))
        fig_tags.update_layout(
            barmode="stack",
            xaxis_title="Month",
            yaxis_title="Count",
            **{k: v for k, v in PLOT_LAYOUT.items() if k not in ("xaxis", "yaxis", "height")},
            xaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
            yaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
            height=380,
        )
        st.plotly_chart(fig_tags, use_container_width=True)
    else:
        st.info("No tag data to display.")

with col_right:
    st.markdown("### Total Contacts Over Time")
    contact_rows = _monthly_contacts(DATE_FROM, TODAY)
    if contact_rows:
        df_contacts = pd.DataFrame(contact_rows)
        pivot_ct    = (
            df_contacts.pivot_table(index="month", columns="contact_type", values="cnt", aggfunc="sum", fill_value=0)
            .sort_index()
        )
        for col in ["call", "form"]:
            if col not in pivot_ct.columns:
                pivot_ct[col] = 0
        pivot_ct["total"] = pivot_ct["call"] + pivot_ct["form"]

        fig_ct = go.Figure()
        fig_ct.add_trace(go.Scatter(
            name="Total Contacts",
            x=pivot_ct.index.tolist(),
            y=pivot_ct["total"],
            mode="lines+markers",
            line=dict(color=BLUE, width=3),
            marker=dict(size=6, color=BLUE),
            hovertemplate="Total: %{y}<extra></extra>",
        ))
        fig_ct.add_trace(go.Scatter(
            name="Calls",
            x=pivot_ct.index.tolist(),
            y=pivot_ct["call"],
            mode="lines+markers",
            line=dict(color=GREEN, width=2, dash="dot"),
            marker=dict(size=5),
            hovertemplate="Calls: %{y}<extra></extra>",
        ))
        fig_ct.add_trace(go.Scatter(
            name="Forms",
            x=pivot_ct.index.tolist(),
            y=pivot_ct["form"],
            mode="lines+markers",
            line=dict(color=TEAL, width=2, dash="dot"),
            marker=dict(size=5),
            hovertemplate="Forms: %{y}<extra></extra>",
        ))
        fig_ct.update_layout(
            title="Total Contacts Over Time (Calls + Forms)",
            xaxis_title="Month",
            yaxis_title="Count",
            **{k: v for k, v in PLOT_LAYOUT.items() if k not in ("xaxis", "yaxis", "height")},
            xaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
            yaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
            height=380,
        )
        st.plotly_chart(fig_ct, use_container_width=True)
    else:
        st.info("No contact data to display.")

st.markdown("---")

# ── Call volume vs Form volume split ──────────────────────────────────────────

st.markdown("## Call vs Form Volume by Month")

if contact_rows:
    fig_split = go.Figure()
    fig_split.add_trace(go.Bar(
        name="Calls",
        x=pivot_ct.index.tolist(),
        y=pivot_ct["call"],
        marker_color=GREEN,
        hovertemplate="Calls: %{y}<extra></extra>",
    ))
    fig_split.add_trace(go.Bar(
        name="Forms",
        x=pivot_ct.index.tolist(),
        y=pivot_ct["form"],
        marker_color=TEAL,
        hovertemplate="Forms: %{y}<extra></extra>",
    ))
    fig_split.update_layout(
        barmode="stack",
        title="Call Volume vs Form Volume",
        xaxis_title="Month",
        yaxis_title="Count",
        **{k: v for k, v in PLOT_LAYOUT.items() if k not in ("xaxis", "yaxis", "height")},
        xaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
        yaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
        height=360,
    )
    st.plotly_chart(fig_split, use_container_width=True)

st.markdown("---")

# ── Source breakdown ───────────────────────────────────────────────────────────

st.markdown("## Lead Source Analysis")

# ── Compute 3 comparison periods from DATE_FROM to today ─────────────────────
import datetime as _dt

def _add_months(d: _dt.date, n: int) -> _dt.date:
    m = d.month + n
    return d.replace(year=d.year + (m - 1) // 12, month=(m - 1) % 12 + 1)

_d_from  = _dt.date.fromisoformat(DATE_FROM)
_d_today = _dt.date.today()
_total_m = (_d_today.year - _d_from.year) * 12 + (_d_today.month - _d_from.month)
_third   = max(_total_m // 3, 1)

_p1_start = DATE_FROM
_p1_end   = _add_months(_d_from, _third).isoformat()
_p2_start = _p1_end
_p2_end   = _add_months(_d_from, _third * 2).isoformat()
_p3_start = _p2_end
_p3_end   = TODAY

_p1_label = f"{_d_from.strftime('%b %Y')} – {_add_months(_d_from, _third).strftime('%b %Y')}"
_p2_label = f"{_add_months(_d_from, _third).strftime('%b %Y')} – {_add_months(_d_from, _third*2).strftime('%b %Y')}"
_p3_label = f"{_add_months(_d_from, _third*2).strftime('%b %Y')} – Today"

_src_tags = selected_tags_tuple if selected_tags else ()


def _make_donut(rows: list[dict], title: str) -> go.Figure:
    """Build a single donut chart from source rows, pinning each source to a fixed color."""
    if not rows:
        return None
    df = pd.DataFrame(rows).sort_values("cnt", ascending=False)
    TOP_N = 7
    top = df.head(TOP_N)
    rest = df.iloc[TOP_N:]["cnt"].sum()
    if rest > 0:
        top = pd.concat([top, pd.DataFrame([{"source": "Other", "cnt": rest}])], ignore_index=True)
    # Look up each slice's color by name so colors are consistent across periods
    colors = [SOURCE_COLOR_MAP.get(s, "#6b7280") for s in top["source"]]
    fig = go.Figure(go.Pie(
        labels=top["source"],
        values=top["cnt"],
        hole=0.42,
        marker=dict(colors=colors),
        textinfo="percent",
        textfont=dict(color="#f1f5f9", size=10),
        hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text=title, font=dict(size=13, color="#f1f5f9"), x=0.5, xanchor="center"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#f1f5f9"),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#cbd5e1", size=10),
                    orientation="v", x=1.0, y=0.5),
        margin=dict(t=50, b=10, l=10, r=10),
        height=320,
    )
    return fig


# ── Hero: 3-period donut comparison ──────────────────────────────────────────

st.markdown("### Source Mix Evolution")

rows_p1 = _source_breakdown(_p1_start, _p1_end, _src_tags)
rows_p2 = _source_breakdown(_p2_start, _p2_end, _src_tags)
rows_p3 = _source_breakdown(_p3_start, _p3_end, _src_tags)

c1, c2, c3 = st.columns(3)
with c1:
    fig_p1 = _make_donut(rows_p1, _p1_label)
    if fig_p1:
        st.plotly_chart(fig_p1, use_container_width=True)
    else:
        st.info("No data for this period.")
with c2:
    fig_p2 = _make_donut(rows_p2, _p2_label)
    if fig_p2:
        st.plotly_chart(fig_p2, use_container_width=True)
    else:
        st.info("No data for this period.")
with c3:
    fig_p3 = _make_donut(rows_p3, _p3_label)
    if fig_p3:
        st.plotly_chart(fig_p3, use_container_width=True)
    else:
        st.info("No data for this period.")

# ── Auto-calculated insight line ──────────────────────────────────────────────
def _pct(rows: list[dict], channel: str) -> float:
    if not rows:
        return 0.0
    total = sum(r["cnt"] for r in rows)
    match = sum(r["cnt"] for r in rows if r["source"] == channel)
    return (match / total * 100) if total else 0.0

# Hardcoded GMB growth story using actual p1 → p3 percentages
_gmb_p1 = _pct(rows_p1, "Google My Business")
_gmb_p3 = _pct(rows_p3, "Google My Business")
if _gmb_p1 > 0 or _gmb_p3 > 0:
    _arrow = "↑" if _gmb_p3 >= _gmb_p1 else "↓"
    _insight = (
        f"**{_arrow} Google My Business** grew from **{_gmb_p1:.0f}%** "
        f"to **{_gmb_p3:.0f}%** of qualified contacts "
        f"(Jun 2024 → Today)"
    )
    st.markdown(_insight)

st.caption("*Unknown sources excluded. Contacts with no source attribution are not shown.*")

st.markdown("---")

# ── Call vs Form Conversion Quality ──────────────────────────────────────────

st.markdown("### Which Converts Better — Calls or Forms?")

_cq_tags = pipeline_tags_tuple if pipeline_tags_tuple else tuple(sorted(PIPELINE_TAGS))

_cq_totals = _conversion_quality_totals(DATE_FROM, TODAY, _cq_tags)
_calls_rate  = _cq_totals.get("calls_rate",  0.0)
_forms_rate  = _cq_totals.get("forms_rate",  0.0)
_calls_qual  = _cq_totals.get("calls_qualified", 0)
_calls_total = _cq_totals.get("calls_total",     0)
_forms_qual  = _cq_totals.get("forms_qualified", 0)
_forms_total = _cq_totals.get("forms_total",     0)

if _calls_total > 0 or _forms_total > 0:
    # Callout line
    if _calls_rate > 0 and _forms_rate > 0:
        _better = "Calls" if _calls_rate >= _forms_rate else "Forms"
        _worse  = "Forms" if _better == "Calls" else "Calls"
        _better_rate = _calls_rate if _better == "Calls" else _forms_rate
        _worse_rate  = _forms_rate if _better == "Calls" else _calls_rate
        _callout = (
            f"**{_better}** convert at **{_better_rate:.1f}%** vs "
            f"**{_worse_rate:.1f}%** for {_worse.lower()} — "
            f"{'calls are' if _better == 'Calls' else 'forms are'} "
            f"**{(_better_rate / _worse_rate):.1f}x more likely** to produce a qualified lead"
        )
    elif _calls_rate > 0:
        _callout = f"Calls convert at **{_calls_rate:.1f}%** (no qualified form data yet)"
    else:
        _callout = f"Forms convert at **{_forms_rate:.1f}%** (no qualified call data yet)"

    st.markdown(_callout)

    # Scorecard row
    _cq1, _cq2, _cq3 = st.columns(3)
    with _cq1:
        st.metric(
            label="Call Conversion Rate",
            value=f"{_calls_rate:.1f}%",
            help=f"{_calls_qual:,} qualified out of {_calls_total:,} total calls",
        )
    with _cq2:
        st.metric(
            label="Form Conversion Rate",
            value=f"{_forms_rate:.1f}%",
            help=f"{_forms_qual:,} qualified out of {_forms_total:,} total forms",
        )
    with _cq3:
        _gap = _calls_rate - _forms_rate
        _gap_sign = "+" if _gap >= 0 else ""
        st.metric(
            label="Calls vs Forms Gap",
            value=f"{_gap_sign}{_gap:.1f}pp",
            help="Positive = calls convert better; negative = forms convert better",
        )

    # Monthly trend line chart
    _cq_monthly = _conversion_quality_by_month(DATE_FROM, TODAY, _cq_tags)
    if _cq_monthly:
        _df_cq = pd.DataFrame(_cq_monthly)
        # Only show months where we have at least some volume
        _df_cq = _df_cq[(_df_cq["calls_total"] > 0) | (_df_cq["forms_total"] > 0)]

        fig_cq = go.Figure()
        fig_cq.add_trace(go.Scatter(
            name="Call Conversion Rate",
            x=_df_cq["month"],
            y=_df_cq["calls_rate"],
            mode="lines+markers",
            line=dict(color=GREEN, width=3),
            marker=dict(size=7, color=GREEN),
            hovertemplate="Calls: %{y:.1f}%<extra></extra>",
        ))
        fig_cq.add_trace(go.Scatter(
            name="Form Conversion Rate",
            x=_df_cq["month"],
            y=_df_cq["forms_rate"],
            mode="lines+markers",
            line=dict(color=BLUE, width=3, dash="dot"),
            marker=dict(size=7, color=BLUE),
            hovertemplate="Forms: %{y:.1f}%<extra></extra>",
        ))
        fig_cq.update_layout(
            title="Monthly Qualified Rate — Calls vs Forms",
            xaxis_title="Month",
            yaxis=dict(
                title="Qualified Rate (%)",
                ticksuffix="%",
                gridcolor="#334155",
                linecolor="#475569",
                tickfont=dict(color="#94a3b8"),
                rangemode="tozero",
            ),
            **{k: v for k, v in PLOT_LAYOUT.items() if k not in ("xaxis", "yaxis", "height", "legend")},
            xaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
            height=360,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0,
                        bgcolor="rgba(0,0,0,0)", font=dict(color="#f1f5f9")),
        )
        st.plotly_chart(fig_cq, use_container_width=True)
else:
    st.info("No contact data yet. Sync data first.")

st.markdown("---")

# ── Call Quality by Duration ───────────────────────────────────────────────────

st.markdown("## Call Quality by Duration")

_dur_tags = pipeline_tags_tuple if pipeline_tags_tuple else tuple(sorted(PIPELINE_TAGS))

_dur_sc = _duration_scorecard(DATE_FROM, TODAY, _dur_tags)
_qual_sec   = _dur_sc.get("qualified_avg", 0.0)
_unqual_sec = _dur_sc.get("unqualified_avg", 0.0)


def _fmt_dur(seconds: float) -> str:
    s = max(0, int(seconds))
    return f"{s // 60}m {s % 60:02d}s"


_gap_sec = _qual_sec - _unqual_sec
if _qual_sec > 0 or _unqual_sec > 0:
    _gap_word = "longer" if _gap_sec >= 0 else "shorter"
    _note = (
        f"Qualified calls average **{_fmt_dur(_qual_sec)}** "
        f"vs **{_fmt_dur(_unqual_sec)}** for all other calls "
        f"(**+{_fmt_dur(abs(_gap_sec))} {_gap_word}**)"
    )
    st.markdown(_note)

    _dc1, _dc2, _dc3 = st.columns(3)
    with _dc1:
        st.metric(
            label="Avg Duration — Qualified Calls",
            value=_fmt_dur(_qual_sec),
            help="Average duration of calls tagged with pipeline tags",
        )
    with _dc2:
        st.metric(
            label="Avg Duration — All Other Calls",
            value=_fmt_dur(_unqual_sec),
            help="Average duration of calls with none of the pipeline tags",
        )
    with _dc3:
        _gap_sign = "+" if _gap_sec >= 0 else "-"
        st.metric(
            label="Duration Gap",
            value=f"{_gap_sign}{_fmt_dur(abs(_gap_sec))}",
            help="Qualified minus unqualified average — longer qualified calls = stronger lead signal",
        )

_dur_rows = _duration_by_month(DATE_FROM, TODAY, _dur_tags)
if _dur_rows:
    _df_dur = pd.DataFrame(_dur_rows)
    _df_dur["qualified_min"]   = (_df_dur["qualified_avg"]   / 60).round(2)
    _df_dur["unqualified_min"] = (_df_dur["unqualified_avg"] / 60).round(2)

    fig_dur = go.Figure()
    fig_dur.add_trace(go.Bar(
        name="Qualified Calls",
        x=_df_dur["month"],
        y=_df_dur["qualified_min"],
        marker_color=GREEN,
        hovertemplate="Qualified: %{y:.1f} min<extra></extra>",
    ))
    fig_dur.add_trace(go.Bar(
        name="Other Calls",
        x=_df_dur["month"],
        y=_df_dur["unqualified_min"],
        marker_color=BLUE,
        hovertemplate="Other: %{y:.1f} min<extra></extra>",
    ))
    fig_dur.update_layout(
        barmode="group",
        title="Average Call Duration by Month — Qualified vs Other Calls",
        xaxis_title="Month",
        yaxis_title="Avg Duration (minutes)",
        **{k: v for k, v in PLOT_LAYOUT.items() if k not in ("xaxis", "yaxis", "height", "legend")},
        xaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
        yaxis=dict(gridcolor="#334155", linecolor="#475569", tickfont=dict(color="#94a3b8")),
        height=380,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0,
                    bgcolor="rgba(0,0,0,0)", font=dict(color="#f1f5f9")),
    )
    st.plotly_chart(fig_dur, use_container_width=True)
else:
    st.info("No call duration data available.")

st.markdown("---")

# ── Footer ────────────────────────────────────────────────────────────────────
st.caption(f"Data pulled from CallRail · Jun 2024 – {TODAY} · Refresh via sidebar")
