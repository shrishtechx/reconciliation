"""
Inter-Company Ledger Reconciliation System
Professional Streamlit Web Application
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import time
import io
from datetime import datetime

from reconciliation.config import ReconciliationConfig
from reconciliation.normalizer import DataNormalizer
from reconciliation.matcher import ReconciliationEngine
from reconciliation.reporter import ReportGenerator, generate_summary_stats
from reconciliation.sample_data import generate_sample_data, save_sample_to_excel

# ── Page Config ──
st.set_page_config(
    page_title="Inter-Company Ledger Reconciliation",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

NAVY = "#1B2A4A"

# ── Custom CSS ──
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; font-size: 14px; }}
#MainMenu, footer, header {{ visibility: hidden; }}
.block-container {{ padding-top: 0.5rem !important; padding-bottom: 0 !important; max-width: 100% !important; }}

/* Hide sidebar completely */
section[data-testid="stSidebar"] {{ display: none !important; }}
button[data-testid="stSidebarCollapsedControl"] {{ display: none !important; }}

/* Animations */
@keyframes spin {{ to {{ transform: rotate(360deg); }} }}
@keyframes pulse {{ 0%,100% {{ opacity:1; }} 50% {{ opacity:0.5; }} }}
@keyframes shimmer {{
    0% {{ background-position: -200% 0; }}
    100% {{ background-position: 200% 0; }}
}}
@keyframes slideIn {{
    from {{ opacity: 0; transform: translateY(8px); }}
    to {{ opacity: 1; transform: translateY(0); }}
}}

/* Loading card */
.loading-card {{
    display: flex; flex-direction: column; align-items: center; justify-content: center;
    padding: 3rem 2rem; margin: 2rem auto; max-width: 420px;
    background: #fff; border-radius: 16px; border: 1px solid #E2E8F0;
    box-shadow: 0 8px 32px rgba(27,42,74,0.08);
    animation: slideIn 0.3s ease-out;
}}
.ld-spinner {{
    width: 52px; height: 52px; border: 3px solid #E2E8F0;
    border-top: 3px solid {NAVY}; border-radius: 50%;
    animation: spin 0.7s linear infinite; margin-bottom: 18px;
}}
.ld-title {{
    color: {NAVY}; font-weight: 700; font-size: 16px;
    animation: pulse 1.5s ease-in-out infinite;
}}
.ld-sub {{ color: #64748B; font-size: 12px; margin-top: 6px; }}

/* Spinner override */
[data-testid="stSpinner"] div div {{
    border-top-color: {NAVY} !important;
}}

/* Progress bar */
[data-testid="stProgress"] > div > div > div > div {{
    background: linear-gradient(90deg, {NAVY}, #3B6FA0, {NAVY}) !important;
    background-size: 200% 100% !important;
    animation: shimmer 1.8s ease-in-out infinite !important;
}}

/* Metric cards */
.mc {{ background:#fff; border:1px solid #E2E8F0; border-radius:10px; padding:0.65rem 0.5rem; text-align:center; animation: slideIn 0.3s ease-out; }}
.mv {{ font-size:22px; font-weight:700; line-height:1.2; margin:2px 0; }}
.ml {{ font-size:11px; color:#64748B; font-weight:600; text-transform:uppercase; letter-spacing:0.5px; }}

/* Section header */
.sh {{ font-size:15px; font-weight:700; color:{NAVY}; margin:0.5rem 0 0.4rem 0; padding-bottom:0.3rem; border-bottom:2px solid #E2E8F0; }}

/* Badges */
.sb {{ display:inline-block; padding:3px 10px; border-radius:50px; font-size:12px; font-weight:600; }}
.bg {{ background:#DCFCE7; color:#166534; }}
.bw {{ background:#FEF3C7; color:#92400E; }}
.bd {{ background:#FEE2E2; color:#991B1B; }}
.bi {{ background:#DBEAFE; color:#1E40AF; }}

/* Colors */
.navy {{ color:{NAVY}; }}
.green {{ color:#059669; }}
.orange {{ color:#D97706; }}
.red {{ color:#DC2626; }}

/* Upload card */
.uc {{ background:linear-gradient(135deg,#F8FAFC,#F1F5F9); border:2px dashed #CBD5E1; border-radius:12px; padding:1rem; text-align:center; }}
.uc:hover {{ border-color:{NAVY}; }}
.ut {{ font-size:14px; font-weight:700; color:{NAVY}; }}
.us {{ font-size:12px; color:#64748B; }}

/* Info box */
.ib {{ background:#F0F9FF; border:1px solid #BAE6FD; border-radius:10px; padding:0.5rem 0.75rem; }}
.ib p {{ color:#0C4A6E; margin:0; font-size:13px; }}

/* Scenario chips */
.sg {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(170px,1fr)); gap:6px; margin:0.4rem 0; }}
.sc {{ background:#F8FAFC; border:1px solid #E2E8F0; border-radius:8px; padding:5px 8px; font-size:12px; color:#334155; display:flex; align-items:center; gap:5px; }}
.sc .dt {{ width:8px; height:8px; border-radius:50%; flex-shrink:0; }}

/* Filter header row */
.fh {{ color:{NAVY}; font-weight:700; font-size:12px; padding:6px 4px; border-bottom:2px solid {NAVY}; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.fh-row {{ border:1px solid #E2E8F0; border-radius:8px 8px 0 0; overflow:hidden; margin-bottom:0; }}

/* Buttons */
.stButton > button {{ border-radius:8px; font-weight:600; font-size:13px; }}
.stDownloadButton > button {{ background:{NAVY} !important; color:#fff !important; border:none !important; border-radius:8px !important; font-weight:600 !important; }}

/* Title bar */
.title-bar {{
    display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 0.4rem; padding-bottom: 0.3rem;
    border-bottom: 2px solid {NAVY};
}}
.title-text {{
    font-size: 20px; font-weight: 700; color: {NAVY};
    display: flex; align-items: center; gap: 8px;
}}
.title-sub {{ font-size: 12px; color: #64748B; font-weight: 500; margin-left: 12px; }}
</style>
""", unsafe_allow_html=True)


# ── Session State ──
for key, default in [
    ('active_step', 0), ('results', None), ('execution_time', 0),
    ('config', ReconciliationConfig()),
    ('df_a_raw', None), ('df_b_raw', None),
    ('df_a_norm', None), ('df_b_norm', None),
    ('_recon_error', None),
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ── Callbacks (all state changes via on_click — no st.rerun needed) ──
def _reset_all():
    for k in ['df_a_raw', 'df_b_raw', 'df_a_norm', 'df_b_norm', 'results']:
        st.session_state[k] = None
    st.session_state['execution_time'] = 0
    st.session_state['active_step'] = 0
    st.session_state['_recon_error'] = None

def _set_step(i):
    st.session_state.active_step = i

def _load_sample():
    _, _, dfa, dfb = save_sample_to_excel()
    st.session_state.df_a_raw = dfa
    st.session_state.df_b_raw = dfb
    st.session_state.results = None

def _do_reconciliation():
    """on_click callback: runs reconciliation, updates session_state only."""
    if st.session_state.df_a_raw is None or st.session_state.df_b_raw is None:
        st.session_state._recon_error = "Please load data first."
        return
    try:
        config = st.session_state.config
        normalizer = DataNormalizer(config)
        engine = ReconciliationEngine(config)
        t0 = time.time()
        df_a = normalizer.normalize(st.session_state.df_a_raw, company_label="A")
        df_b = normalizer.normalize(st.session_state.df_b_raw, company_label="B")

        if len(df_a) == 0 or len(df_b) == 0:
            cols_a = list(st.session_state.df_a_raw.columns)
            cols_b = list(st.session_state.df_b_raw.columns)
            st.session_state._recon_error = (
                f"Could not detect valid ledger data. "
                f"Company A columns: {cols_a[:5]}... "
                f"Company B columns: {cols_b[:5]}... "
                f"Please ensure the file has Date, Debit/Credit, and Description columns."
            )
            return

        st.session_state.df_a_norm = df_a
        st.session_state.df_b_norm = df_b
        results = engine.reconcile(df_a, df_b)
        elapsed = time.time() - t0
        st.session_state.results = results
        st.session_state.execution_time = elapsed
        st.session_state.active_step = 2
        st.session_state._recon_error = None
    except Exception as e:
        st.session_state._recon_error = str(e)


# ── Title Bar (top-left) ──
t1, t2 = st.columns([5, 1])
with t1:
    st.markdown(
        f'<div class="title-bar"><div class="title-text">'
        f'⚖️ Inter-Company Ledger Reconciliation'
        f'<span class="title-sub">Enterprise Matching Engine</span>'
        f'</div></div>',
        unsafe_allow_html=True)
with t2:
    st.button("🔄 Reset All", use_container_width=True, type="secondary",
              on_click=_reset_all)


# ── Helper: Sanitize DataFrame for display ──
def sanitize_for_display(df):
    """Fix mixed-type columns that crash pyarrow / st.dataframe.
    Converts object columns with mixed types (e.g. datetime + str) to strings."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == 'object':
            types = set(type(v).__name__ for v in df[col].dropna().head(50).values)
            if len(types) > 1:
                df[col] = df[col].apply(
                    lambda x: str(x) if pd.notna(x) else x)
    return df



# ── Helper: Render table with per-column filters (matching reference image) ──
def render_filtered_table(df, key_prefix):
    """Render a dataframe with navy-blue column headers and a filter input
    below each column name, matching the reference image design."""
    cols = list(df.columns)
    n = len(cols)

    # --- Header row: navy blue column names ---
    hdr_cols = st.columns(n)
    for j, col_name in enumerate(cols):
        with hdr_cols[j]:
            st.markdown(
                f'<div class="fh">{col_name}</div>',
                unsafe_allow_html=True)

    # --- Filter row: one input per column ---
    flt_cols = st.columns(n)
    filter_vals = {}
    for j, col_name in enumerate(cols):
        with flt_cols[j]:
            nuniq = df[col_name].dropna().nunique()
            dtype = str(df[col_name].dtype)

            if nuniq <= 20 and 'object' in dtype:
                try:
                    opts = sorted(df[col_name].dropna().unique().tolist(), key=str)
                except Exception:
                    opts = df[col_name].dropna().unique().tolist()
                filter_vals[col_name] = ('select', st.selectbox(
                    col_name, ['All'] + opts,
                    key=f"{key_prefix}_{col_name}",
                    label_visibility="collapsed"))
            else:
                filter_vals[col_name] = ('text', st.text_input(
                    col_name,
                    key=f"{key_prefix}_{col_name}",
                    label_visibility="collapsed",
                    placeholder=f"Filter {col_name}..."))

    # --- Apply filters ---
    filtered_df = df.copy()
    for col_name, (ftype, val) in filter_vals.items():
        if ftype == 'select' and val and val != 'All':
            filtered_df = filtered_df[filtered_df[col_name] == val]
        elif ftype == 'text' and val:
            dtype = str(df[col_name].dtype)
            if 'float' in dtype or 'int' in dtype:
                try:
                    num = float(val)
                    filtered_df = filtered_df[filtered_df[col_name] >= num]
                except ValueError:
                    filtered_df = filtered_df[
                        filtered_df[col_name].astype(str).str.contains(
                            val, case=False, na=False)]
            else:
                filtered_df = filtered_df[
                    filtered_df[col_name].astype(str).str.contains(
                        val, case=False, na=False)]

    st.caption(f"Showing {len(filtered_df)} of {len(df)} records")
    st.dataframe(sanitize_for_display(filtered_df), use_container_width=True, hide_index=True)
    return filtered_df


# ── Stepper Navigation ──
STEPS = [
    ("📁", "Upload Data"),
    ("🔍", "Data Preview"),
    ("📊", "Results Dashboard"),
    ("📋", "Match Details"),
    ("⚠️", "Exceptions"),
    ("⚙️", "Configuration"),
]
nav_cols = st.columns(len(STEPS))
for i, (icon, label) in enumerate(STEPS):
    with nav_cols[i]:
        btype = "primary" if i == st.session_state.active_step else "secondary"
        st.button(f"{icon} {label}", key=f"nav_{i}", type=btype,
                  use_container_width=True, on_click=_set_step, args=(i,))

step = st.session_state.active_step

# Show reconciliation error if any
if st.session_state._recon_error:
    st.error(f"❌ Reconciliation failed: {st.session_state._recon_error}")
    st.session_state._recon_error = None


# ═══════════════════════════════════════════════════════════
# SCREEN 1: UPLOAD DATA
# ═══════════════════════════════════════════════════════════
if step == 0:
    cl, cr = st.columns(2, gap="medium")
    with cl:
        st.markdown(
            '<div class="uc"><div class="ut">📂 Company A Ledger</div>'
            '<div class="us">Debit-side / Payables</div></div>',
            unsafe_allow_html=True)
        file_a = st.file_uploader("Upload A", type=['xlsx', 'xls', 'csv'],
                                   key='file_a', label_visibility="collapsed")
    with cr:
        st.markdown(
            '<div class="uc"><div class="ut">📂 Company B Ledger</div>'
            '<div class="us">Credit-side / Receivables</div></div>',
            unsafe_allow_html=True)
        file_b = st.file_uploader("Upload B", type=['xlsx', 'xls', 'csv'],
                                   key='file_b', label_visibility="collapsed")

    # Load uploaded files (no st.rerun — preview renders in same script run)
    if file_a is not None and st.session_state.df_a_raw is None:
        try:
            st.session_state.df_a_raw = DataNormalizer(
                st.session_state.config).load_file(file_a)
        except Exception as e:
            st.error(f"❌ Error loading Company A: {e}")

    if file_b is not None and st.session_state.df_b_raw is None:
        try:
            st.session_state.df_b_raw = DataNormalizer(
                st.session_state.config).load_file(file_b)
        except Exception as e:
            st.error(f"❌ Error loading Company B: {e}")

    # Action row
    ac1, ac2, ac3 = st.columns([1, 1, 2])
    with ac1:
        st.button("📥 Load Sample Data", use_container_width=True, type="secondary",
                  on_click=_load_sample)
    with ac2:
        can_run = (st.session_state.df_a_raw is not None and
                   st.session_state.df_b_raw is not None)
        st.button("🚀 Run Reconciliation", use_container_width=True,
                  type="primary", disabled=not can_run,
                  on_click=_do_reconciliation)
    with ac3:
        if st.session_state.df_a_raw is not None and st.session_state.df_b_raw is not None:
            st.markdown(
                f'<span class="sb bg">Company A: {len(st.session_state.df_a_raw)} rows</span> '
                f'<span class="sb bg">Company B: {len(st.session_state.df_b_raw)} rows</span>',
                unsafe_allow_html=True)
        else:
            st.markdown(
                '<div class="ib"><p><strong>Getting Started:</strong> Upload two ledger files '
                'or click <strong>Load Sample Data</strong>.</p></div>',
                unsafe_allow_html=True)

    # Data preview right on upload screen
    if st.session_state.df_a_raw is not None:
        st.markdown('<div class="sh">📄 Company A — Preview</div>', unsafe_allow_html=True)
        st.dataframe(sanitize_for_display(st.session_state.df_a_raw.head(10)), use_container_width=True, hide_index=True)

    if st.session_state.df_b_raw is not None:
        st.markdown('<div class="sh">📄 Company B — Preview</div>', unsafe_allow_html=True)
        st.dataframe(sanitize_for_display(st.session_state.df_b_raw.head(10)), use_container_width=True, hide_index=True)

    if st.session_state.df_a_raw is not None and st.session_state.df_b_raw is not None:
        with st.expander("🔧 Column Mapping (Auto-detected)", expanded=False):
            norm = DataNormalizer(st.session_state.config)
            ca, cb = st.columns(2)
            with ca:
                st.markdown("**Company A:**")
                for f, c in norm.detect_columns(st.session_state.df_a_raw).items():
                    st.markdown(f"- `{f}` → **{c}**")
            with cb:
                st.markdown("**Company B:**")
                for f, c in norm.detect_columns(st.session_state.df_b_raw).items():
                    st.markdown(f"- `{f}` → **{c}**")

    st.markdown('<div class="sh">🧩 Supported Matching Scenarios</div>', unsafe_allow_html=True)
    scenarios = [
        ("#059669", "Exact Match"), ("#2563eb", "Timing Difference"),
        ("#7c3aed", "Fuzzy Text Match"), ("#d97706", "Rounding Difference"),
        ("#dc2626", "Tax Deduction (TDS)"), ("#0d9488", "Forex Difference"),
        ("#e11d48", "Partial Settlement"), ("#ca8a04", "Aggregated (Many→1)"),
        ("#64748b", "Missing Entries"), ("#4f46e5", "Duplicate Detection"),
    ]
    html = '<div class="sg">'
    for clr, lbl in scenarios:
        html += f'<div class="sc"><span class="dt" style="background:{clr}"></span>{lbl}</div>'
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
# SCREEN 2: DATA PREVIEW
# ═══════════════════════════════════════════════════════════
elif step == 1:
    if st.session_state.df_a_raw is not None:
        st.markdown('<div class="sh">Company A — Raw Data</div>', unsafe_allow_html=True)
        st.dataframe(sanitize_for_display(st.session_state.df_a_raw), use_container_width=True, hide_index=True)
        c1, c2, c3 = st.columns(3)
        c1.metric("Rows", len(st.session_state.df_a_raw))
        c2.metric("Columns", len(st.session_state.df_a_raw.columns))
        c3.metric("Nulls", int(st.session_state.df_a_raw.isnull().sum().sum()))
    else:
        st.info("⬅️ Upload Company A data on the **Upload Data** screen first.")
    st.markdown("---")
    if st.session_state.df_b_raw is not None:
        st.markdown('<div class="sh">Company B — Raw Data</div>', unsafe_allow_html=True)
        st.dataframe(sanitize_for_display(st.session_state.df_b_raw), use_container_width=True, hide_index=True)
        c1, c2, c3 = st.columns(3)
        c1.metric("Rows", len(st.session_state.df_b_raw))
        c2.metric("Columns", len(st.session_state.df_b_raw.columns))
        c3.metric("Nulls", int(st.session_state.df_b_raw.isnull().sum().sum()))
    else:
        st.info("⬅️ Upload Company B data on the **Upload Data** screen first.")


# ═══════════════════════════════════════════════════════════
# SCREEN 3: RESULTS DASHBOARD
# ═══════════════════════════════════════════════════════════
elif step == 2:
    if st.session_state.results is not None:
        try:
            results = st.session_state.results
            summary = results.get('summary', {})
            stats = generate_summary_stats(results)

            st.markdown('<div class="sh">📈 Key Performance Indicators</div>',
                        unsafe_allow_html=True)
            k1, k2, k3, k4, k5, k6 = st.columns(6)
            kpis = [
                (k1, "Company A Txns",
                 summary.get('Total Transactions Company A', 0), "navy"),
                (k2, "Company B Txns",
                 summary.get('Total Transactions Company B', 0), "navy"),
                (k3, "Total Matches", stats.get('total_matches', 0), "green"),
                (k4, "Exceptions", stats.get('total_exceptions', 0), "red"),
                (k5, "Match Rate A",
                 f"{summary.get('Match Rate A (%)', 0):.1f}%", "green"),
                (k6, "Match Rate B",
                 f"{summary.get('Match Rate B (%)', 0):.1f}%", "green"),
            ]
            for col, label, val, cls in kpis:
                dv = f"{val:,}" if isinstance(val, (int, float)) else val
                with col:
                    st.markdown(
                        f'<div class="mc"><div class="ml">{label}</div>'
                        f'<div class="mv {cls}">{dv}</div></div>',
                        unsafe_allow_html=True)

            k7, k8, k9, k10 = st.columns(4)
            var = summary.get('Net Balance Variance', 0)
            vc = 'green' if abs(var) < 1 else 'orange'
            with k7:
                st.markdown(
                    f'<div class="mc"><div class="ml">Balance Variance</div>'
                    f'<div class="mv {vc}">₹{var:,.2f}</div></div>',
                    unsafe_allow_html=True)
            with k8:
                st.markdown(
                    f'<div class="mc"><div class="ml">Unmatched A</div>'
                    f'<div class="mv orange">'
                    f'{summary.get("Unmatched Company A", 0):,}</div></div>',
                    unsafe_allow_html=True)
            with k9:
                st.markdown(
                    f'<div class="mc"><div class="ml">Unmatched B</div>'
                    f'<div class="mv orange">'
                    f'{summary.get("Unmatched Company B", 0):,}</div></div>',
                    unsafe_allow_html=True)
            with k10:
                st.markdown(
                    f'<div class="mc"><div class="ml">Processing Time</div>'
                    f'<div class="mv navy">'
                    f'{st.session_state.execution_time:.2f}s</div></div>',
                    unsafe_allow_html=True)

            # Charts
            colors = ['#059669', '#2563eb', '#d97706', '#dc2626', '#7c3aed',
                      '#0d9488', '#e11d48', '#ca8a04']
            ch1, ch2 = st.columns(2)
            with ch1:
                st.markdown('<div class="sh">Match Type Distribution</div>',
                            unsafe_allow_html=True)
                mt = stats.get('match_types', {})
                if mt:
                    fig = go.Figure(data=[go.Pie(
                        labels=list(mt.keys()), values=list(mt.values()),
                        hole=0.45, marker=dict(colors=colors[:len(mt)]),
                        textinfo='label+value', textfont=dict(size=11),
                    )])
                    fig.update_layout(height=340,
                        margin=dict(t=10, b=10, l=10, r=10),
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)',
                        legend=dict(font=dict(size=10), orientation="h", y=-0.15),
                        font=dict(family="Inter"))
                    st.plotly_chart(fig, use_container_width=True)

            with ch2:
                st.markdown('<div class="sh">Confidence Score Distribution</div>',
                            unsafe_allow_html=True)
                conf = stats.get('confidence_distribution', {})
                if conf:
                    fig2 = go.Figure(data=[go.Bar(
                        x=list(conf.keys()), y=list(conf.values()),
                        marker_color=[NAVY, '#2563eb', '#d97706', '#dc2626'],
                        text=list(conf.values()), textposition='outside',
                    )])
                    fig2.update_layout(height=340,
                        margin=dict(t=10, b=40, l=40, r=10),
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)',
                        xaxis=dict(title="Range", gridcolor='#f1f5f9'),
                        yaxis=dict(title="Count", gridcolor='#f1f5f9'),
                        font=dict(family="Inter"))
                    st.plotly_chart(fig2, use_container_width=True)

            # Layer breakdown
            matched_list = results.get('matched', [])
            if matched_list:
                mdf_t = pd.DataFrame(matched_list)
                if not mdf_t.empty and 'Matching_Layer' in mdf_t.columns:
                    st.markdown('<div class="sh">Matching Layer Breakdown</div>',
                                unsafe_allow_html=True)
                    lc = mdf_t['Matching_Layer'].value_counts().sort_index()
                    fig3 = go.Figure(data=[go.Bar(
                        x=lc.index.tolist(), y=lc.values.tolist(),
                        marker_color=[NAVY, '#2563eb', '#d97706', '#e11d48',
                                      '#7c3aed', '#0d9488'][:len(lc)],
                        text=lc.values.tolist(), textposition='outside',
                    )])
                    fig3.update_layout(height=260,
                        margin=dict(t=10, b=40, l=40, r=10),
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)',
                        font=dict(family="Inter"))
                    st.plotly_chart(fig3, use_container_width=True)

            # Exception breakdown
            exc_cats = stats.get('exception_categories', {})
            if exc_cats:
                st.markdown('<div class="sh">Exception Categories</div>',
                            unsafe_allow_html=True)
                fig4 = go.Figure(data=[go.Bar(
                    x=list(exc_cats.values()), y=list(exc_cats.keys()),
                    orientation='h',
                    marker_color=['#dc2626', '#e11d48', '#d97706'][:len(exc_cats)],
                    text=list(exc_cats.values()), textposition='outside',
                )])
                fig4.update_layout(height=max(160, len(exc_cats) * 50),
                    margin=dict(t=10, b=10, l=10, r=40),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(family="Inter"))
                st.plotly_chart(fig4, use_container_width=True)

            # Downloads
            st.markdown('<div class="sh">📥 Download Reports</div>',
                        unsafe_allow_html=True)
            dl1, dl2, dl3 = st.columns(3)
            with st.spinner("📊 Preparing reports..."):
                reporter = ReportGenerator(st.session_state.config)
                rbuf = reporter.generate_excel_report(
                    results, st.session_state.execution_time)
            with dl1:
                st.download_button("⬇️ Excel Audit Report", data=rbuf,
                    file_name=f"Reconciliation_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument."
                         "spreadsheetml.sheet")
            with dl2:
                mdf = pd.DataFrame(results.get('matched', []))
                if not mdf.empty:
                    st.download_button("⬇️ Matches CSV",
                        data=mdf.to_csv(index=False).encode(),
                        file_name=f"Matched_{datetime.now():%Y%m%d_%H%M%S}.csv",
                        mime="text/csv")
            with dl3:
                edf = pd.DataFrame(results.get('exceptions', []))
                if not edf.empty:
                    st.download_button("⬇️ Exceptions CSV",
                        data=edf.to_csv(index=False).encode(),
                        file_name=f"Exceptions_{datetime.now():%Y%m%d_%H%M%S}.csv",
                        mime="text/csv")

        except Exception as e:
            st.error(f"❌ Error displaying results: {e}")
            with st.expander("Debug: Raw Results"):
                st.json(st.session_state.results)
    else:
        st.markdown(
            '<div class="ib"><p><strong>No results yet.</strong> Upload data and run '
            'reconciliation from the <strong>Upload Data</strong> screen.</p></div>',
            unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
# SCREEN 4: MATCH DETAILS
# ═══════════════════════════════════════════════════════════
elif step == 3:
    if st.session_state.results is not None:
        matched_list = st.session_state.results.get('matched', [])
        if matched_list:
            matched_df = pd.DataFrame(matched_list)
            if not matched_df.empty:
                st.markdown(
                    '<div class="sh">📋 Matched Transactions — Full Detail</div>',
                    unsafe_allow_html=True)
                render_filtered_table(matched_df, "md")
            else:
                st.info("No matched transactions found.")
        else:
            st.info("No matched transactions found.")
    else:
        st.info("⬅️ Run reconciliation first from the **Upload Data** screen.")


# ═══════════════════════════════════════════════════════════
# SCREEN 5: EXCEPTIONS
# ═══════════════════════════════════════════════════════════
elif step == 4:
    if st.session_state.results is not None:
        exc_list = st.session_state.results.get('exceptions', [])
        if exc_list:
            exc_df = pd.DataFrame(exc_list)
            if not exc_df.empty:
                st.markdown('<div class="sh">⚠️ Exception Analysis</div>',
                            unsafe_allow_html=True)

                exc_cats = exc_df['Category'].value_counts()
                bcols = st.columns(min(len(exc_cats), 6))
                bmap = {'Missing in Company A': 'bd',
                        'Missing in Company B': 'bd',
                        'Duplicate Entry': 'bw'}
                for i, (cat, cnt) in enumerate(exc_cats.items()):
                    with bcols[i % len(bcols)]:
                        st.markdown(
                            f'<span class="sb {bmap.get(cat, "bi")}">'
                            f'{cat}: {cnt}</span>',
                            unsafe_allow_html=True)

                render_filtered_table(exc_df, "exc")

                st.download_button("⬇️ Download Exceptions (CSV)",
                    data=exc_df.to_csv(index=False).encode(),
                    file_name=f"Exceptions_{datetime.now():%Y%m%d_%H%M%S}.csv",
                    mime="text/csv")
            else:
                st.success("🎉 No exceptions found! All transactions matched.")
        else:
            st.success("🎉 No exceptions found! All transactions matched.")
    else:
        st.info("⬅️ Run reconciliation first from the **Upload Data** screen.")


# ═══════════════════════════════════════════════════════════
# SCREEN 6: ENGINE CONFIGURATION
# ═══════════════════════════════════════════════════════════
elif step == 5:
    st.markdown('<div class="sh">⚙️ Engine Configuration</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="ib"><p>'
        'The <strong>Engine Configuration</strong> controls how the reconciliation matching '
        'engine operates. These settings define:<br>'
        '• <strong>Tolerances</strong> — How much difference is acceptable for dates, amounts, '
        'and rounding before two transactions are considered non-matching.<br>'
        '• <strong>Scoring Weights</strong> — The relative importance of amount, date, reference, '
        'and narration similarity when computing a composite match score.<br>'
        '• <strong>Text Similarity Threshold</strong> — Minimum percentage similarity required '
        'for fuzzy narration / description matching.<br>'
        '• <strong>Tax & Forex Parameters</strong> — Known TDS/tax deduction rates and acceptable '
        'foreign-exchange conversion tolerance.<br>'
        '• <strong>Partial Settlement</strong> — Maximum number of transactions that can be grouped '
        'together in a one-to-many or many-to-one match.<br><br>'
        'Adjust these values to fine-tune matching accuracy for your specific business requirements.'
        '</p></div>',
        unsafe_allow_html=True)

    cfg = st.session_state.config
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 📅 Date Settings")
        date_tol = st.slider("Date Tolerance (days)", 1, 30,
                              cfg.date_tolerance_days, key="c_date")
        st.markdown("#### 💰 Amount Settings")
        rounding_tol = st.number_input("Rounding Tolerance (₹)", 0.01, 100.0,
                                        cfg.rounding_tolerance, step=0.5, key="c_round")
        amt_pct = st.slider("Amount Match Tolerance (%)", 0.0, 5.0,
                             cfg.amount_match_tolerance_pct, 0.1, key="c_amt")
        st.markdown("#### 📝 Text Matching")
        fuzzy_thresh = st.slider("Fuzzy Match Threshold (%)", 50, 100,
                                  int(cfg.fuzzy_match_threshold), key="c_fuzzy")
    with c2:
        st.markdown("#### 🎯 Scoring Weights")
        w_amt = st.slider("Amount Weight", 0.0, 1.0, cfg.weight_amount, 0.05, key="c_wamt")
        w_date = st.slider("Date Weight", 0.0, 1.0, cfg.weight_date, 0.05, key="c_wdate")
        w_ref = st.slider("Reference Weight", 0.0, 1.0, cfg.weight_reference, 0.05,
                           key="c_wref")
        w_narr = st.slider("Narration Weight", 0.0, 1.0, cfg.weight_narration, 0.05,
                            key="c_wnarr")
        match_thresh = st.slider("Overall Match Threshold (%)", 50, 100,
                                  int(cfg.overall_match_threshold), key="c_mthresh")

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("#### 🏦 Tax & Forex")
        tax_str = st.text_input("Tax Rates (comma-separated %)",
                                 ", ".join(str(r) for r in cfg.tax_rates), key="c_tax")
        forex_tol = st.slider("Forex Tolerance (%)", 0.5, 15.0,
                               cfg.forex_tolerance_pct, 0.5, key="c_forex")
    with c4:
        st.markdown("#### 📦 Partial Settlement")
        max_group = st.slider("Max Group Size", 2, 10, cfg.max_group_size, key="c_maxgrp")

    try:
        parsed_tax = [float(x.strip()) for x in tax_str.split(',') if x.strip()]
    except ValueError:
        parsed_tax = cfg.tax_rates

    st.session_state.config = ReconciliationConfig(
        date_tolerance_days=date_tol, rounding_tolerance=rounding_tol,
        amount_match_tolerance_pct=amt_pct, fuzzy_match_threshold=float(fuzzy_thresh),
        weight_amount=w_amt, weight_date=w_date,
        weight_reference=w_ref, weight_narration=w_narr,
        overall_match_threshold=float(match_thresh),
        tax_rates=parsed_tax, forex_tolerance_pct=forex_tol, max_group_size=max_group,
    )

    wsum = w_amt + w_date + w_ref + w_narr
    wc = "#059669" if abs(wsum - 1.0) < 0.01 else "#DC2626"
    st.markdown(
        f"**Weights Sum:** <span style='color:{wc};font-weight:700;font-size:16px'>"
        f"{wsum:.2f}</span> (should be 1.00)",
        unsafe_allow_html=True)
