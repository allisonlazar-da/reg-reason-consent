"""Reg Reason & Consent — Streamlit-in-Snowflake rebuild of the Tableau workbook.

Source workbook: "Reg Reason & Consent" (Niche App Analytics Tracking
Dashboard v3.0). Every worksheet and filter from the original Tableau
dashboard is reproduced 1:1 here.

Data is pulled live on every page load from
`NICHE_DATA_HUB.RAT.REG_CONSENT` (cached client-side for 1 hour).
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st
from snowflake.snowpark.context import get_active_session

# ---------------------------------------------------------------------------
# Niche brand palette
# ---------------------------------------------------------------------------
NICHE_GREEN = "#003024"
NICHE_MID_GREEN = "#016853"
NICHE_JADE = "#00BE76"
NICHE_GREIGE = "#F3E9E3"
NICHE_YELLOW = "#FF9B00"
NICHE_ORANGE_RED = "#FB5A00"

# Tableau workbook colors (matched exactly)
APP_COLOR = "#FF9B00"          # REG_START_PLATFORM = "App"
WEB_COLOR = "#8CA6FF"          # REG_START_PLATFORM = "Web"
FULL_CONSENT_COLOR = "#016853"
LIST_CONSENT_COLOR = "#9F651E"
NO_CONSENT_COLOR   = "#898989"
LINE_GRAY          = "#898989"
HEAT_LOW           = "#F1F1F1"
HEAT_HIGH          = "#00BE76"

CONSENT_COLORS = {
    "Full Consent": FULL_CONSENT_COLOR,
    "List Consent": LIST_CONSENT_COLOR,
    "No Consent":   NO_CONSENT_COLOR,
}

# ---------------------------------------------------------------------------
# Plotly theme (light + dark aware)
# ---------------------------------------------------------------------------
pio.templates["niche"] = go.layout.Template(
    layout=dict(
        font=dict(family="Inter, -apple-system, sans-serif", size=12, color="#1f2937"),
        title=dict(font=dict(family="Inter, sans-serif", size=15, color=NICHE_GREEN)),
        colorway=[NICHE_GREEN, NICHE_JADE, NICHE_YELLOW, NICHE_ORANGE_RED, NICHE_MID_GREEN, "#8CA6FF"],
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(gridcolor=NICHE_GREIGE, zerolinecolor="#E5E7EB"),
        yaxis=dict(gridcolor=NICHE_GREIGE, zerolinecolor="#E5E7EB"),
        hoverlabel=dict(bgcolor="white", bordercolor=NICHE_GREEN,
                        font=dict(family="Inter, sans-serif", size=12)),
    )
)
pio.templates.default = "niche"

try:
    _THEME_DARK = (st.get_option("theme.base") == "dark")
except Exception:
    _THEME_DARK = False

# ---------------------------------------------------------------------------
# Data load (live from Snowflake)
# ---------------------------------------------------------------------------
session = get_active_session()

SQL_REG_CONSENT = r"""
SELECT *
FROM NICHE_DATA_HUB.RAT.REG_CONSENT
"""


@st.cache_data(ttl=3600, show_spinner="Loading registration data…")
def load_reg_consent() -> pd.DataFrame:
    df = session.sql(SQL_REG_CONSENT).to_pandas()
    # Normalize column names (Snowflake returns uppercase)
    df.columns = [c.upper() for c in df.columns]
    if "ACCOUNT_CREATION_DATE" in df.columns:
        # Keep as pandas datetime64 (NOT .dt.date) so Plotly renders a true
        # time axis. Converting to python `date` makes Plotly treat it as
        # categorical, which can silently collapse sparse data into a
        # straight line between the first and last points.
        df["ACCOUNT_CREATION_DATE"] = pd.to_datetime(df["ACCOUNT_CREATION_DATE"])
    if "USER_ACCOUNTS" in df.columns:
        df["USER_ACCOUNTS"] = pd.to_numeric(df["USER_ACCOUNTS"], errors="coerce").fillna(0).astype(int)

    # Tableau calculated fields — resolved at row level so downstream
    # aggregations match the workbook exactly.
    df["FULL_REG"] = df.apply(
        lambda r: r["USER_ACCOUNTS"] if r.get("REG_STATUS") == "Full Registration" else 0,
        axis=1,
    )
    df["FULL_CONSENT"] = df.apply(
        lambda r: r["USER_ACCOUNTS"]
        if r.get("REG_STATUS") == "Full Registration" and r.get("CONSENT_STATUS") == "Full Consent"
        else 0,
        axis=1,
    )
    return df


# ---------------------------------------------------------------------------
# Filter application
# ---------------------------------------------------------------------------
def apply_filters(df: pd.DataFrame, *, user_types, consent_statuses, reg_platforms,
                  reg_statuses, grouped_reasons, app_os, page_path_contains,
                  date_start, date_end) -> pd.DataFrame:
    out = df
    if user_types:
        out = out[out["USER_TYPE"].isin(user_types)]
    if consent_statuses:
        out = out[out["CONSENT_STATUS"].isin(consent_statuses)]
    if reg_platforms:
        out = out[out["REG_START_PLATFORM"].isin(reg_platforms)]
    if reg_statuses:
        out = out[out["REG_STATUS"].isin(reg_statuses)]
    if grouped_reasons:
        out = out[out["GROUPED_REG_REASON"].isin(grouped_reasons)]
    if app_os:
        out = out[out["APP_OPERATING_SYSTEM"].isin(app_os)]
    if page_path_contains:
        q = page_path_contains.strip().lower()
        out = out[out["REG_PAGEPATH"].fillna("").str.lower().str.contains(q, na=False)]
    if date_start is not None:
        out = out[out["ACCOUNT_CREATION_DATE"] >= date_start]
    if date_end is not None:
        out = out[out["ACCOUNT_CREATION_DATE"] <= date_end]
    return out


# ---------------------------------------------------------------------------
# Aggregations (mirror Tableau's % Full Reg, % Full Consent)
# ---------------------------------------------------------------------------
def safe_pct(num, den) -> float:
    if den is None or den == 0 or pd.isna(den):
        return 0.0
    return float(num) / float(den)


def kpi_figures(df: pd.DataFrame) -> dict:
    accts = int(df["USER_ACCOUNTS"].sum())
    full_reg = int(df["FULL_REG"].sum())
    full_consent = int(df["FULL_CONSENT"].sum())

    # Platform splits — mirror College App Analytics' calc fields exactly.
    # Those calcs use strict equality: 'College App' → App bucket,
    # 'Website' → Web bucket. Any other values (e.g. bare 'Web' or 'App')
    # fall into neither bucket. Verified against the KPI - Accts (Web)
    # worksheet in the Tableau XML (twb line 3434).
    is_app = df["REG_START_PLATFORM"] == "College App"
    is_web = df["REG_START_PLATFORM"] == "Website"

    accts_app = int(df.loc[is_app, "USER_ACCOUNTS"].sum())
    accts_web = int(df.loc[is_web, "USER_ACCOUNTS"].sum())
    full_reg_app = int(df.loc[is_app, "FULL_REG"].sum())
    full_reg_web = int(df.loc[is_web, "FULL_REG"].sum())
    full_consent_app = int(df.loc[is_app, "FULL_CONSENT"].sum())
    full_consent_web = int(df.loc[is_web, "FULL_CONSENT"].sum())

    return {
        "accounts_created": accts,
        "full_reg": full_reg,
        "full_consent": full_consent,
        "pct_full_reg": safe_pct(full_reg, accts),
        "pct_full_consent": safe_pct(full_consent, full_reg),
        # Platform splits
        "accts_app": accts_app,
        "accts_web": accts_web,
        "full_reg_app": full_reg_app,
        "full_reg_web": full_reg_web,
        "full_consent_app": full_consent_app,
        "full_consent_web": full_consent_web,
        "pct_full_reg_app": safe_pct(full_reg_app, accts_app),
        "pct_full_reg_web": safe_pct(full_reg_web, accts_web),
        "pct_full_consent_app": safe_pct(full_consent_app, full_reg_app),
        "pct_full_consent_web": safe_pct(full_consent_web, full_reg_web),
    }


def fmt_pct(x: float) -> str:
    return f"{x * 100:.1f}%"


def fmt_int(x) -> str:
    try:
        return f"{int(x):,}"
    except Exception:
        return "—"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def _hex_to_rgb(h: str) -> tuple[int, int, int]:
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def to_period(series: pd.Series, granularity: str) -> pd.Series:
    """Truncate a datetime64 Series to the start of the chosen period."""
    s = pd.to_datetime(series)
    if granularity == "Weekly":
        # Week starting Monday (W-MON). `to_period` yields a PeriodIndex;
        # .start_time puts the label at the week's first day.
        return s.dt.to_period("W-MON").dt.start_time
    if granularity == "Monthly":
        return s.dt.to_period("M").dt.start_time
    return s.dt.floor("D")


def tickformat_for(granularity: str) -> str:
    return {"Daily": "%b %d, %Y", "Weekly": "%b %d, %Y", "Monthly": "%b %Y"}[granularity]


def green_heat_bg(pct: float) -> str:
    """Linear ramp HEAT_LOW -> HEAT_HIGH for pct in [0,1]."""
    if pct is None or pd.isna(pct):
        return ""
    t = max(0.0, min(1.0, float(pct)))
    r1, g1, b1 = _hex_to_rgb(HEAT_LOW)
    r2, g2, b2 = _hex_to_rgb(HEAT_HIGH)
    R = int(r1 + (r2 - r1) * t)
    G = int(g1 + (g2 - g1) * t)
    B = int(b1 + (b2 - b1) * t)
    lum = 0.299 * R + 0.587 * G + 0.114 * B
    fg = "#111" if lum > 170 else "#fff"
    return f"background-color: rgb({R},{G},{B}); color: {fg};"


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Reg Reason & Consent", layout="wide", page_icon="📱")

_DASHBOARD_CSS = f"""
<style>
  /* Hide the default sidebar */
  [data-testid="stSidebar"] {{ display: none; }}

  /* Hero banner */
  .niche-hero {{
      background: linear-gradient(90deg, {NICHE_GREEN} 0%, {NICHE_MID_GREEN} 100%);
      color: white; border-radius: 12px;
      padding: 18px 22px; margin: 2px 0 16px 0;
  }}
  .niche-hero h1 {{
      margin: 0; font-size: 24px; font-weight: 700; color: white;
  }}
  .niche-hero .sub {{
      margin-top: 4px; opacity: 0.88; font-size: 13px;
  }}

  /* KPI cards */
  .niche-kpi {{
      background: {NICHE_GREIGE};
      border-left: 5px solid {NICHE_GREEN};
      border-radius: 10px; padding: 14px 16px;
      box-shadow: 0 1px 2px rgba(0,0,0,0.05);
      height: 100%;
  }}
  .niche-kpi .label {{
      font-size: 12px; color: #374151; font-weight: 500;
      letter-spacing: 0.2px;
  }}
  .niche-kpi .value {{
      margin-top: 4px; font-size: 26px; font-weight: 700;
      color: {NICHE_GREEN}; font-family: Inter, -apple-system, sans-serif;
  }}
  .niche-kpi .sub {{
      font-size: 11px; color: #6b7280; margin-top: 2px;
  }}

  /* Section header */
  .niche-section {{
      color: {NICHE_GREEN};
      font-weight: 600; font-size: 15px;
      margin: 18px 0 8px 0;
      padding-left: 10px;
      border-left: 4px solid {NICHE_JADE};
  }}

  /* Data tables (mimic Tableau heat-table) */
  .niche-heat {{
      border-collapse: collapse; width: 100%; font-size: 12.5px;
      background: white; table-layout: fixed;
  }}
  .niche-heat th, .niche-heat td {{
      padding: 6px 10px; border: 1px solid #e5e7eb;
      background: white; color: #1f2937;
      text-align: right; font-variant-numeric: tabular-nums;
  }}
  .niche-heat th {{
      background: {NICHE_GREEN}; color: white;
      font-weight: 600; text-align: center; font-size: 12px;
      letter-spacing: 0.3px; position: sticky; top: 0;
  }}
  .niche-heat td.label {{
      text-align: left; font-weight: 500; color: {NICHE_GREEN};
      background: #FAFAF7;
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
      max-width: 0;
  }}
  .niche-heat tr:hover td {{ background: #F5F1EE; }}

  /* ── Dark-mode overrides ─────────────────────────────────────────── */
  @media (prefers-color-scheme: dark) {{
      .niche-kpi {{
          background: #1a2624; border-left-color: {NICHE_JADE};
      }}
      .niche-kpi .label {{ color: #9CA3AF; }}
      .niche-kpi .value {{ color: #7ADDB3; }}
      .niche-kpi .sub   {{ color: #9CA3AF; }}
      .niche-section {{ color: #7ADDB3; }}
      .niche-heat, .niche-heat th, .niche-heat td {{
          background: #0F1A17; color: #E7F4EE; border-color: #374151;
      }}
      .niche-heat th {{ background: #1a2624; color: #7ADDB3; }}
      .niche-heat td.label {{ background: #152422; color: #7ADDB3; }}
      .niche-heat tr:hover td {{ background: #16322A; }}
  }}
  [data-theme="dark"] .niche-kpi {{
      background: #1a2624; border-left-color: {NICHE_JADE};
  }}
  [data-theme="dark"] .niche-kpi .label {{ color: #9CA3AF; }}
  [data-theme="dark"] .niche-kpi .value {{ color: #7ADDB3; }}
  [data-theme="dark"] .niche-kpi .sub   {{ color: #9CA3AF; }}
  [data-theme="dark"] .niche-section {{ color: #7ADDB3; }}
  [data-theme="dark"] .niche-heat,
  [data-theme="dark"] .niche-heat th,
  [data-theme="dark"] .niche-heat td {{
      background: #0F1A17; color: #E7F4EE; border-color: #374151;
  }}
  [data-theme="dark"] .niche-heat th {{ background: #1a2624; color: #7ADDB3; }}
  [data-theme="dark"] .niche-heat td.label {{ background: #152422; color: #7ADDB3; }}
  [data-theme="dark"] .niche-heat tr:hover td {{ background: #16322A; }}
</style>
"""
st.markdown(_DASHBOARD_CSS, unsafe_allow_html=True)


def kpi_card(label: str, value: str, sub: str = ""):
    st.markdown(
        f'<div class="niche-kpi"><div class="label">{label}</div>'
        f'<div class="value">{value}</div>'
        + (f'<div class="sub">{sub}</div>' if sub else "")
        + "</div>",
        unsafe_allow_html=True,
    )


def section(title: str):
    st.markdown(f'<div class="niche-section">{title}</div>', unsafe_allow_html=True)


# ── Hero banner ───────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="niche-hero">
        <h1>App Analytics Tracking Dashboard</h1>
        <div class="sub">
            Tracking usage and engagement for the Niche App (v3.0) ·
            Live from <code>NICHE_DATA_HUB.RAT.REG_CONSENT</code>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


def render_readme():
    st.markdown(
        """
## Reg Reason & Consent

Streamlit-in-Snowflake rebuild of the Tableau **Reg Reason & Consent**
workbook (App Analytics Tracking Dashboard v3.0). Data is queried **live**
from Snowflake on every load and cached client-side for 1 hour via
`@st.cache_data(ttl=3600)`.

Source repo:
[github.com/allisonlazar-da/niche-dashboards](https://github.com/allisonlazar-da/niche-dashboards)
→ `reg_reason_consent/`.

### Data source

Single custom SQL — identical to the Tableau workbook:

```sql
SELECT *
FROM NICHE_DATA_HUB.RAT.REG_CONSENT
```

### Views (1:1 with Tableau — no views added, none removed)

| Section | Worksheet | Type |
|---|---|---|
| KPIs | Full Registrations | Text tile |
| KPIs | Fully-Consenting | Text tile |
| KPIs | Accts Created | Text tile |
| KPIs | % Full Reg CVR | Text tile |
| KPIs | % Full Consent CVR | Text tile |
| Row 1 | When are new user accounts created throughout the year? | Line |
| Row 1 | What prompts users to register? | Heat table (Grouped Reg Reason) |
| Row 2 | App vs. Web Trends — % Full Reg / % Full Consent | Line (toggled by `Plot Name`) |
| Row 2 | From which pages did users create their account? | Heat table (Reg Page Path) |
| Row 3 | What is the breakdown of consent status of fully-registered users? | Treemap |

### Calculated fields (verbatim from Tableau)

| Name | Formula |
|---|---|
| Full Reg | `IF REG_STATUS = 'Full Registration' THEN USER_ACCOUNTS ELSE NULL END` |
| Full Consent | `IF REG_STATUS = 'Full Registration' AND CONSENT_STATUS = 'Full Consent' THEN USER_ACCOUNTS ELSE NULL END` |
| % Full Reg | `SUM(Full Reg) / SUM(USER_ACCOUNTS)` |
| % Full Consent | `SUM(Full Consent) / SUM(Full Reg)` |

### Filters

**Global** (apply to every view, collapsed by default):
- **🎓 Match College App Analytics scope** — toggle that reconciles KPIs
  with the *College App Analytics* Tableau workbook by applying every
  row-level predicate that workbook bakes into its custom SQL:
  `USER_TYPE ∈ {hsstudent, adult, college, transfer}` (drops `other` + NULL) ·
  `REG_START_PLATFORM` excludes `Old App` + NULL ·
  `CONSENT_STATUS` / `REG_STATUS` / `APP_OPERATING_SYSTEM` exclude NULL ·
  `ACCOUNT_CREATION_DATE ∈ [2023-01-25, 2025-09-01]`. Leave OFF for the
  broader Reg Reason & Consent scope (this workbook's default).
- **User Type** — multiselect
- **Consent Status** — multiselect
- **Reg Start Platform** — multiselect (default excludes `Old App` + NULL, matching Tableau)
- **Grouped Reg Reason** — multiselect (default excludes `Undefined` + NULL, matching Tableau)
- **App Operating System** — multiselect
- **Reg Page Path** — substring search (case-insensitive)
- **Account Creation Date** — range picker (preset dropdown: Last 7/30/90 days, Last 6/12 months, MTD, QTD, YTD, All time, Custom)
- **Chart granularity** — Daily / Weekly / Monthly (applied to all time charts)

**Parameter** (Tableau `Plot Name`):
- Radio toggle: `Full Reg Trends` ↔ `Full Consent Trends` — switches which
  line chart renders in Row 2.

### Colors

Tableau workbook colors preserved exactly:
- App `#FF9B00` / Web `#8CA6FF`
- Full Consent `#016853` / List Consent `#9F651E` / No Consent `#898989`
- Heat-table gradient `#F1F1F1` → `#00BE76`

### Dark mode

CSS ships both `@media (prefers-color-scheme: dark)` and
`[data-theme="dark"]` overrides for KPI cards, section headers, and heat
tables. Plotly template switches to `plotly_dark` when Streamlit's base
theme is dark (Snowsight toggle or OS-level).

### Cache

The Snowflake query caches for 1 h (`@st.cache_data(ttl=3600)`). Force a
reload by hard-refreshing the browser (Cmd+Shift+R).

### Deploy

In-place code updates (fast, no full redeploy):

```
cd /Users/allisonlazar/Tableau/reg_reason_consent && \\
    snow sql -q "PUT file://$(pwd)/app_sis.py \\
        @DW_STREAMLIT_APPS.PUBLIC.reg_reason_consent_stage \\
        AUTO_COMPRESS=FALSE OVERWRITE=TRUE;" \\
        --database DW_STREAMLIT_APPS --schema PUBLIC
```

Full redeploy:

```
cd /Users/allisonlazar/Tableau/reg_reason_consent && \\
    snow streamlit deploy --database DW_STREAMLIT_APPS --schema PUBLIC --replace
```

### Required grants

`SELECT` on `NICHE_DATA_HUB.RAT.REG_CONSENT`. Plus `CREATE STREAMLIT`,
`CREATE STAGE` on `DW_STREAMLIT_APPS.PUBLIC` and `USAGE` on
`BUSINESS_USER_WH`.

### Release history

- **2026-04-23** — Added "🎓 Match College App Analytics scope" toggle
  so KPIs can reconcile 1:1 with the *College App Analytics* Tableau
  workbook. Added `App Operating System` filter. Added a second KPI row
  of platform splits (Accts / Full Reg / % Full Reg / % Full Consent ×
  App and Web) mirroring that workbook's platform-scoped calcs.
- **2026-04-20** — Initial commit: live-query pattern mirroring the
  `gtm_funnel` project. All 11 worksheets + 5 KPIs from the Tableau
  workbook rebuilt 1:1. Tableau parameter (`Plot Name`) exposed as a radio.
  Default filter exclusions preserved.
        """
    )


df_all = load_reg_consent()

# ── Filters row ───────────────────────────────────────────────────────────
# The original Reg Reason & Consent Tableau workbook hardcodes two
# datasource-level exclusions (GROUPED_REG_REASON ∉ {Undefined, NULL}
# and REG_START_PLATFORM ∉ {Old App, NULL}). The College App Analytics
# workbook has NEITHER. We expose these as a single checkbox so the
# user can flip between the two scopes cleanly.
use_rr_defaults = st.session_state.get("match_college_app_scope", False) is False
if use_rr_defaults:
    df_base = df_all[
        df_all["GROUPED_REG_REASON"].notna()
        & (df_all["GROUPED_REG_REASON"] != "Undefined")
        & df_all["REG_START_PLATFORM"].notna()
        & (df_all["REG_START_PLATFORM"] != "Old App")
    ].copy()
else:
    df_base = df_all.copy()

with st.expander("🔧 Filters", expanded=True):
    # ── College App Analytics scope — applies the preset by populating
    # the actual widget values so users can add/remove individual
    # members afterwards. The checkbox itself is a one-shot "apply"
    # trigger rather than a persistent gate.
    # User types pre-filtered at the SQL layer in the College App
    # workbook: USER_TYPE IN ('hsstudent','adult','college','transfer','other').
    # The per-worksheet USER_TYPE filter is `ui-enumeration='all'` —
    # keeps every value in the extract (so 'other' IS counted, despite
    # earlier reports to the contrary). Verified in twb line 3469-3471.
    cas_all_types = {"hsstudent", "adult", "college", "transfer", "other"}
    cas_user_types = sorted([
        t for t in df_base["USER_TYPE"].dropna().unique() if t in cas_all_types
    ])
    cas_consent = sorted([s for s in df_base["CONSENT_STATUS"].dropna().unique()])
    cas_reg_statuses = sorted([s for s in df_base["REG_STATUS"].dropna().unique()])
    cas_platforms = sorted([
        p for p in df_base["REG_START_PLATFORM"].dropna().unique() if p != "Old App"
    ])
    cas_app_os = sorted([s for s in df_base["APP_OPERATING_SYSTEM"].dropna().unique()])

    def _apply_college_app_scope():
        """Fires when the checkbox state flips to ON.

        Populates only the filters that the College App Analytics
        Tableau workbook hard-filters at the *SQL level*:
          - USER_TYPE ∈ {hsstudent, adult, college, transfer}
          - REG_START_PLATFORM != 'Old App'
          - ACCOUNT_CREATION_DATE ∈ [2023-01-25, 2025-09-01]

        Deliberately does NOT pre-populate Consent Status / App OS /
        Grouped Reg Reason / Reg Status — those are worksheet-level
        exclude-NULL filters in Tableau that only apply to *some*
        tiles. Auto-populating them here was dropping ~8% of Accts
        Created rows vs. Tableau's KPI tiles (rows where CONSENT_STATUS
        or APP_OPERATING_SYSTEM is NULL but a full-reg row exists)."""
        if not st.session_state.get("match_college_app_scope"):
            return
        st.session_state["flt_user_types"] = cas_user_types
        st.session_state["flt_reg_platforms"] = cas_platforms
        # Empirically, Tableau's Accts KPI tiles include rows where
        # CONSENT_STATUS / REG_STATUS / APP_OPERATING_SYSTEM are NULL
        # (Web users have NULL app_os and Tableau still counts them in
        # "Accounts Created on Web"). The worksheet XML declares
        # `except empty-level` filters on these columns, but they
        # don't actually exclude NULL rows in practice — leave these
        # multiselects empty so our numbers reconcile with Tableau.
        st.session_state["flt_consent_statuses"] = []
        st.session_state["flt_reg_statuses"] = []
        st.session_state["flt_app_os"] = []
        st.session_state["flt_grouped_reasons"] = []
        # Date preset → Custom, with the 2023-01-25 → 2025-09-01 window.
        # We store dates in our OWN slots (_date_from/_date_to), not
        # in the widget's session_state key — this avoids Streamlit's
        # "default value already set via session state" collisions.
        st.session_state["date_preset"] = "Custom"
        st.session_state["_date_from"] = max(dmin_global, date(2023, 1, 25))
        st.session_state["_date_to"]   = min(dmax_global, date(2025, 9, 1))

    # Pre-compute date bounds so the callback can use them
    dmin_global = df_base["ACCOUNT_CREATION_DATE"].min().date()
    dmax_global = df_base["ACCOUNT_CREATION_DATE"].max().date()

    match_college_app_scope = st.checkbox(
        "🎓 Apply College App Analytics scope",
        value=False,
        key="match_college_app_scope",
        on_change=_apply_college_app_scope,
        help=(
            "Populates the filter widgets with the College App "
            "Analytics workbook's scope so KPIs reconcile with that "
            "dashboard:\n"
            "• USER_TYPE = all non-NULL values (Tableau's KPI tiles "
            "don't restrict to the 4 hardcoded types — they count "
            "everything non-NULL)\n"
            "• REG_START_PLATFORM excludes 'Old App' + NULL\n"
            "• ACCOUNT_CREATION_DATE = 2023-01-25 → 2025-09-01\n\n"
            "Consent Status / App OS / Grouped Reg Reason are left "
            "empty — the College App workbook applies those NULL "
            "exclusions per worksheet rather than globally.\n\n"
            "Trim any of the pre-populated multiselects (e.g. remove "
            "'other' from User Type) to narrow further."
        ),
    )
    if match_college_app_scope:
        st.caption(
            "🎓 College App scope applied — edit any filter below to refine."
        )

    r1 = st.columns(3)
    with r1[0]:
        user_types = st.multiselect(
            "User Type",
            sorted(df_base["USER_TYPE"].dropna().unique()),
            key="flt_user_types",
        )
    with r1[1]:
        consent_statuses = st.multiselect(
            "Consent Status",
            sorted(df_base["CONSENT_STATUS"].dropna().unique()),
            key="flt_consent_statuses",
        )
    with r1[2]:
        reg_platforms = st.multiselect(
            "Reg Start Platform",
            sorted(df_base["REG_START_PLATFORM"].dropna().unique()),
            key="flt_reg_platforms",
        )

    r2 = st.columns([1, 1, 1, 1])
    with r2[0]:
        reg_statuses = st.multiselect(
            "Registration Status",
            sorted(df_base["REG_STATUS"].dropna().unique()),
            key="flt_reg_statuses",
        )
    with r2[1]:
        grouped_reasons = st.multiselect(
            "Grouped Reg Reason",
            sorted(df_base["GROUPED_REG_REASON"].dropna().unique()),
            key="flt_grouped_reasons",
        )
    with r2[2]:
        app_os = st.multiselect(
            "App Operating System",
            sorted(df_base["APP_OPERATING_SYSTEM"].dropna().unique()),
            key="flt_app_os",
        )
    with r2[3]:
        page_path_contains = st.text_input(
            "Reg Page Path (contains)", value="",
            help="Case-insensitive substring match",
            key="flt_page_path",
        )

    # Date preset dropdown (matches dms_campaign_management pattern).
    # dmin/dmax were pre-computed above as dmin_global/dmax_global so
    # the College-App-scope callback could use them.
    dmin, dmax = dmin_global, dmax_global

    def _clamp(d: date) -> date:
        return max(dmin, min(dmax, d))

    preset_options = ["Last 7 days", "Last 30 days", "Last 90 days",
                      "Last 6 months", "Last 12 months",
                      "Month to date", "Quarter to date", "Year to date",
                      "All time", "Custom"]
    # Initialize default once; after that session_state is authoritative.
    if "date_preset" not in st.session_state:
        st.session_state["date_preset"] = "All time"
    r3 = st.columns([1, 1, 1])
    with r3[0]:
        preset = st.selectbox(
            "📅 Account Creation — Quick range",
            preset_options,
            key="date_preset",
        )

    if preset == "Last 7 days":
        ps, pe = _clamp(dmax - timedelta(days=6)), dmax
    elif preset == "Last 30 days":
        ps, pe = _clamp(dmax - timedelta(days=29)), dmax
    elif preset == "Last 90 days":
        ps, pe = _clamp(dmax - timedelta(days=89)), dmax
    elif preset == "Last 6 months":
        first_of_month = date(dmax.year, dmax.month, 1)
        pe = first_of_month - timedelta(days=1)
        ym = pe.year * 12 + pe.month - 1 - 5
        ps = _clamp(date(ym // 12, ym % 12 + 1, 1))
        pe = _clamp(pe)
    elif preset == "Last 12 months":
        first_of_month = date(dmax.year, dmax.month, 1)
        pe = first_of_month - timedelta(days=1)
        ym = pe.year * 12 + pe.month - 1 - 11
        ps = _clamp(date(ym // 12, ym % 12 + 1, 1))
        pe = _clamp(pe)
    elif preset == "Month to date":
        ps, pe = _clamp(date(dmax.year, dmax.month, 1)), dmax
    elif preset == "Quarter to date":
        q_start_month = ((dmax.month - 1) // 3) * 3 + 1
        ps, pe = _clamp(date(dmax.year, q_start_month, 1)), dmax
    elif preset == "Year to date":
        ps, pe = _clamp(date(dmax.year, 1, 1)), dmax
    elif preset == "All time":
        ps, pe = dmin, dmax
    else:
        ps, pe = dmin, dmax

    custom = (preset == "Custom")

    def _coerce_date(v, fallback: date) -> date:
        """Normalize any shape (scalar date, datetime, list/tuple,
        Timestamp, stale out-of-range value) into a clamped scalar date."""
        if v is None:
            return fallback
        if isinstance(v, (list, tuple)):
            v = v[0] if v else fallback
        if hasattr(v, "date") and callable(getattr(v, "date", None)) and not isinstance(v, date):
            v = v.date()
        if not isinstance(v, date):
            return fallback
        return _clamp(v)

    # We store dates in our own session_state slots (_date_from/_date_to),
    # NOT the widget's key. This sidesteps every Streamlit rule about
    # "can't set widget value via session_state API + value= default".
    # The CAS callback writes to these slots; preset logic overrides
    # them for non-Custom. The widget uses `value=` only, no `key=`.
    if not custom:
        resolved_from = _clamp(ps)
        resolved_to   = _clamp(pe)
    else:
        resolved_from = _coerce_date(st.session_state.get("_date_from"), dmin)
        resolved_to   = _coerce_date(st.session_state.get("_date_to"), dmax)

    with r3[1]:
        d0 = st.date_input("Date from",
                           value=resolved_from,
                           min_value=dmin, max_value=dmax,
                           disabled=not custom)
    with r3[2]:
        d1 = st.date_input("Date to",
                           value=resolved_to,
                           min_value=dmin, max_value=dmax,
                           disabled=not custom)
    # Persist what the widget returned back into our slots.
    st.session_state["_date_from"] = _coerce_date(d0, dmin)
    st.session_state["_date_to"]   = _coerce_date(d1, dmax)
    if isinstance(d0, (list, tuple)):
        d0 = d0[0] if d0 else ps
    if isinstance(d1, (list, tuple)):
        d1 = d1[-1] if d1 else pe
    if not custom:
        d0, d1 = ps, pe
    st.caption(f"📅 {d0:%Y-%m-%d} → {d1:%Y-%m-%d} · {(d1 - d0).days + 1} days")

    date_start = pd.Timestamp(d0)
    date_end = pd.Timestamp(d1) + pd.Timedelta(hours=23, minutes=59, seconds=59)

    # Granularity + Tableau Plot Name parameter
    r4 = st.columns([1, 1])
    with r4[0]:
        granularity = st.radio(
            "⏱ Chart granularity",
            ["Daily", "Weekly", "Monthly"],
            horizontal=True,
            index=0,
            key="granularity",
            help="Controls the x-axis bucketing on every time-series chart.",
        )
    with r4[1]:
        plot_name = st.radio(
            "📈 Line Chart — Plot Name",
            options=["Full Reg Trends", "Full Consent Trends"],
            horizontal=True,
            help="Toggles the bottom-left line chart between % Full Reg and % Full Consent.",
        )

df_f = apply_filters(
    df_base,
    user_types=user_types,
    consent_statuses=consent_statuses,
    reg_platforms=reg_platforms,
    reg_statuses=reg_statuses,
    grouped_reasons=grouped_reasons,
    app_os=app_os,
    page_path_contains=page_path_contains,
    date_start=date_start,
    date_end=date_end,
)


if df_f.empty:
    st.warning("No rows match the current filters — relax a selection to see results.")
    _has_data = False
else:
    _has_data = True


# ── Tabs (created after setup so filters sit above the tab bar)
tab_readme, tab_dash, tab_trends = st.tabs(["📖 README", "📱 Dashboard", "📈 Trends"])

with tab_readme:
    render_readme()

# ─ Close the dashboard-tab setup block so the Trends tab can render
# below even when df_f is empty (which previously triggered st.stop()
# and orphaned the Trends widgets, causing a KeyError on next rerun).

if _has_data:
  with tab_dash:
    # ── KPI row ───────────────────────────────────────────────────────────────
    kpis = kpi_figures(df_f)

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        kpi_card("Full Registrations", fmt_int(kpis["full_reg"]), "on Niche platforms")
    with c2:
        kpi_card("Fully-Consenting", fmt_int(kpis["full_consent"]), "users who completed reg")
    with c3:
        kpi_card("Accts Created", fmt_int(kpis["accounts_created"]), "on Niche platforms")
    with c4:
        kpi_card("% Full Reg CVR", fmt_pct(kpis["pct_full_reg"]), "on Niche platforms")
    with c5:
        kpi_card("% Full Consent CVR", fmt_pct(kpis["pct_full_consent"]), "of fully-registered accts")

    # ── KPI row 2: Platform splits (App vs Web) ──────────────────────────────
    section("Platform splits — App vs Web")
    a1, a2, a3, a4 = st.columns(4)
    with a1:
        kpi_card("Accts Created · App", fmt_int(kpis["accts_app"]), "REG_START_PLATFORM = App")
    with a2:
        kpi_card("Full Reg · App", fmt_int(kpis["full_reg_app"]), "")
    with a3:
        kpi_card("% Full Reg · App", fmt_pct(kpis["pct_full_reg_app"]), "Full Reg / Accts (App)")
    with a4:
        kpi_card("% Full Consent · App", fmt_pct(kpis["pct_full_consent_app"]), "Full Consent / Full Reg (App)")

    w1, w2, w3, w4 = st.columns(4)
    with w1:
        kpi_card("Accts Created · Web", fmt_int(kpis["accts_web"]), "REG_START_PLATFORM = Web")
    with w2:
        kpi_card("Full Reg · Web", fmt_int(kpis["full_reg_web"]), "")
    with w3:
        kpi_card("% Full Reg · Web", fmt_pct(kpis["pct_full_reg_web"]), "Full Reg / Accts (Web)")
    with w4:
        kpi_card("% Full Consent · Web", fmt_pct(kpis["pct_full_consent_web"]), "Full Consent / Full Reg (Web)")

    # ── Row: LINE - Accounts Created   +   TABLE - Reg Reasons ────────────────
    left, right = st.columns([1, 1])

    with left:
        section(f"When are new user accounts created? · {granularity}")
        df_bucketed = df_f.copy()
        df_bucketed["PERIOD"] = to_period(df_bucketed["ACCOUNT_CREATION_DATE"], granularity)
        by_period = (
            df_bucketed.groupby("PERIOD", as_index=False)["USER_ACCOUNTS"]
            .sum()
            .sort_values("PERIOD")
        )
        fig = go.Figure()
        mode = "lines+markers" if len(by_period) <= 60 else "lines"
        hover_date_fmt = tickformat_for(granularity)
        fig.add_trace(go.Scatter(
            x=pd.to_datetime(by_period["PERIOD"]).tolist(),
            y=[float(v) for v in by_period["USER_ACCOUNTS"].tolist()],
            mode=mode,
            line=dict(color=LINE_GRAY, width=1.6),
            marker=dict(size=4, color=LINE_GRAY),
            name="Accts Created",
            hovertemplate=f"%{{x|{hover_date_fmt}}}<br># Accts: %{{y:,.0f}}<extra></extra>",
        ))
        fig.update_layout(
            height=360, margin=dict(l=10, r=10, t=20, b=10),
            xaxis=dict(title="Acct Creation Date", type="date",
                       tickformat={"Daily": "%b %d", "Weekly": "%b %d", "Monthly": "%b %Y"}[granularity]),
            yaxis=dict(title="# Accts", type="linear", rangemode="tozero"),
            template=("plotly_dark" if _THEME_DARK else "niche"),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"{len(by_period):,} {granularity.lower()} periods · total accounts: {int(by_period['USER_ACCOUNTS'].sum()):,}")

    with right:
        section("What prompts users to register?")
        tbl = (
            df_f.groupby("GROUPED_REG_REASON", as_index=False)
            .agg(accts=("USER_ACCOUNTS", "sum"),
                 full_reg=("FULL_REG", "sum"),
                 full_consent=("FULL_CONSENT", "sum"))
            .sort_values("accts", ascending=False)
        )
        tbl["pct_full_reg"] = tbl.apply(lambda r: safe_pct(r["full_reg"], r["accts"]), axis=1)
        tbl["pct_full_consent"] = tbl.apply(lambda r: safe_pct(r["full_consent"], r["full_reg"]), axis=1)

        rows_html = []
        for _, r in tbl.iterrows():
            label = str(r["GROUPED_REG_REASON"]) if pd.notna(r["GROUPED_REG_REASON"]) else "(null)"
            rows_html.append(
                f"<tr>"
                f"<td class='label' title='{label}'>{label}</td>"
                f"<td>{fmt_int(r['accts'])}</td>"
                f"<td>{fmt_int(r['full_reg'])}</td>"
                f"<td style=\"{green_heat_bg(r['pct_full_reg'])}\">{fmt_pct(r['pct_full_reg'])}</td>"
                f"<td>{fmt_int(r['full_consent'])}</td>"
                f"<td style=\"{green_heat_bg(r['pct_full_consent'])}\">{fmt_pct(r['pct_full_consent'])}</td>"
                f"</tr>"
            )
        st.markdown(
            "<div style='max-height:360px; overflow:auto;'>"
            "<table class='niche-heat'>"
            "<thead><tr>"
            "<th style='text-align:left; width:42%;'>Grouped Reg Reason</th>"
            "<th style='width:11.6%;'># Accts</th>"
            "<th style='width:11.6%;'># Full Reg</th>"
            "<th style='width:11.6%;'>% Full Reg</th>"
            "<th style='width:11.6%;'># Full Consent</th>"
            "<th style='width:11.6%;'>% Full Consent</th>"
            "</tr></thead><tbody>"
            + "".join(rows_html)
            + "</tbody></table></div>",
            unsafe_allow_html=True,
        )

    # ── Row: LINE - % Full Reg / % Full Consent   +   TABLE - Reg Page Path ───
    left, right = st.columns([1, 1])

    with left:
        # Parameter-driven line chart (App vs Web trends) — bucketed by granularity
        trend_src = df_f.copy()
        trend_src["PERIOD"] = to_period(trend_src["ACCOUNT_CREATION_DATE"], granularity)
        if plot_name == "Full Reg Trends":
            section(f"App vs. Web Trends — % Full Reg · {granularity}")
            trend = (
                trend_src.groupby(["PERIOD", "REG_START_PLATFORM"], as_index=False)
                .agg(full_reg=("FULL_REG", "sum"), accts=("USER_ACCOUNTS", "sum"))
            )
            trend["pct"] = trend.apply(lambda r: safe_pct(r["full_reg"], r["accts"]), axis=1)
            y_title = "% Full Reg"
        else:
            section(f"App vs. Web Trends — % Full Consent · {granularity}")
            trend = (
                trend_src.groupby(["PERIOD", "REG_START_PLATFORM"], as_index=False)
                .agg(full_consent=("FULL_CONSENT", "sum"), full_reg=("FULL_REG", "sum"))
            )
            trend["pct"] = trend.apply(lambda r: safe_pct(r["full_consent"], r["full_reg"]), axis=1)
            y_title = "% Full Consent"

        # Use whichever platform values actually exist in the filtered data
        # (Tableau hardcodes "App"/"Web" but the column casing varies by source).
        platform_colors = {
            "App": APP_COLOR, "Web": WEB_COLOR,
            "app": APP_COLOR, "web": WEB_COLOR,
            "APP": APP_COLOR, "WEB": WEB_COLOR,
        }
        fallback_palette = [APP_COLOR, WEB_COLOR, NICHE_JADE, NICHE_GREEN, NICHE_YELLOW]
        platforms = [p for p in trend["REG_START_PLATFORM"].dropna().unique()]

        fig = go.Figure()
        any_trace = False
        hover_date_fmt = tickformat_for(granularity)
        for i, plat in enumerate(platforms):
            color = platform_colors.get(plat, fallback_palette[i % len(fallback_palette)])
            sub = trend[trend["REG_START_PLATFORM"] == plat].sort_values("PERIOD")
            if sub.empty:
                continue
            any_trace = True
            mode = "lines+markers" if len(sub) <= 60 else "lines"
            fig.add_trace(go.Scatter(
                x=pd.to_datetime(sub["PERIOD"]).tolist(),
                y=[float(v) for v in sub["pct"].tolist()],
                mode=mode, name=plat,
                line=dict(color=color, width=1.8),
                marker=dict(size=4, color=color),
                hovertemplate=f"{plat}<br>%{{x|{hover_date_fmt}}}<br>{y_title}: %{{y:.1%}}<extra></extra>",
            ))
        fig.update_layout(
            height=360, margin=dict(l=10, r=10, t=20, b=10),
            xaxis=dict(title="Acct Creation Date", type="date",
                       tickformat={"Daily": "%b %d", "Weekly": "%b %d", "Monthly": "%b %Y"}[granularity]),
            yaxis=dict(title=y_title, type="linear", tickformat=".0%", range=[0, 1]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
            template=("plotly_dark" if _THEME_DARK else "niche"),
        )
        if not any_trace:
            found = ", ".join(str(p) for p in df_f["REG_START_PLATFORM"].dropna().unique()) or "(none)"
            st.info(f"No trend data after grouping. Platforms in filter: {found}")
        else:
            st.plotly_chart(fig, use_container_width=True)

    with right:
        section("From which pages did users create their account?")
        page_tbl = (
            df_f.groupby("REG_PAGEPATH", as_index=False)
            .agg(accts=("USER_ACCOUNTS", "sum"),
                 full_reg=("FULL_REG", "sum"),
                 full_consent=("FULL_CONSENT", "sum"))
            .sort_values("accts", ascending=False)
        )
        page_tbl["pct_full_reg"] = page_tbl.apply(lambda r: safe_pct(r["full_reg"], r["accts"]), axis=1)
        page_tbl["pct_full_consent"] = page_tbl.apply(lambda r: safe_pct(r["full_consent"], r["full_reg"]), axis=1)

        rows_html = []
        for _, r in page_tbl.iterrows():
            path = str(r["REG_PAGEPATH"]) if pd.notna(r["REG_PAGEPATH"]) else "(null)"
            # Escape single-quote for title attr, keep CSS ellipsis truncation
            path_esc = path.replace("'", "&#39;")
            rows_html.append(
                f"<tr>"
                f"<td class='label' title='{path_esc}'>{path}</td>"
                f"<td>{fmt_int(r['accts'])}</td>"
                f"<td>{fmt_int(r['full_reg'])}</td>"
                f"<td style=\"{green_heat_bg(r['pct_full_reg'])}\">{fmt_pct(r['pct_full_reg'])}</td>"
                f"<td>{fmt_int(r['full_consent'])}</td>"
                f"<td style=\"{green_heat_bg(r['pct_full_consent'])}\">{fmt_pct(r['pct_full_consent'])}</td>"
                f"</tr>"
            )
        st.markdown(
            "<div style='max-height:360px; overflow:auto;'>"
            "<table class='niche-heat'>"
            "<thead><tr>"
            "<th style='text-align:left; width:42%;'>Reg Page Path</th>"
            "<th style='width:11.6%;'># Accts</th>"
            "<th style='width:11.6%;'># Full Reg</th>"
            "<th style='width:11.6%;'>% Full Reg</th>"
            "<th style='width:11.6%;'># Full Consent</th>"
            "<th style='width:11.6%;'>% Full Consent</th>"
            "</tr></thead><tbody>"
            + "".join(rows_html)
            + "</tbody></table></div>",
            unsafe_allow_html=True,
        )
        st.caption("Hover any path to see the full URL.")

    # ── Tree: Consent Breakdown ───────────────────────────────────────────────
    section("What is the breakdown of consent status of fully-registered users?")

    df_fr = df_f[df_f["REG_STATUS"] == "Full Registration"].copy()
    if df_fr.empty:
        st.info("No fully-registered users in the current filter set.")
    else:
        tree = (
            df_fr.groupby("CONSENT_STATUS", as_index=False)["USER_ACCOUNTS"]
            .sum()
            .rename(columns={"USER_ACCOUNTS": "full_reg"})
            .sort_values("full_reg", ascending=False)
        )
        total_fr = tree["full_reg"].sum()
        tree["pct"] = tree["full_reg"] / total_fr if total_fr else 0

        labels = [
            f"<b>{row['CONSENT_STATUS']}</b><br>{row['pct']*100:.1f}%"
            for _, row in tree.iterrows()
        ]
        colors = [CONSENT_COLORS.get(s, NICHE_GREEN) for s in tree["CONSENT_STATUS"]]

        fig = go.Figure(go.Treemap(
            labels=[s for s in tree["CONSENT_STATUS"]],
            parents=[""] * len(tree),
            values=[float(v) for v in tree["full_reg"].tolist()],
            customdata=[[float(p)] for p in tree["pct"].tolist()],
            marker=dict(colors=colors, line=dict(width=1, color="white")),
            text=labels,
            textinfo="text",
            textfont=dict(family="Inter, sans-serif", size=14, color="white"),
            hovertemplate="<b>%{label}</b><br># Full Reg: %{value:,.0f}<br>% of Full Reg: %{customdata[0]:.1%}<extra></extra>",
            textposition="middle center",
        ))
        fig.update_layout(
            height=220, margin=dict(l=0, r=0, t=4, b=0),
            template=("plotly_dark" if _THEME_DARK else "niche"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── BAR: Consent rate by user type · App vs Web ──────────────────────────
    section("How does consent rate compare on App vs. Web?")
    st.caption(
        "Breakdown of % of fully-consenting registrations (i.e. prospects) "
        "by user type."
    )

    consent_by_type = (
        df_f.groupby(["USER_TYPE", "REG_START_PLATFORM"], as_index=False)
        .agg(full_reg=("FULL_REG", "sum"),
             full_consent=("FULL_CONSENT", "sum"))
    )
    consent_by_type["pct"] = consent_by_type.apply(
        lambda r: safe_pct(r["full_consent"], r["full_reg"]), axis=1
    )
    # Only keep App ('College App') and Web ('Website') rows — strict match,
    # same rule the other platform-split tiles use.
    consent_by_type = consent_by_type[
        consent_by_type["REG_START_PLATFORM"].isin(["College App", "Website"])
    ]

    if consent_by_type.empty:
        st.info("No fully-registered users in the current filter set.")
    else:
        # Map platform → bucket label + color (green for App, bronze for Web)
        consent_by_type["Bucket"] = consent_by_type["REG_START_PLATFORM"].map({
            "College App": "App",
            "Website":     "Web",
        })
        bucket_colors = {"App": FULL_CONSENT_COLOR, "Web": LIST_CONSENT_COLOR}

        user_types_sorted = sorted(consent_by_type["USER_TYPE"].dropna().unique())
        fig = go.Figure()
        for bucket in ["App", "Web"]:
            sub = consent_by_type[consent_by_type["Bucket"] == bucket]
            sub = sub.set_index("USER_TYPE").reindex(user_types_sorted)
            fig.add_trace(go.Bar(
                x=user_types_sorted,
                y=[float(v) if pd.notna(v) else None for v in sub["pct"].tolist()],
                name=bucket,
                marker=dict(color=bucket_colors[bucket]),
                text=[f"{v*100:.1f}%" if pd.notna(v) else "" for v in sub["pct"].tolist()],
                textposition="outside",
                textfont=dict(family="Inter, sans-serif", size=12),
                hovertemplate=(f"<b>{bucket}</b> · %{{x}}<br>"
                               f"% Full Consent: %{{y:.1%}}<br>"
                               f"# Full Reg: %{{customdata[0]:,.0f}}<br>"
                               f"# Full Consent: %{{customdata[1]:,.0f}}<extra></extra>"),
                customdata=[[float(r) if pd.notna(r) else 0,
                             float(c) if pd.notna(c) else 0]
                            for r, c in zip(sub["full_reg"].tolist(),
                                             sub["full_consent"].tolist())],
            ))
        fig.update_layout(
            height=420, margin=dict(l=10, r=10, t=30, b=10),
            barmode="group", bargap=0.15, bargroupgap=0.05,
            xaxis=dict(title="", type="category"),
            yaxis=dict(title="% Full Consent", tickformat=".1%", range=[0, 1.05]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
            template=("plotly_dark" if _THEME_DARK else "niche"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── TREE: Which user types complete full reg? ────────────────────────────
    section("Which user types complete full reg?")
    st.caption("Breakdown of full reg by user type (filter by platform on right).")

    ut_tree = (
        df_f.groupby("USER_TYPE", as_index=False)["FULL_REG"]
        .sum()
        .rename(columns={"FULL_REG": "full_reg"})
        .sort_values("full_reg", ascending=False)
    )
    ut_tree = ut_tree[ut_tree["full_reg"] > 0]

    if ut_tree.empty:
        st.info("No fully-registered users in the current filter set.")
    else:
        ut_total = ut_tree["full_reg"].sum()
        ut_tree["pct"] = ut_tree["full_reg"] / ut_total if ut_total else 0

        # Categorical palette matching the source Tableau chart's feel —
        # green / pink / orange / blue / extras.
        ut_palette = ["#4FB787", "#E5A6E0", "#E69342", "#8CA6FF",
                      NICHE_MID_GREEN, NICHE_YELLOW, NICHE_ORANGE_RED]
        ut_labels = [
            f"<b>{row['USER_TYPE']}</b><br>{row['pct'] * 100:.1f}%"
            for _, row in ut_tree.iterrows()
        ]

        fig = go.Figure(go.Treemap(
            labels=[str(u) if pd.notna(u) else "(null)" for u in ut_tree["USER_TYPE"]],
            parents=[""] * len(ut_tree),
            values=[float(v) for v in ut_tree["full_reg"].tolist()],
            customdata=[[float(p)] for p in ut_tree["pct"].tolist()],
            marker=dict(
                colors=[ut_palette[i % len(ut_palette)] for i in range(len(ut_tree))],
                line=dict(width=1, color="white"),
            ),
            text=ut_labels,
            textinfo="text",
            textfont=dict(family="Inter, sans-serif", size=14, color="#1f2937"),
            hovertemplate=("<b>%{label}</b><br>"
                           "# Full Reg: %{value:,.0f}<br>"
                           "% of Full Reg: %{customdata[0]:.1%}<extra></extra>"),
            textposition="top left",
        ))
        fig.update_layout(
            height=340, margin=dict(l=0, r=0, t=4, b=0),
            template=("plotly_dark" if _THEME_DARK else "niche"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Footer ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='margin-top:24px; color:#6b7280; font-size:11px; text-align:center;'>"
        f"Live from Snowflake · cached 1h · rebuilt 1:1 from Tableau workbook "
        f"<b>Reg Reason &amp; Consent</b></div>",
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Trends tab — dynamic metric × dimension × granularity explorer
# ──────────────────────────────────────────────────────────────────────────────
TREND_METRICS = {
    # label: (aggregation function over filtered df, y-tick format, hover format)
    "Accts Created":    ("USER_ACCOUNTS", "sum",  ",",    "%{y:,.0f}"),
    "Full Registrations": ("FULL_REG",    "sum",  ",",    "%{y:,.0f}"),
    "Fully-Consenting":   ("FULL_CONSENT","sum",  ",",    "%{y:,.0f}"),
    "% Full Reg CVR":     ("PCT_FULL_REG",    "rate", ".1%", "%{y:.1%}"),
    "% Full Consent CVR": ("PCT_FULL_CONSENT","rate", ".1%", "%{y:.1%}"),
}
TREND_DIMENSIONS = {
    "(none — single series)": None,
    "Reg Start Platform":      "REG_START_PLATFORM",
    "User Type":               "USER_TYPE",
    "Consent Status":          "CONSENT_STATUS",
    "Grouped Reg Reason":      "GROUPED_REG_REASON",
    "Reg Page Path":           "REG_PAGEPATH",
    "Reg Status":              "REG_STATUS",
    "App Operating System":    "APP_OPERATING_SYSTEM",
}


def compute_trend(df: pd.DataFrame, metric_col: str, kind: str,
                  granularity: str, dim_col: str | None) -> pd.DataFrame:
    """Group the filtered df into PERIOD × optional DIM, returning the
    requested metric. Rates are computed as ratios of summed components —
    not as averages of the per-row rate — so bucketed rates match the KPI
    tiles."""
    g = df.copy()
    g["PERIOD"] = to_period(g["ACCOUNT_CREATION_DATE"], granularity)
    group_cols = ["PERIOD"] + ([dim_col] if dim_col else [])
    agg = (g.groupby(group_cols, as_index=False)
            .agg(USER_ACCOUNTS=("USER_ACCOUNTS", "sum"),
                 FULL_REG=("FULL_REG", "sum"),
                 FULL_CONSENT=("FULL_CONSENT", "sum")))
    if kind == "sum":
        agg["VALUE"] = agg[metric_col]
    elif metric_col == "PCT_FULL_REG":
        agg["VALUE"] = agg.apply(lambda r: safe_pct(r["FULL_REG"], r["USER_ACCOUNTS"]), axis=1)
    elif metric_col == "PCT_FULL_CONSENT":
        agg["VALUE"] = agg.apply(lambda r: safe_pct(r["FULL_CONSENT"], r["FULL_REG"]), axis=1)
    return agg


with tab_trends:
    st.markdown("### 📈 Trends Explorer")
    st.caption(
        "Pick any metric, split it by any dimension, and change granularity "
        "or chart type. Inherits every filter from the Dashboard tab."
    )

    # Always render the widgets — gating on df_f.empty leaves orphaned
    # widget state that Streamlit's on_script_will_rerun bookkeeping
    # trips on (KeyError: '$$WIDGET_ID-…-trend_chart_type').
    tc = st.columns([1, 1, 1, 1])
    with tc[0]:
        metric_label = st.selectbox("Metric", list(TREND_METRICS.keys()),
                                    key="trend_metric", index=0)
    with tc[1]:
        split_label = st.selectbox("Split by", list(TREND_DIMENSIONS.keys()),
                                   key="trend_split_dim", index=1)
    with tc[2]:
        chart_type = st.selectbox(
            "Chart type",
            ["Line", "Stacked bar", "100% stacked bar"],
            key="trend_chart_type",
        )
    with tc[3]:
        _trend_gran_default = ["Daily", "Weekly", "Monthly"].index(granularity) \
            if granularity in ["Daily", "Weekly", "Monthly"] else 0
        trend_gran = st.radio("Granularity",
                              ["Daily", "Weekly", "Monthly", "All Time"],
                              horizontal=True,
                              index=_trend_gran_default,
                              key="trend_gran",
                              help="All Time collapses the time axis into a single composition donut by the selected dimension.")

    if df_f.empty:
        st.info("No rows match the current filters. Relax a selection on the Dashboard tab.")
    else:

        tc2 = st.columns([1, 1, 2])
        with tc2[0]:
            top_n = st.number_input("Top N series", min_value=3, max_value=20,
                                    value=8, step=1, key="trend_top_n",
                                    help="Limit number of series shown — keeps legend legible.")
        with tc2[1]:
            zero_y = st.checkbox("Start Y axis at 0", value=False, key="trend_zero",
                                 help="Anchor the y-axis at 0 (ignored for 100% stacked).")

        metric_col, kind, yfmt, hover_fmt = TREND_METRICS[metric_label]
        dim_col = TREND_DIMENSIONS[split_label]
        is_rate = kind == "rate"
        is_pct_stack = chart_type == "100% stacked bar"

        # Guardrails — don't let stacking misleadingly show rates.
        if chart_type != "Line" and is_rate:
            st.warning("Stacked bars only make sense for volume metrics "
                       "(Accts / Full Reg / Full Consent). Rates don't sum — "
                       "switching to Line.")
            chart_type = "Line"
            is_pct_stack = False
        if chart_type != "Line" and dim_col is None:
            st.info("Pick a Split by dimension to use a stacked-bar view.")
            chart_type = "Line"

        # ── All Time: single composition donut for the selected metric ──────
        if trend_gran == "All Time":
            if not dim_col:
                st.info("Pick a Split by dimension to see the All-Time composition.")
                st.stop()
            if is_rate:
                st.warning(
                    f"Rate metrics like {metric_label} don't sum, so a composition "
                    "donut isn't meaningful. Pick a volume metric "
                    "(Accts Created / Full Registrations / Fully-Consenting) to "
                    "see the All-Time breakdown."
                )
                st.stop()

            platform_colors_local = {"App": APP_COLOR, "Web": WEB_COLOR,
                                     "app": APP_COLOR, "web": WEB_COLOR}
            consent_colors_local = {"Full Consent": FULL_CONSENT_COLOR,
                                    "List Consent": LIST_CONSENT_COLOR,
                                    "No Consent":   NO_CONSENT_COLOR}

            def _color_list(names):
                if split_label == "Reg Start Platform":
                    return [platform_colors_local.get(n, NICHE_GREEN) for n in names]
                if split_label == "Consent Status":
                    return [consent_colors_local.get(n, NICHE_GREEN) for n in names]
                return None

            comp = (df_f.groupby(dim_col, dropna=False)[metric_col].sum()
                      .reset_index()
                      .sort_values(metric_col, ascending=False))
            # Apply top-N to keep donut legible
            comp = comp.head(int(top_n))
            comp[dim_col] = comp[dim_col].fillna("(null)").astype(str)
            names = comp[dim_col].tolist()
            values = [float(v) for v in comp[metric_col].tolist()]
            total = sum(values) or 1.0

            st.markdown(f"### {metric_label} by {split_label} · All Time")

            fig = go.Figure(go.Pie(
                labels=names, values=values, hole=0.45,
                marker=dict(colors=_color_list(names),
                            line=dict(color="white", width=1)),
                textinfo="label+percent",
                textposition="outside",
                hovertemplate=(f"%{{label}}<br>{metric_label}: %{{value:,.0f}}"
                               f"<br>%{{percent}}<extra></extra>"),
                sort=False,
            ))
            fig.update_layout(
                height=460, margin=dict(l=20, r=20, t=10, b=10),
                showlegend=False,
                template=("plotly_dark" if _THEME_DARK else "niche"),
            )
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("### Totals")
            disp = comp.rename(columns={dim_col: split_label,
                                        metric_col: metric_label})
            disp["Share"] = (disp[metric_label] / total * 100).map(lambda v: f"{v:.2f}%")
            disp[metric_label] = disp[metric_label].map(lambda v: f"{int(v):,}")
            st.dataframe(disp, use_container_width=True, height=360)

            st.download_button(
                "⬇ CSV",
                comp.rename(columns={dim_col: split_label,
                                     metric_col: metric_label}).to_csv(index=False).encode(),
                f"reg_consent_all_time_{metric_col.lower()}_by_{split_label.lower()}.csv",
                key="dl_alltime",
            )
            st.stop()

        g = compute_trend(df_f, metric_col, kind, trend_gran, dim_col)

        # Top-N limiting by total volume of the selected metric.
        if dim_col:
            top_series = (g.groupby(dim_col)["VALUE"].sum()
                          .sort_values(ascending=False).head(int(top_n)).index)
            g = g[g[dim_col].isin(top_series)]

        fig = go.Figure()
        tickformat_x = {"Daily": "%b %d", "Weekly": "%b %d", "Monthly": "%b %Y"}[trend_gran]
        hover_x = tickformat_for(trend_gran)

        if chart_type == "Line":
            if dim_col:
                series_names = sorted(g[dim_col].dropna().unique().tolist())
                platform_colors_local = {"App": APP_COLOR, "Web": WEB_COLOR,
                                         "app": APP_COLOR, "web": WEB_COLOR}
                consent_colors_local = {"Full Consent": FULL_CONSENT_COLOR,
                                        "List Consent": LIST_CONSENT_COLOR,
                                        "No Consent":   NO_CONSENT_COLOR}
                for name in series_names:
                    sub = g[g[dim_col] == name].sort_values("PERIOD")
                    color = None
                    if split_label == "Reg Start Platform":
                        color = platform_colors_local.get(name)
                    elif split_label == "Consent Status":
                        color = consent_colors_local.get(name)
                    fig.add_trace(go.Scatter(
                        x=pd.to_datetime(sub["PERIOD"]).tolist(),
                        y=[float(v) if pd.notna(v) else None for v in sub["VALUE"]],
                        mode="lines+markers" if len(sub) <= 60 else "lines",
                        name=str(name),
                        line=dict(width=2, color=color),
                        marker=dict(size=4, color=color),
                        hovertemplate=(f"<b>{name}</b><br>%{{x|{hover_x}}}<br>"
                                       f"{metric_label}: {hover_fmt}<extra></extra>"),
                    ))
            else:
                sub = g.sort_values("PERIOD")
                fig.add_trace(go.Scatter(
                    x=pd.to_datetime(sub["PERIOD"]).tolist(),
                    y=[float(v) if pd.notna(v) else None for v in sub["VALUE"]],
                    mode="lines+markers" if len(sub) <= 60 else "lines",
                    name=metric_label,
                    line=dict(color=NICHE_GREEN, width=2.5),
                    marker=dict(size=4, color=NICHE_GREEN),
                    hovertemplate=(f"%{{x|{hover_x}}}<br>"
                                   f"{metric_label}: {hover_fmt}<extra></extra>"),
                ))
        else:
            # Stacked / 100% stacked bars
            pivot = (g.pivot_table(index="PERIOD", columns=dim_col,
                                    values="VALUE", aggfunc="sum", fill_value=0)
                      .sort_index())
            if is_pct_stack:
                row_sum = pivot.sum(axis=1).replace(0, pd.NA)
                pivot = pivot.div(row_sum, axis=0) * 100
            for name in pivot.columns:
                fig.add_trace(go.Bar(
                    x=pd.to_datetime(pivot.index).tolist(),
                    y=[float(v) if pd.notna(v) else 0 for v in pivot[name].tolist()],
                    name=str(name),
                ))
            fig.update_layout(barmode="stack")
            if is_pct_stack:
                yfmt = ".0f"

        fig.update_layout(
            title=f"{metric_label} · {trend_gran}"
                  + (f" · split by {split_label}" if dim_col else "")
                  + (f" · {'100% stacked' if is_pct_stack else chart_type.lower()}"
                     if chart_type != "Line" else ""),
            height=460, margin=dict(l=10, r=10, t=50, b=10),
            xaxis=dict(type="date", tickformat=tickformat_x, tickangle=-30),
            yaxis=dict(
                type="linear",
                tickformat=yfmt,
                range=[0, 100] if is_pct_stack else None,
                ticksuffix="%" if is_pct_stack else None,
                rangemode=("tozero" if (zero_y and not is_pct_stack) else "normal"),
            ),
            legend=dict(orientation="h", y=-0.2),
            template=("plotly_dark" if _THEME_DARK else "niche"),
        )
        st.plotly_chart(fig, use_container_width=True)

        rows_suffix = f" · top {int(top_n)} {split_label.lower()}s" if dim_col else ""
        st.caption(f"{len(g):,} {trend_gran.lower()} rows · "
                   f"{pd.to_datetime(g['PERIOD']).min():%Y-%m-%d} → "
                   f"{pd.to_datetime(g['PERIOD']).max():%Y-%m-%d}{rows_suffix}")

        # Download filtered underlying data
        out = g.rename(columns={"VALUE": metric_label}).copy()
        st.download_button("⬇ CSV", out.to_csv(index=False).encode(),
                           f"reg_consent_trends_{trend_gran.lower()}.csv",
                           key="dl_trends")
