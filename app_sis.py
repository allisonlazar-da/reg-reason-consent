"""Reg Reason & Consent — Streamlit-in-Snowflake rebuild of the Tableau workbook.

Source workbook: "Reg Reason & Consent" (Niche App Analytics Tracking
Dashboard v3.0). Every worksheet and filter from the original Tableau
dashboard is reproduced 1:1 here.

Data is pulled live on every page load from
`NICHE_DATA_HUB.RAT.REG_CONSENT` (cached client-side for 1 hour).
"""
from __future__ import annotations

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
                  grouped_reasons, page_path_contains, date_start, date_end) -> pd.DataFrame:
    out = df
    if user_types:
        out = out[out["USER_TYPE"].isin(user_types)]
    if consent_statuses:
        out = out[out["CONSENT_STATUS"].isin(consent_statuses)]
    if reg_platforms:
        out = out[out["REG_START_PLATFORM"].isin(reg_platforms)]
    if grouped_reasons:
        out = out[out["GROUPED_REG_REASON"].isin(grouped_reasons)]
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
    return {
        "accounts_created": accts,
        "full_reg": full_reg,
        "full_consent": full_consent,
        "pct_full_reg": safe_pct(full_reg, accts),
        "pct_full_consent": safe_pct(full_consent, full_reg),
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

tab_readme, tab_dash = st.tabs(["📖 README", "📱 Dashboard"])


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
- **User Type** — multiselect
- **Consent Status** — multiselect
- **Reg Start Platform** — multiselect (default excludes `Old App` + NULL, matching Tableau)
- **Grouped Reg Reason** — multiselect (default excludes `Undefined` + NULL, matching Tableau)
- **Reg Page Path** — substring search (case-insensitive)
- **Account Creation Date** — range picker

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

- **2026-04-20** — Initial commit: live-query pattern mirroring the
  `gtm_funnel` project. All 11 worksheets + 5 KPIs from the Tableau
  workbook rebuilt 1:1. Tableau parameter (`Plot Name`) exposed as a radio.
  Default filter exclusions preserved.
        """
    )


with tab_readme:
    render_readme()

with tab_dash:
    df_all = load_reg_consent()

    # ── Filters row ───────────────────────────────────────────────────────────
    # Apply Tableau's default exclusions first (so they show up as the "base"
    # population and the filter widgets list only the remaining members).
    df_base = df_all[
        df_all["GROUPED_REG_REASON"].notna()
        & (df_all["GROUPED_REG_REASON"] != "Undefined")
        & df_all["REG_START_PLATFORM"].notna()
        & (df_all["REG_START_PLATFORM"] != "Old App")
    ].copy()

    with st.expander("🔧 Filters", expanded=True):
        r1 = st.columns(3)
        with r1[0]:
            user_types = st.multiselect(
                "User Type",
                sorted(df_base["USER_TYPE"].dropna().unique()),
            )
        with r1[1]:
            consent_statuses = st.multiselect(
                "Consent Status",
                sorted(df_base["CONSENT_STATUS"].dropna().unique()),
            )
        with r1[2]:
            reg_platforms = st.multiselect(
                "Reg Start Platform",
                sorted(df_base["REG_START_PLATFORM"].dropna().unique()),
            )

        r2 = st.columns([1, 1, 1])
        with r2[0]:
            grouped_reasons = st.multiselect(
                "Grouped Reg Reason",
                sorted(df_base["GROUPED_REG_REASON"].dropna().unique()),
            )
        with r2[1]:
            page_path_contains = st.text_input(
                "Reg Page Path (contains)", value="",
                help="Case-insensitive substring match",
            )
        with r2[2]:
            dmin = df_base["ACCOUNT_CREATION_DATE"].min().date()
            dmax = df_base["ACCOUNT_CREATION_DATE"].max().date()
            date_range = st.date_input(
                "Account Creation Date",
                value=(dmin, dmax),
                min_value=dmin, max_value=dmax,
            )
            if isinstance(date_range, tuple) and len(date_range) == 2:
                date_start, date_end = date_range
            else:
                date_start, date_end = dmin, dmax
            # Back to Timestamps so filter comparison matches the datetime64 column
            date_start = pd.Timestamp(date_start)
            date_end = pd.Timestamp(date_end) + pd.Timedelta(hours=23, minutes=59, seconds=59)

        # Tableau parameter: Plot Name
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
        grouped_reasons=grouped_reasons,
        page_path_contains=page_path_contains,
        date_start=date_start,
        date_end=date_end,
    )

    if df_f.empty:
        st.warning("No rows match the current filters — relax a selection to see results.")
        st.stop()

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

    # ── Row: LINE - Accounts Created   +   TABLE - Reg Reasons ────────────────
    left, right = st.columns([1, 1])

    with left:
        section("When are new user accounts created throughout the year?")
        by_day = (
            df_f.groupby("ACCOUNT_CREATION_DATE", as_index=False)["USER_ACCOUNTS"]
            .sum()
            .sort_values("ACCOUNT_CREATION_DATE")
        )
        fig = go.Figure()
        mode = "lines+markers" if len(by_day) <= 60 else "lines"
        fig.add_trace(go.Scatter(
            x=pd.to_datetime(by_day["ACCOUNT_CREATION_DATE"]).tolist(),
            y=[float(v) for v in by_day["USER_ACCOUNTS"].tolist()],
            mode=mode,
            line=dict(color=LINE_GRAY, width=1.6),
            marker=dict(size=4, color=LINE_GRAY),
            name="Accts Created",
            hovertemplate="%{x|%b %d, %Y}<br># Accts: %{y:,.0f}<extra></extra>",
        ))
        fig.update_layout(
            height=360, margin=dict(l=10, r=10, t=20, b=10),
            xaxis=dict(title="Acct Creation Date", type="date"),
            yaxis=dict(title="# Accts", type="linear", rangemode="tozero"),
            template=("plotly_dark" if _THEME_DARK else "niche"),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"{len(by_day):,} unique dates · total accounts: {int(by_day['USER_ACCOUNTS'].sum()):,}")

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
        # Parameter-driven line chart (App vs Web trends)
        if plot_name == "Full Reg Trends":
            section("App vs. Web Trends — % Full Reg")
            trend = (
                df_f.groupby(["ACCOUNT_CREATION_DATE", "REG_START_PLATFORM"], as_index=False)
                .agg(full_reg=("FULL_REG", "sum"), accts=("USER_ACCOUNTS", "sum"))
            )
            trend["pct"] = trend.apply(lambda r: safe_pct(r["full_reg"], r["accts"]), axis=1)
            y_title = "% Full Reg"
        else:
            section("App vs. Web Trends — % Full Consent")
            trend = (
                df_f.groupby(["ACCOUNT_CREATION_DATE", "REG_START_PLATFORM"], as_index=False)
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
        for i, plat in enumerate(platforms):
            color = platform_colors.get(plat, fallback_palette[i % len(fallback_palette)])
            sub = trend[trend["REG_START_PLATFORM"] == plat].sort_values("ACCOUNT_CREATION_DATE")
            if sub.empty:
                continue
            any_trace = True
            mode = "lines+markers" if len(sub) <= 60 else "lines"
            fig.add_trace(go.Scatter(
                x=pd.to_datetime(sub["ACCOUNT_CREATION_DATE"]).tolist(),
                y=[float(v) for v in sub["pct"].tolist()],
                mode=mode, name=plat,
                line=dict(color=color, width=1.8),
                marker=dict(size=4, color=color),
                hovertemplate=f"{plat}<br>%{{x|%b %d, %Y}}<br>{y_title}: %{{y:.1%}}<extra></extra>",
            ))
        fig.update_layout(
            height=360, margin=dict(l=10, r=10, t=20, b=10),
            xaxis=dict(title="Acct Creation Date", type="date"),
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

    # ── Footer ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='margin-top:24px; color:#6b7280; font-size:11px; text-align:center;'>"
        f"Live from Snowflake · cached 1h · rebuilt 1:1 from Tableau workbook "
        f"<b>Reg Reason &amp; Consent</b></div>",
        unsafe_allow_html=True,
    )
