# Reg Reason & Consent — Streamlit in Snowflake

Rebuild of the Tableau **Reg Reason & Consent** workbook (App Analytics Tracking Dashboard v3.0) as a Streamlit-in-Snowflake app. Data is queried **live** from Snowflake on every load — no materialized tables, no scheduled refresh tasks. Results cached client-side for 1 hour via `@st.cache_data(ttl=3600)`.

Source repo: [github.com/allisonlazar-da/niche-dashboards](https://github.com/allisonlazar-da/niche-dashboards) → `reg_reason_consent/`.

## What's inside

```
reg_reason_consent/
├── app_sis.py              # Streamlit app (single-page dashboard) — SQL inlined as SQL_REG_CONSENT
├── sql/
│   └── reg_consent.sql     # Full Tableau custom SQL (source of truth — paste into app_sis.py)
├── environment.yml
├── snowflake.yml
└── README.md
```

## Views

Every worksheet from the original Tableau dashboard is reproduced 1:1 — no views added, none removed.

| Section | Worksheet | Type | Status |
|---|---|---|---|
| KPIs | Full Registrations | Text tile | Complete |
| KPIs | Fully-Consenting | Text tile | Complete |
| KPIs | Accts Created | Text tile | Complete |
| KPIs | % Full Reg CVR | Text tile | Complete |
| KPIs | % Full Consent CVR | Text tile | Complete |
| Row 1 | When are new user accounts created throughout the year? | Line | Complete |
| Row 1 | What prompts users to register? | Heat table (Grouped Reg Reason) | Complete |
| Row 2 | App vs. Web Trends — % Full Reg / % Full Consent | Line (toggled by `Plot Name`) | Complete |
| Row 2 | From which pages did users create their account? | Heat table (Reg Page Path) | Complete |
| Row 3 | What is the breakdown of consent status of fully-registered users? | Treemap | Complete |

## Calculated fields (verbatim from Tableau)

| Name | Formula |
|---|---|
| Full Reg | `IF REG_STATUS = 'Full Registration' THEN USER_ACCOUNTS ELSE NULL END` |
| Full Consent | `IF REG_STATUS = 'Full Registration' AND CONSENT_STATUS = 'Full Consent' THEN USER_ACCOUNTS ELSE NULL END` |
| % Full Reg | `SUM(Full Reg) / SUM(USER_ACCOUNTS)` |
| % Full Consent | `SUM(Full Consent) / SUM(Full Reg)` |

## Filters

**Global** (apply to every view, collapsed by default):
- User Type — multiselect
- Consent Status — multiselect
- Reg Start Platform — multiselect (default excludes `Old App` + NULL, matching Tableau)
- Grouped Reg Reason — multiselect (default excludes `Undefined` + NULL, matching Tableau)
- Reg Page Path — substring search (case-insensitive)
- Account Creation Date — range picker

**Parameter** (Tableau `Plot Name`):
- Radio toggle: `Full Reg Trends` ↔ `Full Consent Trends` — switches which line chart renders in Row 2.

## Live-query viability

Query: `SELECT * FROM NICHE_DATA_HUB.RAT.REG_CONSENT`

Cold app load: a few seconds on `BUSINESS_USER_WH`. Subsequent filter changes: instant (in-memory on the pandas DataFrame).

If this becomes sluggish, convert to the Marketing-KPIs materialized pattern: add `CREATE OR REPLACE TABLE DW_STREAMLIT_APPS.PUBLIC.REG_CONSENT_APP AS ...`, swap `session.sql(SQL_REG_CONSENT)` for `session.table(...)`, and schedule a refresh task.

## Deploy to Streamlit in Snowflake

```bash
cd /Users/allisonlazar/Tableau/reg_reason_consent
snow streamlit deploy --database DW_STREAMLIT_APPS --schema PUBLIC --replace
```

URL prints at the end. Hard-refresh (`Cmd+Shift+R`) after redeploy — SiS aggressively caches the app bundle.

### Required grants

`SELECT` on:
```
NICHE_DATA_HUB.RAT.REG_CONSENT
```

Plus `CREATE STREAMLIT, CREATE STAGE` on `DW_STREAMLIT_APPS.PUBLIC` and `USAGE` on `BUSINESS_USER_WH`.

## Dark mode

CSS in `app_sis.py` ships both `@media (prefers-color-scheme: dark)` and `[data-theme="dark"]` overrides for KPI cards, section headers, and heat tables. The Plotly template switches to `plotly_dark` when Streamlit's base theme is dark (Snowsight toggle or OS-level).

## Release history

- **2026-04-20** — Initial commit: live-query pattern mirroring the `gtm_funnel` project. All 11 worksheets + 5 KPIs from the Tableau workbook rebuilt 1:1. Tableau parameter (`Plot Name`) exposed as a radio. Default filter exclusions (`GROUPED_REG_REASON ∉ {Undefined, NULL}`, `REG_START_PLATFORM ∉ {Old App, NULL}`) preserved.
