# Power BI Dashboard Specification
## Customer 360 Banking Analytics — Azure Databricks Medallion Lakehouse

---

## Connection Setup

**Connection type:** DirectQuery → Snowflake
**Server:** `<your-account>.snowflakecomputing.com`
**Database:** `CUSTOMER360_DB`
**Schema:** `GOLD`
**Warehouse:** `CUSTOMER360_WH` (auto-suspend 60s)
**Authentication:** Username + Password (service account via Azure Key Vault)

**Tables / Views to import:**
- `VW_EXECUTIVE_KPIS`
- `VW_SEGMENT_BREAKDOWN`
- `VW_GEOGRAPHIC_DISTRIBUTION`
- `VW_CHURN_RISK_DETAIL`
- `VW_MONTHLY_TXN_TREND`
- `VW_CROSSSELL_OPPORTUNITIES`
- `VW_SUPPORT_OPERATIONS`

---

## Dashboard Pages

### Page 1 — Executive Overview

**Purpose:** C-suite summary. One glance = full health of the portfolio.

**Visuals:**

| Visual | Type | Source field(s) | Config |
|---|---|---|---|
| Total AUM | KPI Card | `aum_billions` | Format: $B, compare to prior month |
| Total customers | KPI Card | `total_customers` | No comparison needed |
| Avg credit score | KPI Card | `avg_credit_score` | Color: Green if >720, Amber if >660 |
| Churn risk % | KPI Card | `churn_risk_pct` | Color: Red if >15%, Amber if >10% |
| AUM by segment | Donut chart | `VW_SEGMENT_BREAKDOWN.segment`, `aum_millions` | Legend right, % labels |
| Customers by segment | Bar chart | `segment`, `customer_count` | Horizontal, sorted desc |
| AUM by state | Filled map | `state`, `aum_millions` | US map, blue color scale |
| Monthly spend trend | Line chart | `VW_MONTHLY_TXN_TREND.period`, `total_spend_millions` | Show last 12 months |

**Filters panel:** Segment slicer, State slicer, Date range

---

### Page 2 — Customer 360 Explorer

**Purpose:** Relationship manager tool — look up any customer, see full profile.

**Visuals:**

| Visual | Type | Config |
|---|---|---|
| Customer search | Search slicer on `customer_name` | Single select |
| Customer profile card | Multi-row card | Name, segment, age_band, state, KYC status, preferred channel |
| Balance & spend | KPI cards (2) | total_balance, spend_12m |
| Credit profile | Gauge | credit_score, min=300, max=850, target=720 |
| Products held | Text/Tag visual | product_types_held (comma list) |
| Monthly spend line | Line chart | period, total_debit per customer | filter by selected customer |
| Churn indicators | Conditional card | days_since_last_txn, avg_csat_score, escalated_tickets — red if flagged |
| Ticket history | Table | category, status, created_date, csat_score, resolution_days |

---

### Page 3 — Churn Risk Management

**Purpose:** Retention team — identify, prioritize, act on at-risk customers.

**Visuals:**

| Visual | Type | Config |
|---|---|---|
| Total at-risk customers | KPI card | COUNT from VW_CHURN_RISK_DETAIL |
| Estimated AUM at risk | KPI card | SUM(total_balance) where churn_risk |
| Priority breakdown | Stacked bar | churn_priority × segment | Color: Critical=Red, High=Amber, Medium=Yellow |
| At-risk by state | Map | state, customer count, AUM at risk | Bubble size = AUM |
| Churn risk table | Table | customer_name, segment, total_balance, days_since_last_txn, csat_score, recommended_action | Conditional formatting on days_since_last_txn |
| CSAT distribution | Histogram | avg_csat_score (0–5) | Binned 0.5 intervals |
| Days inactive distribution | Bar | days_since_last_txn | Bins: 0-30, 30-90, 90-180, 180+ |

**Filters:** Segment, Priority level, State, Balance tier

---

### Page 4 — Transaction Trends

**Purpose:** Finance & Risk — understand spend patterns, detect anomalies.

**Visuals:**

| Visual | Type | Config |
|---|---|---|
| MoM spend change | KPI Card | spend_mom_pct_change — green if positive, red if negative |
| Monthly spend vs deposits | Dual-axis line chart | period × total_spend_millions + total_deposits_millions |
| Net cash flow trend | Area chart | period × net_flow_millions — positive = green, negative = red |
| Avg transaction size | Line chart | period × avg_txn_size |
| Flagged transactions | Bar chart | period × flagged_txns | Conditional: red if above average |
| Active customers trend | Line chart | period × active_customers |
| Spend by channel | Donut | channel × sum of debit amount |

---

### Page 5 — Operations & Support

**Purpose:** Customer experience team — monitor SLA, CSAT, ticket volumes.

**Visuals:**

| Visual | Type | Config |
|---|---|---|
| Overall CSAT | Gauge | portfolio_avg_csat, target = 4.0 |
| Avg resolution time | KPI Card | avg_days_to_resolve — red if > 5 |
| Resolution rate | KPI Card | resolution_rate_pct |
| Escalation rate | KPI Card | escalation_rate_pct |
| Tickets by category | Horizontal bar | category × total_tickets — sorted desc |
| Resolution time by category | Bar chart | category × avg_days_to_resolve — sorted desc, red threshold at 7 days |
| CSAT by priority | Matrix | priority × category × avg_csat_score — heatmap conditional formatting |
| Ticket volume trend | Line chart | created_date × ticket_count | Rolling 30-day |

---

## DAX Measures to create in Power BI

```dax
-- AUM in billions formatted
AUM Billions = FORMAT(SUM(VW_EXECUTIVE_KPIS[aum_billions]), "$#,##0.0B")

-- Churn risk indicator color
Churn Color =
    IF(SELECTEDVALUE(VW_EXECUTIVE_KPIS[churn_risk_pct]) > 15, "#E24B4A",
    IF(SELECTEDVALUE(VW_EXECUTIVE_KPIS[churn_risk_pct]) > 10, "#EF9F27", "#1D9E75"))

-- Month-over-month label
MoM Label =
    VAR pct = SELECTEDVALUE(VW_MONTHLY_TXN_TREND[spend_mom_pct_change])
    RETURN IF(pct > 0, "+" & FORMAT(pct, "0.0%"), FORMAT(pct, "0.0%"))

-- Credit score band for conditional formatting
Credit Score Color =
    VAR score = SELECTEDVALUE(CUSTOMER_360[credit_score])
    RETURN IF(score >= 740, "#1D9E75", IF(score >= 670, "#EF9F27", "#E24B4A"))
```

---

## Screenshots to include in GitHub repo

1. `docs/screenshots/pbi_executive_overview.png` — Page 1 full screenshot
2. `docs/screenshots/pbi_churn_risk.png` — Churn risk table with conditional formatting
3. `docs/screenshots/pbi_monthly_trend.png` — Spend trend line chart
4. `docs/screenshots/pbi_customer_explorer.png` — Single customer 360 view

**How to generate screenshots without real Databricks/Snowflake:**
- Use the synthetic CSV data directly in Power BI Desktop (File → Get Data → CSV)
- Import `data/synthetic/customers.csv`, `transactions.csv`, etc.
- Build the visuals against the CSV data — screenshots look identical to production

---

## Interview talking point for Power BI

*"Power BI connects via DirectQuery to Snowflake — that means queries hit Snowflake
at report open time, not a cached import. The Snowflake views pre-join the tables so
Power BI only fires simple SELECT queries, not complex joins at report time. I cluster
the Snowflake tables on segment + state — the same columns the report slicers filter on —
so every DirectQuery hits a data-skipping path. The warehouse auto-suspends in 60 seconds
so the BI layer costs almost nothing between daily refreshes."*
