# ncaa-cbb-analytics

An end-to-end analytics engineering pipeline for NCAA college basketball, built on the modern data stack. Ingests data daily from KenPom and BartTorvik, transforms it through a dbt pipeline in Snowflake, and surfaces it through an interactive Streamlit dashboard.

**[View Live Dashboard →](https://your-app.streamlit.app)**

---

## Overview

This project demonstrates a production-grade analytics engineering workflow applied to a real-world domain. The pipeline runs automatically every morning, pulling the latest team efficiency ratings and player stats, rebuilding all dbt models, and making fresh data available in the dashboard — with no manual intervention.

The analytical focus is NCAA tournament profiling: identifying which teams have the efficiency margins, strength of schedule, and player quality to succeed in March.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        GitHub Actions                           │
│                    Daily @ 8am EST (cron)                       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┴────────────────┐
          │                                 │
          ▼                                 ▼
  ┌───────────────┐                ┌────────────────┐
  │    KenPom     │                │  BartTorvik    │
  │  (kenpompy)   │                │  (direct CSV)  │
  └───────┬───────┘                └───────┬────────┘
          │                                │
          └────────────────┬───────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │    Snowflake    │
                  │   RAW Schema   │
                  └───────┬─────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │      dbt        │
                  │ Staging → Marts │
                  └───────┬─────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │   Streamlit     │
                  │   Dashboard     │
                  └─────────────────┘
```

---

## Data Sources

| Source | Data | Access |
|--------|------|--------|
| [KenPom](https://kenpom.com) | Adjusted offensive/defensive efficiency, tempo, strength of schedule | Subscription via `kenpompy` |
| [BartTorvik](https://barttorvik.com) | Team results, Barthag, wins above bubble, player stats | Free CSV download |

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Ingestion | Python, pandas, kenpompy |
| Warehouse | Snowflake |
| Transformation | dbt Core (dbt-snowflake) |
| Orchestration | GitHub Actions |
| Visualization | Streamlit, Plotly |
| Version Control | Git / GitHub |

---

## Project Structure

```
ncaa-cbb-analytics/
├── ingestion/
│   ├── kenpom/
│   │   └── extract_kenpom.py       # KenPom ingestion via kenpompy
│   └── barttorvik/
│       └── extract_torvik.py       # BartTorvik team + player ingestion
│
├── dbt/
│   └── cbb_analytics/
│       ├── dbt_project.yml
│       ├── packages.yml
│       └── models/
│           ├── staging/
│           │   ├── sources.yml
│           │   ├── kenpom/
│           │   │   ├── stg_kenpom__team_ratings.sql
│           │   │   └── kenpom.yml
│           │   └── torvik/
│           │       ├── stg_torvik__team_results.sql
│           │       ├── stg_torvik__player_stats.sql
│           │       └── torvik.yml
│           └── marts/
│               ├── dim_teams.sql
│               ├── fct_team_ratings.sql
│               ├── fct_player_stats.sql
│               ├── fct_tournament_profile.sql
│               └── marts.yml
│
├── app.py                          # Streamlit dashboard
├── requirements.txt
├── .github/
│   └── workflows/
│       └── daily_ingest.yml        # Scheduled pipeline
└── .env.example                    # Credential template
```

---

## dbt Models

### Staging Layer
One model per source table. Renames columns to clean snake_case, casts types, and filters bad rows. No joins happen in staging.

| Model | Description |
|-------|-------------|
| `stg_kenpom__team_ratings` | KenPom team efficiency metrics — tempo, AdjO, AdjD |
| `stg_torvik__team_results` | BartTorvik team results — Barthag, WAB, SOS, quality metrics |
| `stg_torvik__player_stats` | BartTorvik player stats — 40+ metrics per player per season |

### Marts Layer
Business logic, joins, and aggregations. These are the tables exposed to the dashboard.

| Model | Description |
|-------|-------------|
| `dim_teams` | Canonical team dimension resolving name differences between KenPom and BartTorvik |
| `fct_team_ratings` | Combined KenPom + BartTorvik metrics per team — core analytical table |
| `fct_player_stats` | Player stats enriched with team context |
| `fct_tournament_profile` | Master table combining team efficiency, SOS, WAB, and starting five player averages |

---

## Key Design Decisions

**Canonical team dimension**
KenPom and BartTorvik use different team names (e.g. "CSUN" vs "Cal St. Northridge"). `dim_teams` resolves 6 naming inconsistencies with a `CASE` statement so downstream models join cleanly without dropping rows.

**Official column headers**
BartTorvik's player stats CSV ships without headers. Rather than relying on fragile positional column mapping, the ingestion script assigns the official column names sourced directly from BartTorvik's published header reference file — making the pipeline resilient to future column additions.

**Starting five aggregation**
`fct_tournament_profile` aggregates player stats across each team's top 5 players by minutes percentage (minimum 10 games) rather than relying on a single player metric. This includes an experience index counting seniors and graduate students — a historically predictive tournament factor.

**Consensus efficiency margin**
Rather than choosing one model over the other, `fct_tournament_profile` averages KenPom and BartTorvik adjusted efficiency margins into a single `consensus_adj_em` metric. Where the two models strongly disagree the source comparison chart in the dashboard surfaces the gap.

---

## Dashboard

The Streamlit dashboard queries `fct_tournament_profile` directly from Snowflake and refreshes every hour.

**Tabs:**
- **Efficiency Rankings** — Bar chart of top teams by consensus AdjEM, color-encoded by conference. Includes a barbell chart comparing KenPom vs BartTorvik rankings with disagreement callouts.
- **Offense vs Defense** — Scatter plot of adjusted offensive vs defensive efficiency with quadrant labels and a diagonal line separating elite two-way teams.
- **Starting Five** — Sortable table of team and player metrics aggregated across each team's top 5 by minutes.
- **WAB vs SOS** — Scatter identifying teams with strong résumés against tough schedules vs teams padding stats against weak competition.

---

## Setup

### Prerequisites
- Python 3.12+
- Snowflake account
- KenPom subscription
- dbt Core

### Local Setup

```bash
# Clone the repo
git clone https://github.com/sunnyyan97/ncaa-cbb-analytics.git
cd ncaa-cbb-analytics

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure credentials
cp .env.example .env
# Fill in your Snowflake and KenPom credentials in .env

# Configure dbt
# Add your Snowflake connection to ~/.dbt/profiles.yml

# Run ingestion
python ingestion/kenpom/extract_kenpom.py
python ingestion/barttorvik/extract_torvik.py

# Run dbt
cd dbt/cbb_analytics
dbt deps
dbt run
dbt test

# Launch dashboard
cd ../..
python -m streamlit run app.py
```

### GitHub Actions Setup

Add the following secrets to your repository under Settings → Secrets → Actions:

```
KENPOM_EMAIL
KENPOM_PASSWORD
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
```

The pipeline runs automatically every day at 8am EST. Trigger a manual run from the Actions tab to test.

---

## Data Freshness

| Step | Schedule | Duration |
|------|----------|----------|
| KenPom ingestion | Daily 8am EST | ~30s |
| BartTorvik ingestion | Daily 8am EST | ~60s |
| dbt run | Daily after ingestion | ~2min |
| Dashboard refresh | Hourly cache TTL | Automatic |

---

## Roadmap

- [ ] Add tournament seeds and regions after Selection Sunday (March 15)
- [ ] Historical backfill — 2023 and 2024 seasons for trend analysis
- [ ] Bracket prediction model — logistic regression on historical game results to predict tournament winners
- [ ] Monte Carlo bracket simulator — simulate tournament 10,000 times to generate round-by-round win probabilities
- [ ] Game-level results ingestion from BartTorvik for head-to-head analysis

---

## Data Notes

- KenPom ratings update daily during the season
- BartTorvik ratings update continuously — pipeline captures a daily snapshot
- All efficiency metrics are per 100 possessions
- BPM (Box Plus/Minus) estimates points contributed per 100 possessions vs an average player
- Barthag represents the probability of beating an average D1 team

---

*Built by Sunny Yan · [LinkedIn](https://linkedin.com/in/sunnyyan) · [GitHub](https://github.com/sunnyyan97)*