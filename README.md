*Built by Sunny Yan · [LinkedIn](https://www.linkedin.com/in/sunny-yan-0504a6120/) · [GitHub](https://github.com/sunnyyan97)*

# ncaa-cbb-analytics

An end-to-end analytics engineering pipeline for NCAA college basketball, built on the modern data stack. Ingests data daily from KenPom and BartTorvik, transforms it through a dbt pipeline in Snowflake, and surfaces it through an interactive Streamlit dashboard.

---

## Overview

This project demonstrates a production-grade analytics engineering workflow applied to a real-world domain. The pipeline runs automatically every morning, pulling the latest team efficiency ratings and player stats, runs all dbt models, and makes fresh data available in the dashboard — with no manual intervention.

The analytical focus is NCAA tournament profiling: identifying which teams have the efficiency margins, strength of schedule, and player quality to succeed in March.

---

## Live Dashboard

**[View Live Dashboard →](https://sunny-yan-cbb-analytics.streamlit.app/)**

Using data ingested from KenPom and Bart Torvik, I created an interactive Streamlit dashboard that queries Snowflake directly and refreshes daily. Built with Plotly.

![Matchup Predictor](https://github.com/sunnyyan97/ncaa-cbb-analytics/blob/main/images/matchup_predictor.png)

**Tabs:**
- **Bracket Simulator** — Full interactive 68-team tournament bracket rendered from seeded data. Displays all teams across four regions with upset indicators (⚡) and gold highlighting for advancing winners. Populated from `bracket_data.py` after Selection Sunday.
- **Matchup Predictor** — Select any two D1 teams and a location (home/away/neutral) to generate a win probability, projected score, and side-by-side metric breakdown. Uses a formula-based log5 model on consensus AdjEM with location adjustment.
- **Efficiency Rankings** — Bar chart of top teams by consensus AdjEM, color-encoded by conference. Includes a barbell chart comparing KenPom vs BartTorvik rankings with disagreement callouts.
- **Offense vs Defense** — Scatter plot of adjusted offensive vs defensive efficiency with quadrant labels and a diagonal line separating elite two-way teams.
- **Starting Five** — Sortable table of team and player metrics aggregated across each team's top 5 by minutes percentage, including upperclassmen count.
- **WAB vs SOS** — Scatter identifying teams with strong résumés against tough schedules vs teams padding stats against weak competition.

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
                  │                │
                  │ + TOURNAMENT_  │
                  │   SEEDS table  │
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
├── modeling/
│   └── predict.py                  # Matchup prediction + player stat queries
│
├── app.py                          # Streamlit dashboard
├── render_bracket.py               # Generates bracket HTML from bracket_data.py
├── bracket_data.py                 # Seedings + bracket structure (generated post-Selection Sunday)
├── bracket_template.html           # HTML/CSS/JS bracket rendering template
├── generate_bracket_data.py        # Builds bracket_data.py + writes seeds to Snowflake
├── simulate.py                     # Tournament simulation logic
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
| `fct_tournament_profile` | Master table combining team efficiency, SOS, WAB, starting five player averages, and tournament seed data joined from `RAW.TOURNAMENT_SEEDS` |

---

## Key Design Decisions

**Canonical team dimension**
KenPom and BartTorvik use different team names (e.g. "CSUN" vs "Cal St. Northridge"). `dim_teams` resolves these naming inconsistencies with a `CASE` statement so downstream models join cleanly without dropping rows.

**Official column headers**
BartTorvik's player stats CSV ships without headers. Rather than relying on fragile positional column mapping, the ingestion script assigns the official column names sourced directly from BartTorvik's published header reference file — making the pipeline resilient to future column additions.

**Starting five aggregation**
`fct_tournament_profile` aggregates player stats across each team's top 5 players by minutes percentage (minimum 10 games) rather than relying on a single player metric. Experience is measured as the count of upperclassmen (Jr/Sr/Gr) among those five — a historically predictive tournament factor.

**Consensus efficiency margin**
Rather than choosing one model over the other, `fct_tournament_profile` averages KenPom and BartTorvik adjusted efficiency margins into a single `consensus_adj_em` metric. Where the two models strongly disagree, the source comparison chart in the dashboard surfaces the gap.

**Separate ingestion and dbt schema secrets**
The GitHub Actions pipeline uses two distinct secrets for Snowflake schema routing: `SNOWFLAKE_RAW_SCHEMA=RAW` for ingestion scripts writing raw data, and `SNOWFLAKE_SCHEMA=DEV` for dbt (which targets `DEV_MARTS`). A single shared secret caused a silent schema routing bug — separating them is the correct pattern.

**Tournament seed infrastructure**
Tournament seeds, regions, and elimination status are stored in `CBB_ANALYTICS.RAW.TOURNAMENT_SEEDS` and joined into `fct_tournament_profile` at query time. This keeps seeding data decoupled from the daily ingestion pipeline and allows the bracket and matchup predictor to filter by tournament status independently.

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
SNOWFLAKE_SCHEMA        # = DEV  (used by dbt — targets DEV_MARTS)
SNOWFLAKE_RAW_SCHEMA    # = RAW  (used by ingestion scripts)
```

> `SNOWFLAKE_SCHEMA` and `SNOWFLAKE_RAW_SCHEMA` must be configured as separate secrets. The pipeline runs automatically every day at 8am EST. Trigger a manual run from the Actions tab to test.

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

- [x] KenPom + BartTorvik ingestion pipeline
- [x] dbt staging and mart models
- [x] Efficiency Rankings tab
- [x] Offense vs. Defense scatter chart
- [x] WAB vs. SOS chart
- [x] Matchup Predictor with metric breakdown and tooltips
- [x] Starting Five player stats table
- [x] Bracket Simulator tab with full 68-team bracket
- [x] Tournament seed infrastructure (`TOURNAMENT_SEEDS` table + `fct_tournament_profile` join)
- [x] `generate_bracket_data.py` with Snowflake seed writer
- [ ] **Selection Sunday bracket population (March 15)** — see workflow below
- [ ] Tournament filter toggle in app (All Teams / Tournament Teams / Active Teams)

### Selection Sunday Workflow (March 15)

After the bracket is announced, follow these steps in order to populate the app with real tournament data.

**1. Verify team name strings**

Run this query to get the exact team name strings used in the data:
```sql
SELECT team_name, conference
FROM CBB_ANALYTICS.DEV_MARTS.FCT_TOURNAMENT_PROFILE
WHERE season = 2026
ORDER BY conference, team_name;
```
Names in `BRACKET_INPUT` must match these strings exactly (case-sensitive).

**2. Fill in the bracket**

Open `generate_bracket_data.py` and populate `BRACKET_INPUT` with all 68 teams, seeds, and regions as announced. Use the exact name strings from step 1.

**3. Run the bracket generator**
```bash
python generate_bracket_data.py
```
This validates team names against Snowflake, generates `bracket_data.py`, and writes all 68 teams with seeds and `status = 'active'` to `CBB_ANALYTICS.RAW.TOURNAMENT_SEEDS`.

**4. Verify the bracket visually**
```bash
python render_bracket.py
```
Opens the bracket in your browser. Confirm all teams, seeds, and regions are correct before pushing.

**5. Trigger a dbt run**

Run `dbt run` locally or trigger `workflow_dispatch` on the GitHub Actions `daily_ingest.yml` workflow so `fct_tournament_profile` picks up the new seed data.

**6. Commit and push**
```bash
git add bracket_data.py
git commit -m "Add 2026 tournament bracket"
git push
```
Streamlit Cloud will automatically redeploy on push.

**7. Clear the Streamlit cache**

In the deployed app, use the app menu (⋮ top right) → **Clear cache**, or wait up to 1 hour for the TTL to expire naturally.

**As teams are eliminated**, update their status in Snowflake and trigger a dbt run:
```sql
UPDATE CBB_ANALYTICS.RAW.TOURNAMENT_SEEDS
SET status = 'eliminated'
WHERE team_name = 'Team Name' AND season = 2026;
```

---

## Data Notes

- KenPom ratings update daily during the season
- BartTorvik ratings update continuously — pipeline captures a daily snapshot
- All efficiency metrics are per 100 possessions
- BPM (Box Plus/Minus) estimates points contributed per 100 possessions vs an average player
- Barthag represents the probability of beating an average D1 team on a neutral court
- Experience (upperclassmen) counts Jr/Sr/Gr players among the top 5 by minutes

  
