# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Run the Streamlit dashboard locally
```bash
source venv/bin/activate
python -m streamlit run app.py
```

### Run ingestion scripts
```bash
source venv/bin/activate
python ingestion/kenpom/extract_kenpom.py
python ingestion/barttorvik/extract_torvik.py
```

### Run dbt models
```bash
cd dbt/cbb_analytics
dbt run                        # run all models
dbt run --select staging       # run only staging layer
dbt run --select marts         # run only marts layer
dbt run --select fct_tournament_profile  # run a single model
dbt test                       # run all tests
dbt deps                       # install packages (run once or after packages.yml changes)
```

### Test the prediction model
```bash
source venv/bin/activate
python -m modeling.predict                  # runs Duke vs Michigan example
python -m modeling.simulate                 # runs 10,000-simulation tournament test
python -m dashboard.generate_bracket_data   # regenerates dashboard/bracket_data.py from Snowflake
```

### Install dependencies
```bash
pip install -r requirements.txt
```

## Architecture

This is a full analytics pipeline: ingestion → Snowflake → dbt → Streamlit.

### Data flow
1. **Ingestion** — `ingestion/kenpom/extract_kenpom.py` logs into KenPom via `kenpompy` and writes to `CBB_ANALYTICS.RAW.KENPOM_TEAM_RATINGS`. `ingestion/barttorvik/extract_torvik.py` pulls CSVs directly from BartTorvik's public endpoints and writes to `CBB_ANALYTICS.RAW.TORVIK_TEAM_RESULTS` and `CBB_ANALYTICS.RAW.TORVIK_PLAYER_STATS`. Both scripts do a full `DROP TABLE + recreate` on each run.
2. **dbt transformation** — Staging models (materialized as views in `DEV_STAGING`) clean and rename raw columns. Mart models (materialized as tables in `DEV_MARTS`) join the sources and build the analytical tables.
3. **Dashboard** — `app.py` queries Snowflake directly at runtime via `@st.cache_data`. All Snowflake reads are in `modeling/predict.py`.
4. **Automation** — GitHub Actions (`.github/workflows/daily_ingest.yml`) runs the full pipeline daily at 8am EST.

### dbt model dependency chain
```
RAW.KENPOM_TEAM_RATINGS  ──► stg_kenpom__team_ratings ──┐
RAW.TORVIK_TEAM_RESULTS  ──► stg_torvik__team_results  ──┼──► dim_teams ──► fct_team_ratings ──► fct_tournament_profile
RAW.TORVIK_PLAYER_STATS  ──► stg_torvik__player_stats  ──┘                  fct_player_stats ──►        ↑
RAW.TOURNAMENT_SEEDS ───────────────────────────────────────────────────────────────────────────────────┘
```

`dim_teams` is the canonical team dimension that resolves name differences between KenPom and BartTorvik (e.g. `CSUN` → `Cal St. Northridge`). All mart models join through canonical `team_name`.

`fct_tournament_profile` is the primary table used by the dashboard and prediction model. It left-joins `RAW.TOURNAMENT_SEEDS` for tournament seed/region — these fields are null until Selection Sunday.

### Prediction model (`modeling/`)
- `modeling/predict.py` — `get_all_team_stats()` pulls from `fct_tournament_profile`. `predict_matchup()` uses a log5 formula on `consensus_adj_em` with a ±3.5 pt home court adjustment. Scores are derived from average tempo and offensive efficiencies.
- `modeling/simulate.py` — Monte Carlo tournament simulator that calls `predict_matchup()` for each game. Takes a bracket dict keyed by region (`East/West/South/Midwest`), each a list of 16 `{"seed": int, "team": str}` dicts ordered by `FIRST_ROUND_SEED_ORDER`.

### Snowflake schema layout
- `CBB_ANALYTICS.RAW` — raw ingested tables (written by Python ingestion scripts)
- `CBB_ANALYTICS.DEV_STAGING` — dbt staging views
- `CBB_ANALYTICS.DEV_MARTS` — dbt mart tables (queried by the dashboard)

### Environment variables
All credentials are loaded from `.env` (locally) or GitHub Actions secrets. Required vars: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`. Ingestion also needs `KENPOM_EMAIL` and `KENPOM_PASSWORD`.

### Season handling
The current active season is `2026`. All ingestion scripts, dbt models, and dashboard queries are hardcoded to `season = 2026`. When updating for a new season, change the `seasons = ['2026']` list in both ingestion scripts and the `WHERE season = 2026` filter in `modeling/predict.py`.