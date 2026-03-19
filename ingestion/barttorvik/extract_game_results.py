import os
import re
import time
import requests
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Constants ──────────────────────────────────────────────────────────────────

MIN_EXPECTED_ROWS = 5000   # safety threshold — ~2 rows per game × ~2500+ games
MAX_RETRIES       = 3
BASE_URL          = "https://barttorvik.com/getgamestats.php"

# Column indices in BartTorvik getgamestats.php JSON response (array of arrays)
COL_DATE        = 0   # "MM/DD/YY"
COL_TEAM        = 2   # BartTorvik team name
COL_OPPONENT    = 4   # BartTorvik opponent name
COL_LOCATION    = 5   # "N" / "H" / "A"  (from queried team's perspective)
COL_RESULT      = 6   # "W, 80-71" or "L, 68-74"
COL_SEASON_TYPE = 21  # integer: 1=non-conference, 2=conference, 3=postseason
COL_YEAR        = 22  # e.g. 2026
COL_GAME_ID     = 24  # composite string e.g. "ArkansasDuke11-27"

LOCATION_MAP = {"N": "Neutral", "H": "Home", "A": "Away"}

# ── Team list ──────────────────────────────────────────────────────────────────
# Reuse the full D1 set from the cbbpy script — values are already BartTorvik names
from ingestion.cbbpy.extract_game_results import ESPN_TO_TORVIK
TORVIK_TEAMS = list(set(ESPN_TO_TORVIK.values()))


# ── Helper functions ───────────────────────────────────────────────────────────

def clean_value(v):
    """Safely convert a value for Snowflake insertion."""
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if type(v).__name__ in ("bool_", "bool"):
        return bool(v)
    return v


def parse_result(result_str: str):
    """
    Parse a BartTorvik result string like "W, 80-71" or "L, 68-74".
    Returns (team_score, opp_score, team_win) or (None, None, None) on failure.
    """
    m = re.match(r'([WL]),?\s*(\d+)-(\d+)', result_str or "")
    if not m:
        return None, None, None
    win_flag, score_a, score_b = m.group(1), int(m.group(2)), int(m.group(3))
    return score_a, score_b, (win_flag == "W")


def fetch_team_games(team: str, year: int) -> list[dict]:
    """
    Fetch completed games for a single team from BartTorvik.
    Returns a list of row dicts ready for Snowflake insertion.
    """
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(
                BASE_URL,
                params={"year": year, "tvalue": team},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            break
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
    else:
        raise last_error

    rows = []
    for row in data:
        game_id  = row[COL_GAME_ID]
        result   = row[COL_RESULT]

        # Skip rows with no game_id or no result (future/cancelled games)
        if not game_id or not result:
            continue

        team_score, opp_score, team_win = parse_result(result)
        if team_score is None:
            continue

        location_raw = row[COL_LOCATION]
        rows.append({
            "game_id":        str(game_id),
            "season":         year,
            "game_day":       row[COL_DATE],
            "team_torvik":    row[COL_TEAM],
            "opponent_torvik": row[COL_OPPONENT],
            "team_score":     float(team_score),
            "opp_score":      float(opp_score),
            "team_win":       team_win,
            "season_type":    str(row[COL_SEASON_TYPE]),
            "game_location":  LOCATION_MAP.get(location_raw, location_raw),
        })

    return rows


# ── Core ingestion functions ───────────────────────────────────────────────────

def fetch_all_games(season: int) -> list[dict]:
    """
    Fetch completed game results for all D1 teams for a given season.
    Stores one row per team per game (no deduplication) so game_location
    is always meaningful from the queried team's perspective.
    """
    all_rows = []
    failed_teams = []

    print(f"  Fetching {len(TORVIK_TEAMS)} teams from BartTorvik...")

    for i, team in enumerate(TORVIK_TEAMS):
        try:
            rows = fetch_team_games(team, season)
            all_rows.extend(rows)
        except Exception as e:
            print(f"  WARNING: Failed for '{team}' after {MAX_RETRIES} retries: {e}")
            failed_teams.append(team)

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i + 1}/{len(TORVIK_TEAMS)} teams — {len(all_rows)} rows so far")

        time.sleep(0.3)

    if failed_teams:
        print(f"  {len(failed_teams)} teams failed: {failed_teams}")

    print(f"  Season {season}: {len(all_rows)} total rows from {len(TORVIK_TEAMS)} teams")

    if len(all_rows) < MIN_EXPECTED_ROWS:
        print(
            f"  WARNING: Only {len(all_rows)} rows collected (expected >= {MIN_EXPECTED_ROWS}). "
            f"Skipping Snowflake load to protect existing data."
        )
        return []

    return all_rows


def load_to_snowflake(rows: list[dict], season: int):
    """
    Load game results into RAW.CBB_GAME_RESULTS.
    Adds game_location column if not present, then deletes and reloads for the season.
    """
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "CBB_ANALYTICS"),
        schema="RAW",
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist (matches existing schema + game_location)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CBB_ANALYTICS.RAW.CBB_GAME_RESULTS (
            game_id          VARCHAR,
            season           INTEGER,
            game_day         VARCHAR,
            team_torvik      VARCHAR,
            opponent_torvik  VARCHAR,
            team_score       FLOAT,
            opp_score        FLOAT,
            team_win         BOOLEAN,
            season_type      VARCHAR,
            game_location    VARCHAR,
            loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Add game_location column to any existing table that predates this script
    try:
        cursor.execute(
            "ALTER TABLE CBB_ANALYTICS.RAW.CBB_GAME_RESULTS ADD COLUMN game_location VARCHAR"
        )
    except Exception:
        pass  # column already exists

    # Delete existing rows for this season before reloading
    cursor.execute(f"DELETE FROM CBB_ANALYTICS.RAW.CBB_GAME_RESULTS WHERE season = {season}")
    print(f"  Deleted {cursor.rowcount} existing rows for season {season}")

    target_cols = [
        "game_id", "season", "game_day", "team_torvik", "opponent_torvik",
        "team_score", "opp_score", "team_win", "season_type", "game_location",
    ]
    col_names   = ", ".join(target_cols)
    placeholders = ", ".join(["%s"] * len(target_cols))
    insert_sql  = f"""
        INSERT INTO CBB_ANALYTICS.RAW.CBB_GAME_RESULTS ({col_names})
        VALUES ({placeholders})
    """

    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        clean_batch = [
            [clean_value(row[c]) for c in target_cols]
            for row in batch
        ]
        cursor.executemany(insert_sql, clean_batch)
        total += len(batch)

    conn.commit()
    print(f"  Inserted {total} rows for season {season}")

    cursor.close()
    conn.close()


# ── Entry point ────────────────────────────────────────────────────────────────

def run():
    seasons = [2026]
    for season in seasons:
        print(f"\n{'='*50}")
        print(f"Processing season {season}...")
        print(f"{'='*50}")
        try:
            rows = fetch_all_games(season)
            if not rows:
                print(f"  Skipping Snowflake load for season {season}")
                continue
            load_to_snowflake(rows, season)
            print(f"  Season {season} complete ✓")
        except Exception as e:
            print(f"  ERROR for season {season}: {e}")
            raise


if __name__ == "__main__":
    run()
