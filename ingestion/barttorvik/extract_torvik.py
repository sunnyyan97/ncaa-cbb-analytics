import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()


def extract_team_results(seasons: list[str]) -> pd.DataFrame:
    """
    Pulls team results directly from BartTorvik's public CSV endpoints.
    URL format: barttorvik.com/{year}_team_results.csv
    """
    all_seasons = []

    for season in seasons:
        url = f"https://barttorvik.com/{season}_team_results.csv"
        print(f"Pulling BartTorvik team results for {season}...")

        try:
            df = pd.read_csv(url, header=None)
            df['season'] = season
            all_seasons.append(df)
            print(f"  ✓ {len(df)} rows pulled for {season}")

        except Exception as e:
            print(f"  ✗ Failed for {season}: {e}")

    return pd.concat(all_seasons, ignore_index=True)


def extract_player_stats(seasons: list[str]) -> pd.DataFrame:
    all_seasons = []

    # Known column names for BartTorvik player stats
    column_names = [
        'player_name', 'team', 'conf', 'games', 'minutes_pct',
        'ortg', 'usage', 'ef_g_pct', 'ts_pct', 'orb_pct',
        'drb_pct', 'ast_pct', 'to_pct', 'blk_pct', 'stl_pct',
        'ftr', 'two_pt_pct', 'three_pt_pct', 'ft_pct',
        'two_pt_rate', 'three_pt_rate', 'ft_rate',
        'pprod', 'stops', 'bpm', 'obpm', 'dbpm',
        'player_id', 'year', 'height'
    ]

    for season in seasons:
        url = f"https://barttorvik.com/getadvstats.php?year={season}&csv=1"
        print(f"Pulling BartTorvik player stats for {season}...")

        try:
            df = pd.read_csv(url, header=None)
            
            # Assign column names if count matches, otherwise use generic ones
            if len(df.columns) == len(column_names):
                df.columns = column_names
            else:
                print(f"  ⚠️ Column count mismatch — expected {len(column_names)}, got {len(df.columns)}")
                print(f"  Using generic column names COL_0, COL_1 etc.")
                df.columns = [f"col_{i}" for i in range(len(df.columns))]

            df['season'] = season
            all_seasons.append(df)
            print(f"  ✓ {len(df)} rows pulled for {season}")

        except Exception as e:
            print(f"  ✗ Failed for {season}: {e}")

    return pd.concat(all_seasons, ignore_index=True)


def load_to_snowflake(df: pd.DataFrame, table_name: str):
    """Loads a DataFrame to Snowflake raw schema."""
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    # Prefix numeric column names with COL_ to make valid Snowflake identifiers
    df.columns = [
        f"COL_{c}".upper() if str(c).isdigit() else str(c).upper() 
        for c in df.columns
    ]

    success, chunks, rows, _ = write_pandas(
        conn, df, table_name.upper(), 
        auto_create_table=True,
        quote_identifiers=False  # Don't quote column names, even if they have special characters
    )
    print(f"Loaded {rows} rows to {table_name}")
    conn.close()


if __name__ == "__main__":
    seasons = ['2025']

    team_df = extract_team_results(seasons)
    player_df = extract_player_stats(seasons)

    team_df.to_csv('ingestion/barttorvik/torvik_team_results_raw.csv', index=False)
    player_df.to_csv('ingestion/barttorvik/torvik_player_stats_raw.csv', index=False)

    # load_to_snowflake(team_df, 'TORVIK_TEAM_RESULTS')  ← commented out
    load_to_snowflake(player_df, 'TORVIK_PLAYER_STATS')