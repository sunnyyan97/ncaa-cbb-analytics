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

    # Official column headers from barttorvik.com/pstatheaders.xlsx
    column_names = [
        'player_name', 'team', 'conf', 'gp', 'min_per', 'ortg', 'usg',
        'efg', 'ts_per', 'orb_per', 'drb_per', 'ast_per', 'to_per',
        'ftm', 'fta', 'ft_per', 'two_pm', 'two_pa', 'two_p_per',
        'tpm', 'tpa', 'tp_per', 'blk_per', 'stl_per', 'ftr', 'yr', 'ht',
        'num', 'porpag', 'adjoe', 'pfr', 'year', 'pid', 'type',
        'rec_rank', 'ast_tov', 'rim_made', 'rim_att', 'mid_made', 'mid_att',
        'rim_pct', 'mid_pct', 'dunks_made', 'dunks_att', 'dunk_pct',
        'pick', 'drtg', 'adrtg', 'dporpag', 'stops', 'bpm', 'obpm',
        'dbpm', 'gbpm', 'mp', 'ogbpm', 'dgbpm', 'oreb', 'dreb', 'treb',
        'ast', 'stl', 'blk', 'pts', 'role', 'three_p_per_100', 'season'
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
    """Loads a DataFrame to Snowflake raw schema, replacing existing data."""
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name.upper()}")

    # Convert all column names to strings first
    df.columns = df.columns.astype(str)

    # Clean column names — remove special characters invalid in Snowflake
    df.columns = (
        df.columns
        .str.upper()
        .str.replace('.', '_', regex=False)
        .str.replace('-', '_', regex=False)
        .str.replace(' ', '_', regex=False)
        .str.replace('(', '', regex=False)
        .str.replace(')', '', regex=False)
        .str.strip('_')
    )

    # Prefix any remaining numeric column names
    df.columns = [
        f"COL_{c}" if c[0].isdigit() else c
        for c in df.columns
    ]

    success, chunks, rows, _ = write_pandas(
        conn, df, table_name.upper(),
        auto_create_table=True,
        quote_identifiers=False
    )
    print(f"Loaded {rows} rows to {table_name}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    seasons = ['2026']

    team_df = extract_team_results(seasons)
    player_df = extract_player_stats(seasons)

    team_df.to_csv('ingestion/barttorvik/torvik_team_results_raw.csv', index=False)
    player_df.to_csv('ingestion/barttorvik/torvik_player_stats_raw.csv', index=False)

    load_to_snowflake(team_df, 'TORVIK_TEAM_RESULTS')
    load_to_snowflake(player_df, 'TORVIK_PLAYER_STATS')