import os
import pandas as pd
from dotenv import load_dotenv
from kenpompy.utils import login
import kenpompy.summary as kp_summary
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

EMAIL = os.getenv("KENPOM_EMAIL")
PASSWORD = os.getenv("KENPOM_PASSWORD")

def extract_efficiency(browser, seasons: list[str]) -> pd.DataFrame:
    """Pulls team efficiency ratings for each season."""
    all_seasons = []

    for season in seasons:
        print(f"Pulling efficiency data for {season}...")
        df = kp_summary.get_efficiency(browser, season=season)
        df['season'] = season
        all_seasons.append(df)

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

    # Uppercase column names — Snowflake requirement
    df.columns = [c.upper() for c in df.columns]

    success, chunks, rows, _ = write_pandas(conn, df, table_name.upper(), auto_create_table=True)
    print(f"Loaded {rows} rows to {table_name}")
    conn.close()

if __name__ == "__main__":
    seasons = ['2025']
    browser = login(EMAIL, PASSWORD)

    efficiency_df = extract_efficiency(browser, seasons)
    print(f"Efficiency rows: {len(efficiency_df)}")
    efficiency_df.to_csv('ingestion/kenpom/kenpom_efficiency_raw.csv', index=False)

    load_to_snowflake(efficiency_df, 'KENPOM_TEAM_RATINGS')
    print("Done")