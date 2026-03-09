import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv
import cbbpy.mens_scraper as cbb
import time

load_dotenv()

# ── Constants ──────────────────────────────────────────────────────────────────

MIN_EXPECTED_GAMES = 1000  # safety threshold — skip Snowflake load if below this
MAX_RETRIES = 3            # retry attempts per team on transient failures

# ── ESPN → BartTorvik name mapping ─────────────────────────────────────────────

ESPN_TO_TORVIK = {
    "Air Force Falcons": "Air Force",
    "Akron Zips": "Akron",
    "Alabama Crimson Tide": "Alabama",
    "Alabama A&M Bulldogs": "Alabama A&M",
    "Alabama State Hornets": "Alabama St.",
    "Albany Great Danes": "Albany",
    "Alcorn State Braves": "Alcorn",
    "American Eagles": "American",
    "Appalachian State Mountaineers": "Appalachian St.",
    "Arizona Wildcats": "Arizona",
    "Arizona State Sun Devils": "Arizona St.",
    "Arkansas Razorbacks": "Arkansas",
    "Arkansas State Red Wolves": "Arkansas St.",
    "Arkansas-Pine Bluff Golden Lions": "Arkansas Pine Bluff",
    "Army Black Knights": "Army",
    "Auburn Tigers": "Auburn",
    "Austin Peay Governors": "Austin Peay",
    "Ball State Cardinals": "Ball St.",
    "Baylor Bears": "Baylor",
    "Belmont Bruins": "Belmont",
    "Bethune-Cookman Wildcats": "Bethune-Cookman",
    "Boise State Broncos": "Boise St.",
    "Boston College Eagles": "Boston College",
    "Boston University Terriers": "Boston University",
    "Bowling Green Falcons": "Bowling Green",
    "Bradley Braves": "Bradley",
    "Brown Bears": "Brown",
    "Bryant Bulldogs": "Bryant",
    "Bucknell Bison": "Bucknell",
    "Buffalo Bulls": "Buffalo",
    "Butler Bulldogs": "Butler",
    "BYU Cougars": "BYU",
    "Cal Poly Mustangs": "Cal Poly",
    "Cal State Bakersfield Roadrunners": "CS Bakersfield",
    "Cal State Fullerton Titans": "Cal St. Fullerton",
    "Cal State Northridge Matadors": "Cal St. Northridge",
    "California Golden Bears": "California",
    "Campbell Fighting Camels": "Campbell",
    "Canisius Golden Griffins": "Canisius",
    "Central Arkansas Bears": "Central Arkansas",
    "Central Connecticut Blue Devils": "Central Connecticut",
    "Central Michigan Chippewas": "Central Michigan",
    "Charleston Southern Buccaneers": "Charleston Southern",
    "Charlotte 49ers": "Charlotte",
    "Chicago State Cougars": "Chicago St.",
    "Cincinnati Bearcats": "Cincinnati",
    "Clemson Tigers": "Clemson",
    "Cleveland State Vikings": "Cleveland St.",
    "Coastal Carolina Chanticleers": "Coastal Carolina",
    "Colgate Raiders": "Colgate",
    "College of Charleston Cougars": "Charleston",
    "Colorado Buffaloes": "Colorado",
    "Colorado State Rams": "Colorado St.",
    "Columbia Lions": "Columbia",
    "Connecticut Huskies": "Connecticut",
    "Coppin State Eagles": "Coppin St.",
    "Cornell Big Red": "Cornell",
    "Creighton Bluejays": "Creighton",
    "Dartmouth Big Green": "Dartmouth",
    "Davidson Wildcats": "Davidson",
    "Dayton Flyers": "Dayton",
    "Delaware Fightin Blue Hens": "Delaware",
    "Delaware State Hornets": "Delaware St.",
    "Denver Pioneers": "Denver",
    "DePaul Blue Demons": "DePaul",
    "Detroit Mercy Titans": "Detroit Mercy",
    "Drake Bulldogs": "Drake",
    "Drexel Dragons": "Drexel",
    "Duke Blue Devils": "Duke",
    "Duquesne Dukes": "Duquesne",
    "East Carolina Pirates": "East Carolina",
    "East Tennessee State Buccaneers": "East Tennessee St.",
    "Eastern Illinois Panthers": "Eastern Illinois",
    "Eastern Kentucky Colonels": "Eastern Kentucky",
    "Eastern Michigan Eagles": "Eastern Michigan",
    "Eastern Washington Eagles": "Eastern Washington",
    "Elon Phoenix": "Elon",
    "Evansville Purple Aces": "Evansville",
    "Fairfield Stags": "Fairfield",
    "Fairleigh Dickinson Knights": "Fairleigh Dickinson",
    "Florida Gators": "Florida",
    "Florida A&M Rattlers": "Florida A&M",
    "Florida Atlantic Owls": "Florida Atlantic",
    "Florida Gulf Coast Eagles": "Florida Gulf Coast",
    "Florida International Panthers": "FIU",
    "Florida State Seminoles": "Florida St.",
    "Fordham Rams": "Fordham",
    "Fresno State Bulldogs": "Fresno St.",
    "Furman Paladins": "Furman",
    "Gardner-Webb Runnin' Bulldogs": "Gardner-Webb",
    "George Mason Patriots": "George Mason",
    "George Washington Revolutionaries": "George Washington",
    "Georgetown Hoyas": "Georgetown",
    "Georgia Bulldogs": "Georgia",
    "Georgia Southern Eagles": "Georgia Southern",
    "Georgia State Panthers": "Georgia St.",
    "Georgia Tech Yellow Jackets": "Georgia Tech",
    "Gonzaga Bulldogs": "Gonzaga",
    "Grambling Tigers": "Grambling",
    "Grand Canyon Antelopes": "Grand Canyon",
    "Green Bay Phoenix": "Green Bay",
    "Hampton Pirates": "Hampton",
    "Hartford Hawks": "Hartford",
    "Harvard Crimson": "Harvard",
    "Hawaii Rainbow Warriors": "Hawaii",
    "High Point Panthers": "High Point",
    "Hofstra Pride": "Hofstra",
    "Holy Cross Crusaders": "Holy Cross",
    "Houston Cougars": "Houston",
    "Houston Christian Huskies": "Houston Christian",
    "Howard Bison": "Howard",
    "Idaho Vandals": "Idaho",
    "Idaho State Bengals": "Idaho St.",
    "Illinois Fighting Illini": "Illinois",
    "Illinois State Redbirds": "Illinois St.",
    "Incarnate Word Cardinals": "Incarnate Word",
    "Indiana Hoosiers": "Indiana",
    "Indiana State Sycamores": "Indiana St.",
    "Iona Gaels": "Iona",
    "Iowa Hawkeyes": "Iowa",
    "Iowa State Cyclones": "Iowa St.",
    "Jackson State Tigers": "Jackson St.",
    "Jacksonville Dolphins": "Jacksonville",
    "Jacksonville State Gamecocks": "Jacksonville St.",
    "James Madison Dukes": "James Madison",
    "Kansas Jayhawks": "Kansas",
    "Kansas City Roos": "UMKC",
    "Kansas State Wildcats": "Kansas St.",
    "Kennesaw State Owls": "Kennesaw St.",
    "Kent State Golden Flashes": "Kent St.",
    "Kentucky Wildcats": "Kentucky",
    "La Salle Explorers": "La Salle",
    "Lafayette Leopards": "Lafayette",
    "Lamar Cardinals": "Lamar",
    "Lehigh Mountain Hawks": "Lehigh",
    "Liberty Flames": "Liberty",
    "Lindenwood Lions": "Lindenwood",
    "Lipscomb Bisons": "Lipscomb",
    "Little Rock Trojans": "Little Rock",
    "Long Beach State Beach": "Long Beach St.",
    "Long Island University Sharks": "LIU",
    "Louisiana Ragin' Cajuns": "Louisiana",
    "Louisiana Tech Bulldogs": "Louisiana Tech",
    "Louisville Cardinals": "Louisville",
    "Loyola Chicago Ramblers": "Loyola Chicago",
    "Loyola Maryland Greyhounds": "Loyola Maryland",
    "Loyola Marymount Lions": "LMU (CA)",
    "LSU Tigers": "LSU",
    "Maine Black Bears": "Maine",
    "Manhattan Jaspers": "Manhattan",
    "Marist Red Foxes": "Marist",
    "Marquette Golden Eagles": "Marquette",
    "Marshall Thundering Herd": "Marshall",
    "Maryland Terrapins": "Maryland",
    "McNeese Cowboys": "McNeese St.",
    "Memphis Tigers": "Memphis",
    "Mercer Bears": "Mercer",
    "Miami Hurricanes": "Miami FL",
    "Miami (OH) RedHawks": "Miami OH",
    "Michigan Wolverines": "Michigan",
    "Michigan State Spartans": "Michigan St.",
    "Middle Tennessee Blue Raiders": "Middle Tennessee",
    "Milwaukee Panthers": "Milwaukee",
    "Minnesota Golden Gophers": "Minnesota",
    "Mississippi State Bulldogs": "Mississippi St.",
    "Mississippi Valley State Delta Devils": "Mississippi Valley St.",
    "Missouri Tigers": "Missouri",
    "Missouri State Bears": "Missouri St.",
    "Monmouth Hawks": "Monmouth",
    "Montana Grizzlies": "Montana",
    "Montana State Bobcats": "Montana St.",
    "Morehead State Eagles": "Morehead St.",
    "Morgan State Bears": "Morgan St.",
    "Mount St. Mary's Mountaineers": "Mount St. Mary's",
    "Murray State Racers": "Murray St.",
    "Navy Midshipmen": "Navy",
    "Nebraska Cornhuskers": "Nebraska",
    "Nevada Wolf Pack": "Nevada",
    "New Hampshire Wildcats": "New Hampshire",
    "New Mexico Lobos": "New Mexico",
    "New Mexico State Aggies": "New Mexico St.",
    "New Orleans Privateers": "New Orleans",
    "Niagara Purple Eagles": "Niagara",
    "Nicholls Colonels": "Nicholls St.",
    "NJIT Highlanders": "NJIT",
    "Norfolk State Spartans": "Norfolk St.",
    "North Alabama Lions": "North Alabama",
    "North Carolina Tar Heels": "North Carolina",
    "North Carolina A&T Aggies": "North Carolina A&T",
    "North Carolina Central Eagles": "NC Central",
    "North Carolina State Wolfpack": "NC State",
    "North Dakota Fighting Hawks": "North Dakota",
    "North Dakota State Bison": "North Dakota St.",
    "North Florida Ospreys": "North Florida",
    "North Texas Mean Green": "North Texas",
    "Northeastern Huskies": "Northeastern",
    "Northern Arizona Lumberjacks": "Northern Arizona",
    "Northern Colorado Bears": "Northern Colorado",
    "Northern Illinois Huskies": "Northern Illinois",
    "Northern Iowa Panthers": "Northern Iowa",
    "Northwestern Wildcats": "Northwestern",
    "Northwestern State Demons": "Northwestern St.",
    "Notre Dame Fighting Irish": "Notre Dame",
    "Oakland Golden Grizzlies": "Oakland",
    "Ohio Bobcats": "Ohio",
    "Ohio State Buckeyes": "Ohio St.",
    "Oklahoma Sooners": "Oklahoma",
    "Oklahoma State Cowboys": "Oklahoma St.",
    "Old Dominion Monarchs": "Old Dominion",
    "Ole Miss Rebels": "Mississippi",
    "Oral Roberts Golden Eagles": "Oral Roberts",
    "Oregon Ducks": "Oregon",
    "Oregon State Beavers": "Oregon St.",
    "Pacific Tigers": "Pacific",
    "Penn State Nittany Lions": "Penn St.",
    "Pennsylvania Quakers": "Pennsylvania",
    "Pepperdine Waves": "Pepperdine",
    "Pittsburgh Panthers": "Pittsburgh",
    "Portland Pilots": "Portland",
    "Portland State Vikings": "Portland St.",
    "Prairie View A&M Panthers": "Prairie View",
    "Presbyterian Blue Hose": "Presbyterian",
    "Princeton Tigers": "Princeton",
    "Providence Friars": "Providence",
    "Purdue Boilermakers": "Purdue",
    "Purdue Fort Wayne Mastodons": "Fort Wayne",
    "Quinnipiac Bobcats": "Quinnipiac",
    "Radford Highlanders": "Radford",
    "Rhode Island Rams": "Rhode Island",
    "Rice Owls": "Rice",
    "Richmond Spiders": "Richmond",
    "Rider Broncs": "Rider",
    "Robert Morris Colonials": "Robert Morris",
    "Rutgers Scarlet Knights": "Rutgers",
    "Sacramento State Hornets": "Sacramento St.",
    "Saint Francis Red Flash": "Saint Francis",
    "Saint Joseph's Hawks": "Saint Joseph's",
    "Saint Louis Billikens": "Saint Louis",
    "Saint Mary's Gaels": "Saint Mary's",
    "Saint Peter's Peacocks": "Saint Peter's",
    "Sam Houston State Bearkats": "Sam Houston",
    "Samford Bulldogs": "Samford",
    "San Diego State Aztecs": "San Diego St.",
    "San Diego Toreros": "San Diego",
    "San Francisco Dons": "San Francisco",
    "San Jose State Spartans": "San Jose St.",
    "Santa Clara Broncos": "Santa Clara",
    "Seattle Redhawks": "Seattle",
    "Seton Hall Pirates": "Seton Hall",
    "Siena Saints": "Siena",
    "SIU Edwardsville Cougars": "SIU Edwardsville",
    "SMU Mustangs": "SMU",
    "South Alabama Jaguars": "South Alabama",
    "South Carolina Gamecocks": "South Carolina",
    "South Carolina State Bulldogs": "South Carolina St.",
    "South Dakota Coyotes": "South Dakota",
    "South Dakota State Jackrabbits": "South Dakota St.",
    "South Florida Bulls": "South Florida",
    "Southeast Missouri State Redhawks": "Southeast Missouri St.",
    "Southeastern Louisiana Lions": "SE Louisiana",
    "Southern Jaguars": "Southern",
    "Southern Illinois Salukis": "Southern Illinois",
    "Southern Miss Golden Eagles": "Southern Miss",
    "Southern Utah Thunderbirds": "Southern Utah",
    "St. Bonaventure Bonnies": "St. Bonaventure",
    "St. Francis Brooklyn Terriers": "St. Francis Brooklyn",
    "St. John's Red Storm": "St. John's",
    "St. Thomas Tommies": "St. Thomas",
    "Stanford Cardinal": "Stanford",
    "Stephen F. Austin Lumberjacks": "SFA",
    "Stetson Hatters": "Stetson",
    "Stony Brook Seawolves": "Stony Brook",
    "Syracuse Orange": "Syracuse",
    "TCU Horned Frogs": "TCU",
    "Temple Owls": "Temple",
    "Tennessee Volunteers": "Tennessee",
    "Tennessee State Tigers": "Tennessee St.",
    "Tennessee Tech Golden Eagles": "Tennessee Tech",
    "Texas Longhorns": "Texas",
    "Texas A&M Aggies": "Texas A&M",
    "Texas A&M-Corpus Christi Islanders": "Texas A&M Corpus Christi",
    "Texas Southern Tigers": "Texas Southern",
    "Texas State Bobcats": "Texas St.",
    "Texas Tech Red Raiders": "Texas Tech",
    "The Citadel Bulldogs": "The Citadel",
    "Toledo Rockets": "Toledo",
    "Towson Tigers": "Towson",
    "Troy Trojans": "Troy",
    "Tulane Green Wave": "Tulane",
    "Tulsa Golden Hurricane": "Tulsa",
    "UAB Blazers": "UAB",
    "UC Davis Aggies": "UC Davis",
    "UC Irvine Anteaters": "UC Irvine",
    "UC Riverside Highlanders": "UC Riverside",
    "UC San Diego Tritons": "UC San Diego",
    "UC Santa Barbara Gauchos": "UC Santa Barbara",
    "UCF Knights": "UCF",
    "UCLA Bruins": "UCLA",
    "UMass Minutemen": "Massachusetts",
    "UMass Lowell River Hawks": "UMass Lowell",
    "UNC Asheville Bulldogs": "UNC Asheville",
    "UNC Greensboro Spartans": "UNC Greensboro",
    "UNC Wilmington Seahawks": "UNC Wilmington",
    "UNLV Rebels": "UNLV",
    "USC Trojans": "USC",
    "USC Upstate Spartans": "USC Upstate",
    "UT Arlington Mavericks": "UT Arlington",
    "UT Martin Skyhawks": "UT Martin",
    "UTEP Miners": "UTEP",
    "UTSA Roadrunners": "UTSA",
    "Utah Utes": "Utah",
    "Utah State Aggies": "Utah St.",
    "Utah Valley Wolverines": "Utah Valley",
    "Valparaiso Beacons": "Valparaiso",
    "VCU Rams": "VCU",
    "Vermont Catamounts": "Vermont",
    "Villanova Wildcats": "Villanova",
    "Virginia Cavaliers": "Virginia",
    "Virginia Tech Hokies": "Virginia Tech",
    "VMI Keydets": "VMI",
    "Wagner Seahawks": "Wagner",
    "Wake Forest Demon Deacons": "Wake Forest",
    "Washington Huskies": "Washington",
    "Washington State Cougars": "Washington St.",
    "Weber State Wildcats": "Weber St.",
    "West Virginia Mountaineers": "West Virginia",
    "Western Carolina Catamounts": "Western Carolina",
    "Western Illinois Leathernecks": "Western Illinois",
    "Western Kentucky Hilltoppers": "Western Kentucky",
    "Western Michigan Broncos": "Western Michigan",
    "Wichita State Shockers": "Wichita St.",
    "William & Mary Tribe": "William & Mary",
    "Winthrop Eagles": "Winthrop",
    "Wisconsin Badgers": "Wisconsin",
    "Wofford Terriers": "Wofford",
    "Wright State Raiders": "Wright St.",
    "Wyoming Cowboys": "Wyoming",
    "Xavier Musketeers": "Xavier",
    "Yale Bulldogs": "Yale",
    "Youngstown State Penguins": "Youngstown St.",
}

# All D1 conferences — exact ESPN strings
D1_CONFERENCES = [
    "ACC",
    "Big Ten Conference",
    "Big 12 Conference",
    "SEC",
    "Big East Conference",
    "American Athletic Conference",
    "Atlantic 10 Conference",
    "Mountain West Conference",
    "West Coast Conference",
    "Missouri Valley Conference",
    "Mid-American Conference",
    "Sun Belt Conference",
    "Horizon League",
    "Coastal Athletic Association",
    "Southern Conference",
    "Summit League",
    "Big West Conference",
    "Ivy League",
    "Patriot League",
    "America East Conference",
    "Big South Conference",
    "Northeast Conference",
    "Ohio Valley Conference",
    "Southwestern Athletic Conference",
    "Mid-Eastern Athletic Conference",
    "Western Athletic Conference",
    "ASUN Conference",
    "Southland Conference",
]

# Columns expected from cbbpy schedule output — used for validation
EXPECTED_COLS = [
    "game_id", "game_day", "game_time",
    "team", "opponent", "season_type", "game_result",
]


# ── Helper functions ───────────────────────────────────────────────────────────

def normalize_team_name(espn_name: str) -> str:
    """Convert ESPN full team name to BartTorvik short name."""
    torvik_name = ESPN_TO_TORVIK.get(espn_name)
    if torvik_name is None:
        print(f"  WARNING: No ESPN→Torvik mapping for '{espn_name}' — using ESPN name as fallback")
    return torvik_name or espn_name


def clean_value(v):
    """
    Safely convert a value for Snowflake insertion.
    Handles NaN, pandas NA, numpy bools, and regular Python types.
    """
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    # Convert numpy/pandas bool types to Python bool
    if type(v).__name__ in ("bool_", "bool"):
        return bool(v)
    return v


def fetch_team_schedule_with_retry(team: str, season, max_retries: int = MAX_RETRIES):
    """
    Fetch a team's schedule with exponential backoff retry on failure.
    Pass season=None to use cbbpy's current season default.
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            if season is None:
                return cbb.get_team_schedule(team=team)
            return cbb.get_team_schedule(team=team, season=season)
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s, 4s
                time.sleep(wait)
    raise last_error


def get_all_d1_teams() -> list:
    """
    Dynamically fetch all D1 teams by pulling each conference's roster.
    Logs any conference that fails rather than silently skipping it.
    Returns a deduplicated list of ESPN team name strings.
    """
    all_teams = []
    failed_conferences = []

    for conf in D1_CONFERENCES:
        try:
            # Fixed — always pass explicit season year
            teams = cbb.get_teams_from_conference(conference=conf)
            if teams:
                all_teams.extend(teams)
            time.sleep(0.2)
        except Exception as e:
            print(f"  WARNING: Failed to fetch conference '{conf}': {e}")
            failed_conferences.append(conf)

    if failed_conferences:
        print(f"  {len(failed_conferences)} conferences failed: {failed_conferences}")

    all_teams = list(set(all_teams))
    print(f"  Found {len(all_teams)} D1 teams")
    return all_teams


# ── Core ingestion functions ───────────────────────────────────────────────────

def fetch_all_schedules(season: int) -> pd.DataFrame:
    """
    Fetch completed game results for all D1 teams for a given season.
    Uses retry logic per team and validates output before returning.
    """
    # For current season (2026) omit season param so cbbpy uses its default
    is_current_season = (season == 2026)
    schedule_season = None if is_current_season else season

    # Fixed — always pass the actual season year for conference lookup
    teams = get_all_d1_teams() 

    all_games = []
    failed_teams = []

    for i, team in enumerate(teams):
        try:
            schedule = fetch_team_schedule_with_retry(
                team=team,
                season=schedule_season
            )

            if schedule is None or schedule.empty:
                continue

            # Validate expected columns are present
            missing_cols = [c for c in EXPECTED_COLS if c not in schedule.columns]
            if missing_cols:
                print(f"  WARNING: {team} schedule missing columns: {missing_cols}")

            # Only keep completed games with a result string
            completed = schedule[
                schedule["game_result"].notna() &
                (schedule["game_result"] != "")
            ].copy()

            if completed.empty:
                continue

            # Parse scores from result string e.g. "W 85-72" or "L 68-74 (OT)"
            scores = completed["game_result"].str.extract(r'(\d+)-(\d+)')
            completed["team_score"] = pd.to_numeric(scores[0], errors="coerce")
            completed["opp_score"] = pd.to_numeric(scores[1], errors="coerce")
            completed["result_flag"] = completed["game_result"].str[0]
            completed["team_win"] = completed["result_flag"] == "W"

            # Drop rows where score parsing failed
            before = len(completed)
            completed = completed[
                completed["team_score"].notna() &
                completed["opp_score"].notna()
            ]
            dropped = before - len(completed)
            if dropped > 0:
                print(f"  WARNING: Dropped {dropped} unparseable score rows for {team}")

            completed["season"] = season
            all_games.append(completed)

        except Exception as e:
            print(f"  WARNING: Failed to fetch {team} season {season} after {MAX_RETRIES} retries: {e}")
            failed_teams.append(team)

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i + 1}/{len(teams)} teams")

        time.sleep(0.3)

    if failed_teams:
        print(f"  {len(failed_teams)} teams failed after all retries: {failed_teams}")

    if not all_games:
        print(f"  No game data collected for season {season}")
        return pd.DataFrame()

    df = pd.concat(all_games, ignore_index=True)

    # Normalize team names to BartTorvik format
    df["team_torvik"] = df["team"].apply(normalize_team_name)
    df["opponent_torvik"] = df["opponent"].apply(normalize_team_name)

    # Drop rows with null or empty game_id before deduplication
    before = len(df)
    df = df[df["game_id"].notna() & (df["game_id"].astype(str) != "")]
    null_dropped = before - len(df)
    if null_dropped > 0:
        print(f"  Dropped {null_dropped} rows with null/empty game_id")

    # Deduplicate — each game appears in multiple teams' schedules
    df = df.drop_duplicates(subset=["game_id"])

    print(f"  Season {season}: {len(df)} unique completed games from {len(teams)} teams")

    # Safety check — abort load if suspiciously few games returned
    if len(df) < MIN_EXPECTED_GAMES:
        print(
            f"  WARNING: Only {len(df)} games returned for season {season} "
            f"(expected >= {MIN_EXPECTED_GAMES}). "
            f"Skipping Snowflake load to protect existing data."
        )
        return pd.DataFrame()

    return df


def load_to_snowflake(df: pd.DataFrame, season: int):
    """
    Load game results into RAW.CBB_GAME_RESULTS.
    Deletes existing rows for the season before inserting fresh data.
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

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CBB_ANALYTICS.RAW.CBB_GAME_RESULTS (
            game_id          VARCHAR,
            season           INTEGER,
            game_day         VARCHAR,
            game_time        VARCHAR,
            team             VARCHAR,
            team_torvik      VARCHAR,
            opponent         VARCHAR,
            opponent_torvik  VARCHAR,
            team_score       FLOAT,
            opp_score        FLOAT,
            team_win         BOOLEAN,
            season_type      VARCHAR,
            loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Delete existing rows for this season before reloading
    cursor.execute(f"""
        DELETE FROM CBB_ANALYTICS.RAW.CBB_GAME_RESULTS
        WHERE season = {season}
    """)
    print(f"  Deleted {cursor.rowcount} existing rows for season {season}")

    # Select only columns that exist in both the dataframe and our schema
    target_cols = [
        "game_id", "season", "game_day", "game_time",
        "team", "team_torvik", "opponent", "opponent_torvik",
        "team_score", "opp_score", "team_win", "season_type",
    ]
    cols = [c for c in target_cols if c in df.columns]

    missing_schema_cols = [c for c in target_cols if c not in df.columns]
    if missing_schema_cols:
        print(f"  WARNING: These expected columns are missing from data and will be null: {missing_schema_cols}")

    rows = df[cols].values.tolist()

    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)
    insert_sql = f"""
        INSERT INTO CBB_ANALYTICS.RAW.CBB_GAME_RESULTS ({col_names})
        VALUES ({placeholders})
    """

    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        # Clean all values for safe Snowflake insertion
        clean_batch = [[clean_value(v) for v in row] for row in batch]
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
            df = fetch_all_schedules(season)
            if df.empty:
                print(f"  Skipping Snowflake load for season {season}")
                continue
            load_to_snowflake(df, season)
            print(f"  Season {season} complete ✓")
        except Exception as e:
            print(f"  ERROR for season {season}: {e}")
            raise


if __name__ == "__main__":
    run()