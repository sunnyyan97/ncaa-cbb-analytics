import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def get_all_team_stats() -> pd.DataFrame:
    """
    Pull current season team stats from fct_tournament_profile.
    Returns one row per team with all metrics needed for prediction.
    """
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "CBB_ANALYTICS"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "DEV_MARTS"),
    )

    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            team_name,
            conference,
            record,
            consensus_adj_em,
            kenpom_adj_em,
            torvik_adj_em,
            kenpom_off_efficiency,
            kenpom_def_efficiency,
            torvik_off_efficiency,
            torvik_def_efficiency,
            barthag,
            kenpom_tempo,
            torvik_tempo,
            sos,
            wins_above_bubble,
            avg_bpm,
            avg_obpm,
            avg_dbpm,
            avg_pts,
            avg_ts_pct,
            avg_efg_pct,
            experienced_players,
            top_player_name,
            top_player_bpm
        FROM CBB_ANALYTICS.DEV_MARTS.FCT_TOURNAMENT_PROFILE
        WHERE season = 2026
        ORDER BY consensus_adj_em DESC
    """)

    results = cursor.fetchall()
    columns = [col[0].lower() for col in cursor.description]
    cursor.close()
    conn.close()

    return pd.DataFrame(results, columns=columns)


def predict_matchup(team_a: dict, team_b: dict, location: str = "neutral") -> dict:
    """
    Predict the outcome of a matchup between two teams using
    the formula-based baseline model.

    Args:
        team_a: dict of team stats from fct_tournament_profile
        team_b: dict of team stats from fct_tournament_profile
        location: 'neutral', 'home' (team_a), or 'away' (team_a)

    Returns:
        dict with win probabilities, predicted margin, and metric breakdown
    """

    # Location adjustment — home court worth ~3.5 points historically
    location_adjustment = {
        "neutral": 0,
        "home": 3.5,    # team_a is at home
        "away": -3.5    # team_a is away
    }
    loc_adj = location_adjustment.get(location, 0)

    # Core efficiency margin differential
    em_diff = (
        team_a["consensus_adj_em"] - team_b["consensus_adj_em"] + loc_adj
    )

    # Win probability from log5 formula
    # Divisor of 15 calibrated to college basketball
    win_prob_a = 1 / (1 + 10 ** (-em_diff / 15))
    win_prob_b = 1 - win_prob_a

    # Predicted margin using linear relationship
    # Roughly 1 point of margin per 3 AdjEM points
    predicted_margin = em_diff / 3

    # Predicted scores using tempo and efficiency
    avg_tempo = (
        team_a.get("kenpom_tempo", 68) + team_b.get("kenpom_tempo", 68)
    ) / 2

    # Points = (offensive efficiency / 100) * possessions
    possessions = avg_tempo * 0.98  # slight reduction for turnovers

    team_a_off = (team_a.get("kenpom_off_efficiency", 100) + 
                  team_a.get("torvik_off_efficiency", 100)) / 2
    team_a_def = (team_a.get("kenpom_def_efficiency", 100) + 
                  team_a.get("torvik_def_efficiency", 100)) / 2
    team_b_off = (team_b.get("kenpom_off_efficiency", 100) + 
                  team_b.get("torvik_off_efficiency", 100)) / 2
    team_b_def = (team_b.get("kenpom_def_efficiency", 100) + 
                  team_b.get("torvik_def_efficiency", 100)) / 2

    # Each team's score accounts for both their offense and opponent's defense
    team_a_score = round(
        ((team_a_off + team_b_def) / 2 / 100) * possessions, 1
    )
    team_b_score = round(
        ((team_b_off + team_a_def) / 2 / 100) * possessions, 1
    )

    # Metric-by-metric breakdown — who has the edge on each dimension
    breakdown = {
        "Efficiency Margin": {
            "team_a": round(team_a["consensus_adj_em"], 1),
            "team_b": round(team_b["consensus_adj_em"], 1),
            "edge": team_a["team_name"] if em_diff > 0 else team_b["team_name"],
            "diff": abs(round(em_diff - loc_adj, 1))
        },
        "Offense": {
            "team_a": round(team_a_off, 1),
            "team_b": round(team_b_off, 1),
            "edge": team_a["team_name"] if team_a_off > team_b_off else team_b["team_name"],
            "diff": abs(round(team_a_off - team_b_off, 1))
        },
        "Defense": {
            "team_a": round(team_a_def, 1),
            "team_b": round(team_b_def, 1),
            "edge": team_a["team_name"] if team_a_def < team_b_def else team_b["team_name"],
            "diff": abs(round(team_a_def - team_b_def, 1))
        },
        "Barthag": {
            "team_a": round(team_a.get("barthag", 0), 3),
            "team_b": round(team_b.get("barthag", 0), 3),
            "edge": team_a["team_name"] if team_a.get("barthag", 0) > team_b.get("barthag", 0) else team_b["team_name"],
            "diff": abs(round(team_a.get("barthag", 0) - team_b.get("barthag", 0), 3))
        },
        "Starting Five BPM": {
            "team_a": round(team_a.get("avg_bpm", 0), 2),
            "team_b": round(team_b.get("avg_bpm", 0), 2),
            "edge": team_a["team_name"] if team_a.get("avg_bpm", 0) > team_b.get("avg_bpm", 0) else team_b["team_name"],
            "diff": abs(round(team_a.get("avg_bpm", 0) - team_b.get("avg_bpm", 0), 2))
        },
        "Experience": {
            "team_a": int(team_a.get("experienced_players", 0)),
            "team_b": int(team_b.get("experienced_players", 0)),
            "edge": team_a["team_name"] if team_a.get("experienced_players", 0) > team_b.get("experienced_players", 0) else team_b["team_name"],
            "diff": abs(int(team_a.get("experienced_players", 0) - team_b.get("experienced_players", 0)))
        },
        "Strength of Schedule": {
            "team_a": round(team_a.get("sos", 0), 3),
            "team_b": round(team_b.get("sos", 0), 3),
            "edge": team_a["team_name"] if team_a.get("sos", 0) > team_b.get("sos", 0) else team_b["team_name"],
            "diff": abs(round(team_a.get("sos", 0) - team_b.get("sos", 0), 3))
        },
    }

    return {
        "team_a": team_a["team_name"],
        "team_b": team_b["team_name"],
        "team_a_win_prob": round(win_prob_a, 3),
        "team_b_win_prob": round(win_prob_b, 3),
        "team_a_score": team_a_score,
        "team_b_score": team_b_score,
        "predicted_margin": round(abs(predicted_margin), 1),
        "predicted_winner": team_a["team_name"] if predicted_margin > 0 else team_b["team_name"],
        "location": location,
        "breakdown": breakdown
    }


if __name__ == "__main__":
    # Quick test — Duke vs Michigan
    df = get_all_team_stats()
    teams = df.set_index("team_name").to_dict("index")

# Re-add team_name as a field inside each dict
    for team_name, stats in teams.items():
        stats["team_name"] = team_name

    result = predict_matchup(teams["Duke"], teams["Michigan"])

    print(f"\n{result['team_a']} vs {result['team_b']}")
    print(f"Predicted winner: {result['predicted_winner']}")
    print(f"Predicted score: {result['team_a_score']} - {result['team_b_score']}")
    print(f"Win probability: {result['team_a']} {result['team_a_win_prob']*100:.1f}% / {result['team_b']} {result['team_b_win_prob']*100:.1f}%")
    print(f"\nMetric breakdown:")
    for metric, values in result['breakdown'].items():
        print(f"  {metric}: {values['team_a']} vs {values['team_b']} → Edge: {values['edge']} (+{values['diff']})")