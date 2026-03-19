import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "CBB_ANALYTICS"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "DEV_MARTS"),
    )


def get_all_team_stats() -> pd.DataFrame:
    """
    Pull current season team stats from fct_tournament_profile.
    Returns one row per team with all metrics needed for prediction.
    """
    conn = get_connection()
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
            top_player_bpm,
            is_tournament_team,
            tournament_seed,
            tournament_region
        FROM CBB_ANALYTICS.DEV_MARTS.FCT_TOURNAMENT_PROFILE
        WHERE season = 2026
        ORDER BY consensus_adj_em DESC
    """)

    results = cursor.fetchall()
    columns = [col[0].lower() for col in cursor.description]
    cursor.close()
    conn.close()

    return pd.DataFrame(results, columns=columns)


def get_top5_by_team(team_name: str) -> pd.DataFrame:
    """
    Pull the top 5 players by minutes for a given team from fct_player_stats.
    Minimum 10 games played. Returns one row per player sorted by minutes descending.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            player_name,
            position,
            eligibility,
            pts_per_game,
            reb_per_game,
            ast_per_game,
            bpm,
            obpm,
            dbpm,
            ts_pct,
            usage_pct,
            minutes_pct
        FROM CBB_ANALYTICS.DEV_MARTS.FCT_PLAYER_STATS
        WHERE team_name = %s
          AND season = 2026
          AND games >= 10
        ORDER BY minutes_pct DESC
        LIMIT 5
    """, (team_name,))

    results = cursor.fetchall()
    columns = [col[0].lower() for col in cursor.description]
    cursor.close()
    conn.close()

    return pd.DataFrame(results, columns=columns)


def get_team_game_results(team_name: str, n: int | None = None) -> pd.DataFrame:
    """
    Pull all completed games for a team this season from CBB_GAME_RESULTS.
    Joins fct_team_ratings to get opponent's consensus ranking.
    Returns: game_day, opponent, game_location, opp_rank, result
    Pass n to limit the number of results returned.
    """
    conn = get_connection()
    cursor = conn.cursor()
    limit_clause = f"LIMIT {int(n)}" if n is not None else ""
    cursor.execute(f"""
        WITH ranked_teams AS (
            SELECT team_name,
                   RANK() OVER (ORDER BY (kenpom_adj_em + torvik_adj_em) / 2 DESC) AS rk
            FROM CBB_ANALYTICS.DEV_MARTS.FCT_TEAM_RATINGS
            WHERE season = 2026
        )
        SELECT
            g.game_day                                          AS game_day,
            g.opponent_torvik                                   AS opponent,
            g.game_location                                     AS location,
            rt.rk                                              AS opp_rank,
            CASE WHEN g.team_win THEN 'W' ELSE 'L' END
                || ' ' || CAST(g.team_score AS INTEGER)
                || '-' || CAST(g.opp_score  AS INTEGER)        AS result
        FROM CBB_ANALYTICS.RAW.CBB_GAME_RESULTS g
        LEFT JOIN ranked_teams rt ON rt.team_name = g.opponent_torvik
        WHERE g.team_torvik = %s
          AND g.season      = 2026
        ORDER BY TRY_TO_DATE(g.game_day, 'MM/DD/YY') DESC
        {limit_clause}
    """, (team_name,))

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
        location: 'neutral', 'home' (team_a at home), or 'away' (team_a away)

    Returns:
        dict with win probabilities, predicted scores, margin, and metric breakdown
    """

    # Location adjustment — home court worth ~3.5 points historically
    location_adjustment = {
        "neutral": 0,
        "home":    3.5,   # team_a is at home
        "away":   -3.5    # team_a is away
    }
    loc_adj = location_adjustment.get(location, 0)

    # Core efficiency margin differential (includes location adjustment)
    em_diff = (
        team_a["consensus_adj_em"] - team_b["consensus_adj_em"] + loc_adj
    )

    # Win probability from log5 formula — divisor of 15 calibrated to college basketball
    win_prob_a = 1 / (1 + 10 ** (-em_diff / 15))
    win_prob_b = 1 - win_prob_a

    # --- Score prediction ---
    # Tempo: average both teams' tempos, apply slight possession reduction
    avg_tempo = (
        team_a.get("kenpom_tempo", 68) + team_b.get("kenpom_tempo", 68)
    ) / 2
    possessions = avg_tempo * 0.98

    # Average each team's offensive efficiency (consensus of KenPom + Torvik)
    team_a_adj_o = (
        team_a.get("kenpom_off_efficiency", 100) +
        team_a.get("torvik_off_efficiency", 100)
    ) / 2
    team_b_adj_o = (
        team_b.get("kenpom_off_efficiency", 100) +
        team_b.get("torvik_off_efficiency", 100)
    ) / 2

    # Predicted margin derived directly from em_diff.
    # AdjEM is points per 100 possessions, so scaling to actual possessions
    # gives us the expected point margin. This keeps margin internally consistent
    # with win probability (both are anchored to em_diff).
    predicted_margin = em_diff * (possessions / 100)

    # Baseline score: average of both teams' offensive efficiencies sets the
    # overall scoring level for the game, then split the margin around it.
    baseline_score = ((team_a_adj_o + team_b_adj_o) / 2) / 100 * possessions
    team_a_score = round(baseline_score + predicted_margin / 2, 1)
    team_b_score = round(baseline_score - predicted_margin / 2, 1)

    # Metric-by-metric breakdown — who has the edge on each dimension
    breakdown = {
        "Efficiency Margin": {
            "team_a": round(team_a["consensus_adj_em"], 1),
            "team_b": round(team_b["consensus_adj_em"], 1),
            "edge": team_a["team_name"] if em_diff > 0 else team_b["team_name"],
            "diff": abs(round(em_diff - loc_adj, 1))
        },
        "Offense": {
            "team_a": round(team_a_adj_o, 1),
            "team_b": round(team_b_adj_o, 1),
            "edge": team_a["team_name"] if team_a_adj_o > team_b_adj_o else team_b["team_name"],
            "diff": abs(round(team_a_adj_o - team_b_adj_o, 1))
        },
        "Defense": {
            "team_a": round((team_a.get("kenpom_def_efficiency", 100) + team_a.get("torvik_def_efficiency", 100)) / 2, 1),
            "team_b": round((team_b.get("kenpom_def_efficiency", 100) + team_b.get("torvik_def_efficiency", 100)) / 2, 1),
            "edge": team_a["team_name"] if (team_a.get("kenpom_def_efficiency", 100) + team_a.get("torvik_def_efficiency", 100)) < (team_b.get("kenpom_def_efficiency", 100) + team_b.get("torvik_def_efficiency", 100)) else team_b["team_name"],
            "diff": abs(round(
                ((team_a.get("kenpom_def_efficiency", 100) + team_a.get("torvik_def_efficiency", 100)) -
                 (team_b.get("kenpom_def_efficiency", 100) + team_b.get("torvik_def_efficiency", 100))) / 2, 1
            ))
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

    for team_name, stats in teams.items():
        stats["team_name"] = team_name

    result = predict_matchup(teams["Duke"], teams["Michigan"])

    print(f"\n{result['team_a']} vs {result['team_b']}")
    print(f"Predicted winner:  {result['predicted_winner']}")
    print(f"Predicted score:   {result['team_a_score']} - {result['team_b_score']}")
    print(f"Predicted margin:  {result['predicted_margin']} pts")
    print(f"Win probability:   {result['team_a']} {result['team_a_win_prob']*100:.1f}% / {result['team_b']} {result['team_b_win_prob']*100:.1f}%")
    print(f"\nMetric breakdown:")
    for metric, values in result['breakdown'].items():
        print(f"  {metric}: {values['team_a']} vs {values['team_b']} → Edge: {values['edge']} (+{values['diff']})")

    print(f"\nTop 5 players — Duke:")
    print(get_top5_by_team("Duke").to_string(index=False))