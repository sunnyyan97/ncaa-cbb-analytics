"""
generate_bracket_data.py — Run this on Selection Sunday.

WORKFLOW:
    1. Fill in BRACKET_INPUT below with the real 68 teams (seeds + names).
       Teams must be listed in SEED_ORDER for each region (always the same every year).
    2. Run:  python generate_bracket_data.py
    3. This overwrites bracket_data.py with real data and sets BRACKET_LOCKED = True.
    4. Run:  python render_bracket.py   to visually verify in the browser.
    5. Restart Streamlit — done.

SEED_ORDER reference (always 1v16, 8v9, 5v12, 4v13, 6v11, 3v14, 7v10, 2v15):
    Slot:  0    1    2   3   4    5   6    7   8    9   10   11   12   13   2   15
    Seed:  1   16    8   9   5   12   4   13   6   11   3   14    7   10   2   15
"""

import time
import textwrap
import os
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv
from modeling.predict import get_all_team_stats, predict_matchup
from modeling.simulate import simulate_tournament, get_team_safe

load_dotenv()

SEASON = 2026


def write_seeds_to_snowflake(bracket: dict, round_results: dict | None = None) -> None:
    """
    Truncate and repopulate CBB_ANALYTICS.RAW.TOURNAMENT_SEEDS with the
    68 tournament teams, their seeds, regions, and status.

    Teams that appear as losers in round_results are marked status='eliminated'.
    All other tournament teams are marked status='active'.
    """
    # Build set of eliminated teams from round_results
    eliminated = set()
    for region_name, region_teams in bracket.items():
        region_known = (round_results or {}).get(region_name, {})
        current = [t["team"] for t in region_teams]
        for round_key in ["R64", "R32", "S16", "E8"]:
            winners = region_known.get(round_key, [])
            if not winners:
                break
            next_round = []
            for slot, i in enumerate(range(0, len(current), 2)):
                if slot < len(winners):
                    winner = winners[slot]
                    loser = current[i] if winner == current[i + 1] else current[i + 1]
                    eliminated.add(loser)
                    next_round.append(winner)
            current = next_round

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "CBB_ANALYTICS"),
        schema="RAW",
    )
    cursor = conn.cursor()

    # Clear existing season data
    cursor.execute(
        "DELETE FROM CBB_ANALYTICS.RAW.TOURNAMENT_SEEDS WHERE season = %s",
        (SEASON,)
    )

    # Insert one row per team with correct status
    rows = [
        (team["team"], team["seed"], region,
         "eliminated" if team["team"] in eliminated else "active",
         SEASON)
        for region, teams in bracket.items()
        for team in teams
    ]
    cursor.executemany(
        "INSERT INTO CBB_ANALYTICS.RAW.TOURNAMENT_SEEDS "
        "(team_name, seed, region, status, season) VALUES (%s, %s, %s, %s, %s)",
        rows
    )

    conn.commit()
    cursor.close()
    conn.close()
    n_elim = len(eliminated)
    print(f"      ✓ {len(rows)} teams written to RAW.TOURNAMENT_SEEDS "
          f"({n_elim} eliminated, {len(rows) - n_elim} active).")



# ─────────────────────────────────────────────────────────────────────────────
# FILL THIS IN ON SUNDAY — one entry per team, in seed order shown above.
# ─────────────────────────────────────────────────────────────────────────────
BRACKET_INPUT = {
    "East": [
        {"seed": 1,  "team": "Duke"},
        {"seed": 16, "team": "Siena"},
        {"seed": 8,  "team": "Ohio St."},
        {"seed": 9,  "team": "TCU"},
        {"seed": 5,  "team": "St. John's"},
        {"seed": 12, "team": "Northern Iowa"},
        {"seed": 4,  "team": "Kansas"},
        {"seed": 13, "team": "Cal Baptist"},
        {"seed": 6,  "team": "Louisville"},
        {"seed": 11, "team": "South Florida"},
        {"seed": 3,  "team": "Michigan St."},
        {"seed": 14, "team": "North Dakota St."},
        {"seed": 7,  "team": "UCLA"},
        {"seed": 10, "team": "UCF"},
        {"seed": 2,  "team": "Connecticut"},
        {"seed": 15, "team": "Furman"},
    ],
    "West": [
        {"seed": 1,  "team": "Arizona"},
        {"seed": 16, "team": "LIU"},
        {"seed": 8,  "team": "Villanova"},
        {"seed": 9,  "team": "Utah St."},
        {"seed": 5,  "team": "Wisconsin"},
        {"seed": 12, "team": "High Point"},
        {"seed": 4,  "team": "Arkansas"},
        {"seed": 13, "team": "Hawaii"},
        {"seed": 6,  "team": "BYU"},
        {"seed": 11, "team": "Texas"},  # First Four winner (Texas beat N.C. State)
        {"seed": 3,  "team": "Gonzaga"},
        {"seed": 14, "team": "Kennesaw St."},
        {"seed": 7,  "team": "Miami FL"},
        {"seed": 10, "team": "Missouri"},
        {"seed": 2,  "team": "Purdue"},
        {"seed": 15, "team": "Queens"},
    ],
    "South": [
        {"seed": 1,  "team": "Florida"},
        {"seed": 16, "team": "Lehigh"},  # First Four: Prairie View A&M vs Lehigh — Lehigh (AdjEM -10.31) marginal favorite
        {"seed": 8,  "team": "Clemson"},
        {"seed": 9,  "team": "Iowa"},
        {"seed": 5,  "team": "Vanderbilt"},
        {"seed": 12, "team": "McNeese St."},
        {"seed": 4,  "team": "Nebraska"},
        {"seed": 13, "team": "Troy"},
        {"seed": 6,  "team": "North Carolina"},
        {"seed": 11, "team": "VCU"},
        {"seed": 3,  "team": "Illinois"},
        {"seed": 14, "team": "Penn"},
        {"seed": 7,  "team": "Saint Mary's"},
        {"seed": 10, "team": "Texas A&M"},
        {"seed": 2,  "team": "Houston"},
        {"seed": 15, "team": "Idaho"},
    ],
    "Midwest": [
        {"seed": 1,  "team": "Michigan"},
        {"seed": 16, "team": "UMBC"},  # First Four: UMBC vs Howard — UMBC (AdjEM -1.70) favored
        {"seed": 8,  "team": "Georgia"},
        {"seed": 9,  "team": "Saint Louis"},
        {"seed": 5,  "team": "Texas Tech"},
        {"seed": 12, "team": "Akron"},
        {"seed": 4,  "team": "Alabama"},
        {"seed": 13, "team": "Hofstra"},
        {"seed": 6,  "team": "Tennessee"},
        {"seed": 11, "team": "SMU"},  # First Four: Miami OH vs SMU — SMU (AdjEM 18.11) favored
        {"seed": 3,  "team": "Virginia"},
        {"seed": 14, "team": "Wright St."},
        {"seed": 7,  "team": "Kentucky"},
        {"seed": 10, "team": "Santa Clara"},
        {"seed": 2,  "team": "Iowa St."},
        {"seed": 15, "team": "Tennessee St."},
    ],
}

N_SIMULATIONS = 10_000

# ─────────────────────────────────────────────────────────────────────────────
# FILL IN AFTER EACH ROUND — add actual winners in game-slot order.
# Slot order matches BRACKET_INPUT region order (same seed-pair ordering).
# Leave lists empty for rounds that haven't been played yet.
# Example after R64 East: "R64": ["Duke", "TCU", "St. John's", "Kansas", ...]
# ─────────────────────────────────────────────────────────────────────────────
ROUND_RESULTS = {
    "East":    {"R64": ["Duke", "TCU", "St. John's", "Kansas", "Louisville", "Michigan St.", "UCLA", "Connecticut"], "R32": [], "S16": [], "E8": []},
    "West":    {"R64": ["Arizona", "Utah St.", "High Point", "Arkansas", "Texas", "Gonzaga", "Miami FL", "Purdue"], "R32": [], "S16": [], "E8": []},
    "South":   {"R64": ["Florida", "Iowa", "Vanderbilt", "Nebraska", "VCU", "Illinois", "Texas A&M", "Houston"], "R32": [], "S16": [], "E8": []},
    "Midwest": {"R64": ["Michigan", "Saint Louis", "Texas Tech", "Alabama", "Tennessee", "Virginia", "Kentucky", "Iowa St."], "R32": [], "S16": [], "E8": []},
}
# Final Four actual results: [East/South winner, West/Midwest winner]
# Matches FINAL_FOUR_PAIRINGS order: (East vs South), (West vs Midwest)
F4_RESULTS: list = []   # e.g. ["Florida", "Duke"] once F4 is played
CHAMP_RESULT: str = ""  # e.g. "Duke" once championship is played


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def validate_bracket_input(bracket: dict) -> None:
    """Catch obvious mistakes before running a 10k simulation."""
    for region, teams in bracket.items():
        if len(teams) != 16:
            raise ValueError(f"{region} has {len(teams)} teams, expected 16.")
        for slot in teams:
            if not slot["team"].strip():
                raise ValueError(
                    f"{region} seed {slot['seed']} has an empty team name. "
                    "Fill in all teams before running."
                )
        names = [t["team"] for t in teams]
        if len(names) != len(set(names)):
            raise ValueError(f"{region} contains duplicate team names: {names}")


def build_seed_lookup(bracket: dict) -> dict:
    """Return {team_name: seed} across all regions."""
    lookup = {}
    for teams in bracket.values():
        for t in teams:
            lookup[t["team"]] = t["seed"]
    return lookup


def get_favorite(team_a: str, team_b: str, teams_stats: dict) -> tuple:
    """
    Return (favorite, underdog) using predict_matchup win probability.
    Used to walk the most likely bracket path forward round by round.
    """
    stats_a = get_team_safe(teams_stats, team_a)
    stats_b = get_team_safe(teams_stats, team_b)
    result  = predict_matchup(stats_a, stats_b, location="neutral")
    if result["team_a_win_prob"] >= 0.5:
        return team_a, team_b
    return team_b, team_a


def is_upset(winner: str, loser: str, seed_lookup: dict) -> bool:
    """True if the winner had a higher seed number (lower seed = worse)."""
    return seed_lookup.get(winner, 99) > seed_lookup.get(loser, 0)


def build_regions_data(
    bracket: dict,
    teams_stats: dict,
    seed_lookup: dict,
    round_results: dict | None = None,
) -> dict:
    """
    Walk each region round by round, picking the most likely winner at each
    game slot via predict_matchup. Returns the full REGIONS_DATA structure.

    round_results: optional dict {region → {round_name → [winner, ...]}}
        When provided, actual winners are used for completed rounds instead of
        model predictions. Each game in a completed round is marked "played": True.
    """
    round_results = round_results or {}
    regions_data = {}

    for region_name, region_teams in bracket.items():
        region = {"R64": [], "R32": [], "S16": [], "E8": [], "champion": ""}
        region_known = round_results.get(region_name, {})

        # R64 — derive directly from bracket input; seed info available
        r64_known = region_known.get("R64", [])
        r64_games = []
        for slot, i in enumerate(range(0, 16, 2)):
            team_a_entry = region_teams[i]
            team_b_entry = region_teams[i + 1]
            if slot < len(r64_known):
                winner = r64_known[slot]
                played = True
            else:
                winner, _ = get_favorite(team_a_entry["team"], team_b_entry["team"], teams_stats)
                played = False
            loser = team_b_entry["team"] if winner == team_a_entry["team"] else team_a_entry["team"]
            r64_games.append({
                "a":      {"seed": team_a_entry["seed"], "team": team_a_entry["team"]},
                "b":      {"seed": team_b_entry["seed"], "team": team_b_entry["team"]},
                "winner": winner,
                "upset":  is_upset(winner, loser, seed_lookup),
                "played": played,
            })
        region["R64"] = r64_games

        # R32, S16, E8 — advance winners from previous round
        prev_winners = [g["winner"] for g in r64_games]
        for round_key in ["R32", "S16", "E8"]:
            known = region_known.get(round_key, [])
            games = []
            next_winners = []
            for slot, i in enumerate(range(0, len(prev_winners), 2)):
                team_a = prev_winners[i]
                team_b = prev_winners[i + 1]
                if slot < len(known):
                    winner = known[slot]
                    played = True
                else:
                    winner, _ = get_favorite(team_a, team_b, teams_stats)
                    played = False
                loser = team_a if winner == team_b else team_b
                games.append({
                    "a":      {"seed": seed_lookup.get(team_a), "team": team_a},
                    "b":      {"seed": seed_lookup.get(team_b), "team": team_b},
                    "winner": winner,
                    "upset":  is_upset(winner, loser, seed_lookup),
                    "played": played,
                })
                next_winners.append(winner)
            region[round_key] = games
            prev_winners = next_winners

        region["champion"] = prev_winners[0]
        regions_data[region_name] = region

    return regions_data


def build_f4_and_champ(
    regions_data: dict,
    teams_stats: dict,
    seed_lookup: dict,
    f4_results: list | None = None,
    champ_result: str = "",
) -> tuple:
    """
    Build F4_DATA, CHAMP_DATA, and CHAMPION from regional champions.
    Pairings: East vs South (game 0, LEFT), West vs Midwest (game 1, RIGHT).

    f4_results: optional list of 2 actual F4 winners [East/South winner, West/Midwest winner]
    champ_result: optional actual championship winner
    """
    east_champ    = regions_data["East"]["champion"]
    west_champ    = regions_data["West"]["champion"]
    south_champ   = regions_data["South"]["champion"]
    midwest_champ = regions_data["Midwest"]["champion"]

    if f4_results and len(f4_results) >= 1:
        f4_es_winner = f4_results[0]
        f4_es_played = True
    else:
        f4_es_winner, _ = get_favorite(east_champ, south_champ, teams_stats)
        f4_es_played = False

    if f4_results and len(f4_results) >= 2:
        f4_wm_winner = f4_results[1]
        f4_wm_played = True
    else:
        f4_wm_winner, _ = get_favorite(west_champ, midwest_champ, teams_stats)
        f4_wm_played = False

    f4_data = [
        {"a": {"seed": seed_lookup.get(east_champ),    "team": east_champ},
         "b": {"seed": seed_lookup.get(south_champ),   "team": south_champ},
         "winner": f4_es_winner, "played": f4_es_played},   # LEFT
        {"a": {"seed": seed_lookup.get(west_champ),    "team": west_champ},
         "b": {"seed": seed_lookup.get(midwest_champ), "team": midwest_champ},
         "winner": f4_wm_winner, "played": f4_wm_played},   # RIGHT
    ]

    if champ_result:
        champion  = champ_result
        champ_played = True
    else:
        champion, _ = get_favorite(f4_es_winner, f4_wm_winner, teams_stats)
        champ_played = False
    runner_up = f4_wm_winner if champion == f4_es_winner else f4_es_winner

    champ_data = {
        "a": {"seed": seed_lookup.get(f4_es_winner), "team": f4_es_winner},
        "b": {"seed": seed_lookup.get(f4_wm_winner), "team": f4_wm_winner},
        "winner": champion,
        "played": champ_played,
    }

    return f4_data, champ_data, champion


def build_sim_data(
    sim_results: dict,
    bracket: dict,
    seed_lookup: dict,
    top_n: int = 64,
) -> list:
    """
    Convert simulate_tournament output into the SIM_DATA list format.
    Returns top_n teams sorted by championship probability.
    """
    # Build region lookup
    region_lookup = {}
    for region_name, teams in bracket.items():
        for t in teams:
            region_lookup[t["team"]] = region_name

    rows = []
    for team, probs in sim_results.items():
        rows.append({
            "team":     team,
            "region":   region_lookup.get(team, ""),
            "seed":     seed_lookup.get(team, 0),
            "R32":      round(probs.get("R32", 0.0), 4),   # P(won R64 game, made R32)
            "S16":      round(probs.get("S16", 0.0), 4),   # P(won R32 game, made S16)
            "E8":       round(probs.get("E8",  0.0), 4),   # P(won S16 game, made E8)
            "F4":       round(probs.get("F4",  0.0), 4),   # P(won E8 game, made F4)
            "Champion": round(probs.get("Champion", 0.0), 4),
        })

    rows.sort(key=lambda x: x["Champion"], reverse=True)
    return rows[:top_n]


def write_bracket_data(
    regions_data: dict,
    f4_data: list,
    champ_data: dict,
    champion: str,
    sim_data: list,
    sim_meta: dict,
    output_path: Path,
) -> None:
    """Write the fully populated bracket_data.py file."""

    def fmt(obj, indent=0) -> str:
        """Pretty-format Python literals for bracket_data.py."""
        pad = "    " * indent
        inner = "    " * (indent + 1)

        if isinstance(obj, dict):
            if not obj:
                return "{}"
            items = ",\n".join(f'{inner}"{k}": {fmt(v, indent+1)}' for k, v in obj.items())
            return f"{{\n{items},\n{pad}}}"
        if isinstance(obj, list):
            if not obj:
                return "[]"
            items = ",\n".join(f"{inner}{fmt(v, indent+1)}" for v in obj)
            return f"[\n{items},\n{pad}]"
        if isinstance(obj, bool):
            return "True" if obj else "False"
        if isinstance(obj, str):
            return f'"{obj}"'
        return repr(obj)

    lines = [
        '# ─── Bracket Data ───────────────────────────────────────────────────────────',
        '# Auto-generated by generate_bracket_data.py on Selection Sunday.',
        '# Do not edit manually — re-run the script to regenerate.',
        '',
        'BRACKET_LOCKED = True',
        '',
        '# ── Simulation metadata ───────────────────────────────────────────────────────',
        f'SIM_META = {fmt(sim_meta)}',
        '',
        '# ── Round-by-round probabilities ─────────────────────────────────────────────',
        f'SIM_DATA = {fmt(sim_data)}',
        '',
        '# ── Per-region bracket (most likely outcome path) ────────────────────────────',
        f'REGIONS_DATA = {fmt(regions_data)}',
        '',
        '# ── Final Four & Championship ─────────────────────────────────────────────────',
        f'F4_DATA = {fmt(f4_data)}',
        '',
        f'CHAMP_DATA = {fmt(champ_data)}',
        '',
        f'CHAMPION = "{champion}"',
        '',
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("─" * 60)
    print("BRACKET DATA GENERATOR")
    print("─" * 60)

    # 1. Validate input
    print("\n[1/5] Validating bracket input...")
    validate_bracket_input(BRACKET_INPUT)
    print("      ✓ All 64 teams present, no duplicates.")

    seed_lookup = build_seed_lookup(BRACKET_INPUT)

    # 2. Load team stats from Snowflake
    print("\n[2/5] Loading team stats from Snowflake...")
    df          = get_all_team_stats()
    teams_stats = df.set_index("team_name").to_dict("index")
    for name, stats in teams_stats.items():
        stats["team_name"] = name
    print(f"      ✓ Loaded {len(teams_stats)} teams.")

    # 3. Build most likely bracket path (using actual results where available)
    print("\n[3/5] Building bracket path via predict_matchup (actual results applied)...")
    regions_data       = build_regions_data(BRACKET_INPUT, teams_stats, seed_lookup,
                                            round_results=ROUND_RESULTS)
    f4_data, champ_data, champion = build_f4_and_champ(regions_data, teams_stats, seed_lookup,
                                                        f4_results=F4_RESULTS,
                                                        champ_result=CHAMP_RESULT)
    print(f"      ✓ Projected champion: {champion}")

    # 4. Run Monte Carlo simulation
    print(f"\n[4/5] Running {N_SIMULATIONS:,} simulations...")
    t0          = time.time()
    sim_results = simulate_tournament(BRACKET_INPUT, teams_stats, n_simulations=N_SIMULATIONS,
                                      known_results=ROUND_RESULTS,
                                      f4_results=F4_RESULTS,
                                      champ_result=CHAMP_RESULT)
    elapsed     = round(time.time() - t0, 1)
    print(f"      ✓ Done in {elapsed}s.")

    sim_meta = {
        "n_trials":    N_SIMULATIONS,
        "runtime_sec": elapsed,
        "model":       "Formula-based log5",
        "season":      "2025–26 NCAA Tournament",
    }

    # 5. Build SIM_DATA and write bracket_data.py
    print("\n[5/6] Writing bracket_data.py...")
    sim_data    = build_sim_data(sim_results, BRACKET_INPUT, seed_lookup, top_n=64)
    output_path = Path(__file__).parent / "bracket_data.py"
    write_bracket_data(
        regions_data=regions_data,
        f4_data=f4_data,
        champ_data=champ_data,
        champion=champion,
        sim_data=sim_data,
        sim_meta=sim_meta,
        output_path=output_path,
    )
    print(f"      ✓ bracket_data.py written to {output_path}")

    # 6. Write tournament seeds to Snowflake
    print("\n[6/6] Writing tournament seeds to Snowflake...")
    write_seeds_to_snowflake(BRACKET_INPUT, round_results=ROUND_RESULTS)

    print("\n─" * 60)
    print("NEXT STEPS:")
    print("  1. python render_bracket.py   → open bracket_resolved_debug.html to verify")
    print("  2. streamlit run app.py        → check the live tab")
    print("─" * 60)