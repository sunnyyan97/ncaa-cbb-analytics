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
from pathlib import Path
from modeling.predict import get_all_team_stats, predict_matchup
from simulate import simulate_tournament, get_team_safe

# ─────────────────────────────────────────────────────────────────────────────
# FILL THIS IN ON SUNDAY — one entry per team, in seed order shown above.
# ─────────────────────────────────────────────────────────────────────────────
BRACKET_INPUT = {
    "East": [
        {"seed": 1,  "team": ""},
        {"seed": 16, "team": ""},
        {"seed": 8,  "team": ""},
        {"seed": 9,  "team": ""},
        {"seed": 5,  "team": ""},
        {"seed": 12, "team": ""},
        {"seed": 4,  "team": ""},
        {"seed": 13, "team": ""},
        {"seed": 6,  "team": ""},
        {"seed": 11, "team": ""},
        {"seed": 3,  "team": ""},
        {"seed": 14, "team": ""},
        {"seed": 7,  "team": ""},
        {"seed": 10, "team": ""},
        {"seed": 2,  "team": ""},
        {"seed": 15, "team": ""},
    ],
    "West": [
        {"seed": 1,  "team": ""},
        {"seed": 16, "team": ""},
        {"seed": 8,  "team": ""},
        {"seed": 9,  "team": ""},
        {"seed": 5,  "team": ""},
        {"seed": 12, "team": ""},
        {"seed": 4,  "team": ""},
        {"seed": 13, "team": ""},
        {"seed": 6,  "team": ""},
        {"seed": 11, "team": ""},
        {"seed": 3,  "team": ""},
        {"seed": 14, "team": ""},
        {"seed": 7,  "team": ""},
        {"seed": 10, "team": ""},
        {"seed": 2,  "team": ""},
        {"seed": 15, "team": ""},
    ],
    "South": [
        {"seed": 1,  "team": ""},
        {"seed": 16, "team": ""},
        {"seed": 8,  "team": ""},
        {"seed": 9,  "team": ""},
        {"seed": 5,  "team": ""},
        {"seed": 12, "team": ""},
        {"seed": 4,  "team": ""},
        {"seed": 13, "team": ""},
        {"seed": 6,  "team": ""},
        {"seed": 11, "team": ""},
        {"seed": 3,  "team": ""},
        {"seed": 14, "team": ""},
        {"seed": 7,  "team": ""},
        {"seed": 10, "team": ""},
        {"seed": 2,  "team": ""},
        {"seed": 15, "team": ""},
    ],
    "Midwest": [
        {"seed": 1,  "team": ""},
        {"seed": 16, "team": ""},
        {"seed": 8,  "team": ""},
        {"seed": 9,  "team": ""},
        {"seed": 5,  "team": ""},
        {"seed": 12, "team": ""},
        {"seed": 4,  "team": ""},
        {"seed": 13, "team": ""},
        {"seed": 6,  "team": ""},
        {"seed": 11, "team": ""},
        {"seed": 3,  "team": ""},
        {"seed": 14, "team": ""},
        {"seed": 7,  "team": ""},
        {"seed": 10, "team": ""},
        {"seed": 2,  "team": ""},
        {"seed": 15, "team": ""},
    ],
}

N_SIMULATIONS = 10_000


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


def build_regions_data(bracket: dict, teams_stats: dict, seed_lookup: dict) -> dict:
    """
    Walk each region round by round, picking the most likely winner at each
    game slot via predict_matchup. Returns the full REGIONS_DATA structure.
    """
    regions_data = {}

    for region_name, region_teams in bracket.items():
        region = {"R64": [], "R32": [], "S16": [], "E8": [], "champion": ""}

        # R64 — derive directly from bracket input; seed info available
        current_round_teams = [t["team"] for t in region_teams]
        r64_games = []
        for i in range(0, 16, 2):
            team_a_entry = region_teams[i]
            team_b_entry = region_teams[i + 1]
            winner, _ = get_favorite(team_a_entry["team"], team_b_entry["team"], teams_stats)
            upset_flag = is_upset(winner, [team_a_entry["team"], team_b_entry["team"]][
                0 if winner == team_b_entry["team"] else 1], seed_lookup)
            r64_games.append({
                "a":      {"seed": team_a_entry["seed"], "team": team_a_entry["team"]},
                "b":      {"seed": team_b_entry["seed"], "team": team_b_entry["team"]},
                "winner": winner,
                "upset":  upset_flag,
            })
        region["R64"] = r64_games

        # R32, S16, E8 — advance winners from previous round
        prev_winners = [g["winner"] for g in r64_games]
        for round_key in ["R32", "S16", "E8"]:
            games = []
            next_winners = []
            for i in range(0, len(prev_winners), 2):
                team_a = prev_winners[i]
                team_b = prev_winners[i + 1]
                winner, _ = get_favorite(team_a, team_b, teams_stats)
                games.append({
                    "a":      {"team": team_a},
                    "b":      {"team": team_b},
                    "winner": winner,
                    "upset":  is_upset(winner, team_a if winner == team_b else team_b, seed_lookup),
                })
                next_winners.append(winner)
            region[round_key] = games
            prev_winners = next_winners

        region["champion"] = prev_winners[0]
        regions_data[region_name] = region

    return regions_data


def build_f4_and_champ(regions_data: dict, teams_stats: dict) -> tuple:
    """
    Build F4_DATA, CHAMP_DATA, and CHAMPION from regional champions.
    Pairings: East vs West, South vs Midwest.
    """
    east_champ    = regions_data["East"]["champion"]
    west_champ    = regions_data["West"]["champion"]
    south_champ   = regions_data["South"]["champion"]
    midwest_champ = regions_data["Midwest"]["champion"]

    f4_ew_winner, _ = get_favorite(east_champ, west_champ, teams_stats)
    f4_sm_winner, _ = get_favorite(south_champ, midwest_champ, teams_stats)

    f4_data = [
        {"a": {"team": east_champ},  "b": {"team": west_champ},    "winner": f4_ew_winner},
        {"a": {"team": south_champ}, "b": {"team": midwest_champ}, "winner": f4_sm_winner},
    ]

    champion, _ = get_favorite(f4_ew_winner, f4_sm_winner, teams_stats)
    runner_up   = f4_sm_winner if champion == f4_ew_winner else f4_ew_winner

    champ_data = {"a": {"team": f4_ew_winner}, "b": {"team": f4_sm_winner}, "winner": champion}

    return f4_data, champ_data, champion


def build_sim_data(
    sim_results: dict,
    bracket: dict,
    seed_lookup: dict,
    top_n: int = 25,
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
            "R32":      round(probs.get("R64", 0.0), 4),   # reached R32 = won R64
            "S16":      round(probs.get("R32", 0.0), 4),
            "E8":       round(probs.get("S16", 0.0), 4),
            "F4":       round(probs.get("E8",  0.0), 4),
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

    # 3. Build most likely bracket path
    print("\n[3/5] Building most likely bracket path via predict_matchup...")
    regions_data       = build_regions_data(BRACKET_INPUT, teams_stats, seed_lookup)
    f4_data, champ_data, champion = build_f4_and_champ(regions_data, teams_stats)
    print(f"      ✓ Projected champion: {champion}")

    # 4. Run Monte Carlo simulation
    print(f"\n[4/5] Running {N_SIMULATIONS:,} simulations...")
    t0          = time.time()
    sim_results = simulate_tournament(BRACKET_INPUT, teams_stats, n_simulations=N_SIMULATIONS)
    elapsed     = round(time.time() - t0, 1)
    print(f"      ✓ Done in {elapsed}s.")

    sim_meta = {
        "n_trials":    N_SIMULATIONS,
        "runtime_sec": elapsed,
        "model":       "Formula-based log5",
        "season":      "2025–26 NCAA Tournament",
    }

    # 5. Build SIM_DATA and write bracket_data.py
    print("\n[5/5] Writing bracket_data.py...")
    sim_data    = build_sim_data(sim_results, BRACKET_INPUT, seed_lookup, top_n=25)
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

    print("\n─" * 60)
    print("NEXT STEPS:")
    print("  1. python render_bracket.py   → open bracket_resolved_debug.html to verify")
    print("  2. streamlit run app.py        → check the live tab")
    print("─" * 60)