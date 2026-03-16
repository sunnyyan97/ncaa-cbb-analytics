import random
from collections import defaultdict
from modeling.predict import predict_matchup


# Fixed first-round seed matchup pairs — same every year regardless of teams
# Order matters: winners of slots 0/1 play each other, 2/3 play each other, etc.
FIRST_ROUND_SEED_ORDER = [1, 16, 8, 9, 5, 12, 4, 13, 6, 11, 3, 14, 7, 10, 2, 15]

# Round names in order — index corresponds to which round a team was eliminated
ROUND_NAMES = ["R64", "R32", "S16", "E8", "F4", "Championship"]

# Standard NCAA bracket region pairings for the Final Four
# South champion plays West champion, East champion plays Midwest champion.
# Indices reference REGION_ORDER below: East=0, West=1, South=2, Midwest=3
FINAL_FOUR_PAIRINGS = [(2, 1), (0, 3)]  # (South vs West), (East vs Midwest)
REGION_ORDER = ["East", "West", "South", "Midwest"]


def get_team_safe(teams_stats: dict, team_name: str) -> dict:
    """
    Return team stats if available, otherwise return a D1-average fallback.
    Prevents simulation crashes for mid-major auto-bid teams that may not
    be present in fct_tournament_profile.
    """
    if team_name in teams_stats:
        return teams_stats[team_name]

    print(f"  WARNING: '{team_name}' not found in team stats — using D1 average fallback")
    return {
        "team_name": team_name,
        "consensus_adj_em": 0.0,
        "kenpom_adj_em": 0.0,
        "torvik_adj_em": 0.0,
        "kenpom_off_efficiency": 100.0,
        "kenpom_def_efficiency": 100.0,
        "torvik_off_efficiency": 100.0,
        "torvik_def_efficiency": 100.0,
        "barthag": 0.5,
        "kenpom_tempo": 68.0,
        "torvik_tempo": 68.0,
        "sos": 0.0,
        "wins_above_bubble": 0.0,
        "avg_bpm": 0.0,
        "avg_obpm": 0.0,
        "avg_dbpm": 0.0,
        "avg_pts": 0.0,
        "avg_ts_pct": 0.0,
        "avg_efg_pct": 0.0,
        "experienced_players": 0,
        "top_player_name": "N/A",
        "top_player_bpm": 0.0,
    }


def play_game(team_a_name: str, team_b_name: str, teams_stats: dict) -> tuple:
    """
    Simulate a single game between two teams.
    All tournament games are treated as neutral site.

    Returns (winner_name, loser_name).
    """
    team_a = get_team_safe(teams_stats, team_a_name)
    team_b = get_team_safe(teams_stats, team_b_name)

    result = predict_matchup(team_a, team_b, location="neutral")

    if random.random() < result["team_a_win_prob"]:
        return team_a_name, team_b_name
    else:
        return team_b_name, team_a_name


def simulate_once(
    bracket: dict,
    teams_stats: dict,
    known_results: dict | None = None,
    f4_results: list | None = None,
    champ_result: str = "",
) -> dict:
    """
    Simulate one full 68-team tournament from first round through championship.

    Args:
        bracket:       dict with keys "East", "West", "South", "Midwest".
                       Each value is a list of 16 dicts: {"seed": int, "team": str}.
                       Teams must be ordered to match FIRST_ROUND_SEED_ORDER.
        teams_stats:   dict of team stats keyed by team name (from Snowflake).
        known_results: optional dict {region → {round_name → [winner, ...]}}
                       When a round has known results, those are used verbatim
                       instead of calling play_game(). Partial lists are supported —
                       only filled slots use the known result.
        f4_results:    optional list of 2 actual F4 winners in FINAL_FOUR_PAIRINGS
                       order: [South/West winner, East/Midwest winner].
        champ_result:  optional actual championship winner.

    Returns:
        dict mapping each team name to the furthest round they reached,
        e.g. {"Duke": "Champion", "Tennessee": "F4", "Vermont": "R64", ...}
    """
    known_results = known_results or {}
    results = {}
    regional_champions = []

    # ── Regional rounds (R64 through E8) ──────────────────────────────────
    for region_name in REGION_ORDER:
        region_teams = bracket[region_name]
        region_known = known_results.get(region_name, {})

        # Teams ordered so that index 0/1 play, 2/3 play, 4/5 play, etc.
        current_round = [t["team"] for t in region_teams]
        round_idx = 0  # 0=R64, 1=R32, 2=S16, 3=E8

        while len(current_round) > 1:
            round_name = ROUND_NAMES[round_idx]
            round_known = region_known.get(round_name, [])
            next_round = []
            for slot, i in enumerate(range(0, len(current_round), 2)):
                if slot < len(round_known):
                    # Use known result for this slot
                    winner = round_known[slot]
                    loser = (current_round[i]
                             if winner == current_round[i + 1]
                             else current_round[i + 1])
                else:
                    winner, loser = play_game(
                        current_round[i], current_round[i + 1], teams_stats
                    )
                results[loser] = round_name
                next_round.append(winner)

            current_round = next_round
            round_idx += 1

        # current_round now has exactly one team — the regional champion
        regional_champions.append(current_round[0])

    # ── Final Four ────────────────────────────────────────────────────────
    semifinal_winners = []
    for game_idx, (i, j) in enumerate(FINAL_FOUR_PAIRINGS):
        if f4_results and game_idx < len(f4_results):
            winner = f4_results[game_idx]
            loser = (regional_champions[i]
                     if winner == regional_champions[j]
                     else regional_champions[j])
        else:
            winner, loser = play_game(
                regional_champions[i], regional_champions[j], teams_stats
            )
        results[loser] = "F4"
        semifinal_winners.append(winner)

    # ── Championship ──────────────────────────────────────────────────────
    if champ_result:
        champion  = champ_result
        runner_up = (semifinal_winners[0]
                     if champion == semifinal_winners[1]
                     else semifinal_winners[1])
    else:
        champion, runner_up = play_game(
            semifinal_winners[0], semifinal_winners[1], teams_stats
        )
    results[runner_up] = "Championship"
    results[champion]  = "Champion"

    return results


def simulate_tournament(
    bracket: dict,
    teams_stats: dict,
    n_simulations: int = 10000,
    known_results: dict | None = None,
    f4_results: list | None = None,
    champ_result: str = "",
) -> dict:
    """
    Run the full tournament simulation N times and return round-by-round
    probabilities for every team.

    Args:
        bracket:       Same format as simulate_once().
        teams_stats:   Dict of team stats keyed by team name.
        n_simulations: Number of Monte Carlo trials. Default 10,000 gives
                       ~0.5% standard error on a 50% probability — stable
                       enough for display without being slow.
        known_results: optional dict {region → {round_name → [winner, ...]}}
                       Passed through to simulate_once() — completed rounds use
                       actual results instead of random simulation.
        f4_results:    optional list of 2 actual Final Four winners.
        champ_result:  optional actual championship winner.

    Returns:
        dict mapping team name -> {round_name -> probability (0.0–1.0)}

        Example:
        {
            "Duke": {
                "R64": 0.99, "R32": 0.87, "S16": 0.71,
                "E8": 0.54, "F4": 0.37, "Championship": 0.21, "Champion": 0.14
            },
            ...
        }
    """
    # Count how many times each team reached each round across all simulations
    reach_counts = defaultdict(lambda: defaultdict(int))

    for _ in range(n_simulations):
        single_run = simulate_once(bracket, teams_stats,
                                   known_results=known_results,
                                   f4_results=f4_results,
                                   champ_result=champ_result)

        for team, furthest_round in single_run.items():
            is_champion = furthest_round == "Champion"

            # Determine how deep this team went
            if is_champion:
                reached_index = len(ROUND_NAMES) - 1 # past the last elimination round
            else:
                reached_index = ROUND_NAMES.index(furthest_round)

            # Credit the team for every round they reached, not just the final one.
            # A team eliminated in the S16 still "reached" R64 and R32.
            for round_name in ROUND_NAMES[:reached_index]:
                reach_counts[team][round_name] += 1

            # Credit the elimination round itself (or Champion)
            if is_champion:
                reach_counts[team]["Championship"] += 1
                reach_counts[team]["Champion"] += 1
            else:
                reach_counts[team][furthest_round] += 1

    # Convert raw counts to probabilities
    probabilities = {}
    for team, round_counts in reach_counts.items():
        probabilities[team] = {
            round_name: round(count / n_simulations, 4)
            for round_name, count in round_counts.items()
        }

    return probabilities


def get_championship_probabilities(simulation_results: dict) -> list:
    """
    Extract and sort teams by championship probability.
    Convenience function for dashboard display.

    Returns list of (team_name, probability) tuples, sorted descending.
    """
    champ_probs = [
        (team, probs.get("Champion", 0.0))
        for team, probs in simulation_results.items()
    ]
    return sorted(champ_probs, key=lambda x: x[1], reverse=True)


def get_final_four_probabilities(simulation_results: dict) -> list:
    """
    Extract and sort teams by Final Four probability.
    Convenience function for dashboard display.

    Returns list of (team_name, probability) tuples, sorted descending.
    """
    f4_probs = [
        (team, probs.get("F4", 0.0))
        for team, probs in simulation_results.items()
    ]
    return sorted(f4_probs, key=lambda x: x[1], reverse=True)


# ── Test block ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from modeling.predict import get_all_team_stats

    print("Loading team stats from Snowflake...")
    df = get_all_team_stats()
    teams = df.set_index("team_name").to_dict("index")
    for name, stats in teams.items():
        stats["team_name"] = name
    print(f"Loaded {len(teams)} teams.\n")

    # Stub bracket for testing before the real bracket drops March 15.
    # Teams are ordered to match FIRST_ROUND_SEED_ORDER: 1,16,8,9,5,12,4,13,6,11,3,14,7,10,2,15
    # Slot 0/1 play each other (1v16), 2/3 play (8v9), 4/5 play (5v12), etc.
    TEST_BRACKET = {
        "East": [
            {"seed": 1,  "team": "Duke"},
            {"seed": 16, "team": "Vermont"},
            {"seed": 8,  "team": "Mississippi St."},
            {"seed": 9,  "team": "Boise St."},
            {"seed": 5,  "team": "Oregon"},
            {"seed": 12, "team": "Liberty"},
            {"seed": 4,  "team": "Arizona"},
            {"seed": 13, "team": "High Point"},
            {"seed": 6,  "team": "Illinois"},
            {"seed": 11, "team": "VCU"},
            {"seed": 3,  "team": "Wisconsin"},
            {"seed": 14, "team": "Morehead St."},
            {"seed": 7,  "team": "Marquette"},
            {"seed": 10, "team": "New Mexico"},
            {"seed": 2,  "team": "Tennessee"},
            {"seed": 15, "team": "Wofford"},
        ],
        "West": [
            {"seed": 1,  "team": "Michigan"},
            {"seed": 16, "team": "Sacred Heart"},
            {"seed": 8,  "team": "Dayton"},
            {"seed": 9,  "team": "Creighton"},
            {"seed": 5,  "team": "Clemson"},
            {"seed": 12, "team": "UC San Diego"},
            {"seed": 4,  "team": "Texas A&M"},
            {"seed": 13, "team": "Troy"},
            {"seed": 6,  "team": "BYU"},
            {"seed": 11, "team": "Drake"},
            {"seed": 3,  "team": "Kentucky"},
            {"seed": 14, "team": "Lipscomb"},
            {"seed": 7,  "team": "St. John's"},
            {"seed": 10, "team": "Vanderbilt"},
            {"seed": 2,  "team": "Kansas"},
            {"seed": 15, "team": "Winthrop"},
        ],
        "South": [
            {"seed": 1,  "team": "Auburn"},
            {"seed": 16, "team": "Alabama St."},
            {"seed": 8,  "team": "Louisville"},
            {"seed": 9,  "team": "Colorado St."},
            {"seed": 5,  "team": "Memphis"},
            {"seed": 12, "team": "UNLV"},
            {"seed": 4,  "team": "Maryland"},
            {"seed": 13, "team": "Bryant"},
            {"seed": 6,  "team": "Missouri"},
            {"seed": 11, "team": "Pittsburgh"},
            {"seed": 3,  "team": "Wisconsin"},
            {"seed": 14, "team": "Rider"},
            {"seed": 7,  "team": "UCLA"},
            {"seed": 10, "team": "Utah St."},
            {"seed": 2,  "team": "Florida"},
            {"seed": 15, "team": "NJIT"},
        ],
        "Midwest": [
            {"seed": 1,  "team": "Houston"},
            {"seed": 16, "team": "SIU Edwardsville"},
            {"seed": 8,  "team": "Mississippi"},
            {"seed": 9,  "team": "Georgia"},
            {"seed": 5,  "team": "Gonzaga"},
            {"seed": 12, "team": "Louisiana"},
            {"seed": 4,  "team": "Texas Tech"},
            {"seed": 13, "team": "Colgate"},
            {"seed": 6,  "team": "Nebraska"},
            {"seed": 11, "team": "San Diego St."},
            {"seed": 3,  "team": "Iowa St."},
            {"seed": 14, "team": "Northern Iowa"},
            {"seed": 7,  "team": "Cincinnati"},
            {"seed": 10, "team": "Xavier"},
            {"seed": 2,  "team": "St. John's"},
            {"seed": 15, "team": "Bethune Cookman"},
        ],
    }

    print("Running 10,000 simulations...")
    results = simulate_tournament(TEST_BRACKET, teams, n_simulations=10000)

    print("\n── Championship Probabilities (Top 15) ──")
    for team, prob in get_championship_probabilities(results)[:15]:
        bar = "█" * int(prob * 200)
        print(f"  {team:<25} {prob*100:5.1f}%  {bar}")

    print("\n── Final Four Probabilities (Top 16) ──")
    for team, prob in get_final_four_probabilities(results)[:16]:
        bar = "█" * int(prob * 100)
        print(f"  {team:<25} {prob*100:5.1f}%  {bar}")

    print("\n── Full results for Duke ──")
    if "Duke" in results:
        for round_name, prob in results["Duke"].items():
            print(f"  {round_name:<15} {prob*100:.1f}%")