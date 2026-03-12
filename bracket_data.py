# ─── Bracket Data ────────────────────────────────────────────────────────────
# This is the ONLY file that needs to be updated on Selection Sunday.
#
# Workflow:
#   1. Set BRACKET_LOCKED = True once the bracket is finalized
#   2. Populate REGIONS_DATA with the real 68-team bracket matchups
#   3. Update SIM_DATA with Monte Carlo simulation probabilities
#   4. Set F4_DATA, CHAMP_DATA, and CHAMPION from simulation results
#   5. Update SIM_META with the real simulation run stats
#
# Nothing in app.py, bracket_template.html, or render_bracket.py needs to change.
# ─────────────────────────────────────────────────────────────────────────────

# Flip to True once Selection Sunday bracket is loaded and simulation has run
BRACKET_LOCKED = True

# ── Simulation metadata ───────────────────────────────────────────────────────
SIM_META = {
    "n_trials":    10_000,
    "runtime_sec": 2.3,
    "model":       "Formula-based log5",
    "season":      "2025–26 NCAA Tournament",
}

# ── Round-by-round probabilities (one entry per notable team) ─────────────────
# Fields: team, region, seed, R32, S16, E8, F4, Champion
# All probabilities are floats between 0 and 1.
SIM_DATA = [
    {"team": "Michigan",   "region": "West",    "seed": 1, "R32": 0.990, "S16": 0.952, "E8": 0.901, "F4": 0.857, "Champion": 0.354},
    {"team": "Duke",       "region": "East",    "seed": 1, "R32": 0.999, "S16": 0.983, "E8": 0.612, "F4": 0.470, "Champion": 0.205},
    {"team": "Florida",    "region": "South",   "seed": 3, "R32": 0.985, "S16": 0.901, "E8": 0.812, "F4": 0.680, "Champion": 0.179},
    {"team": "Houston",    "region": "Midwest", "seed": 1, "R32": 0.988, "S16": 0.871, "E8": 0.712, "F4": 0.465, "Champion": 0.084},
    {"team": "Arizona",    "region": "East",    "seed": 4, "R32": 0.981, "S16": 0.742, "E8": 0.501, "F4": 0.264, "Champion": 0.077},
    {"team": "Illinois",   "region": "East",    "seed": 6, "R32": 0.921, "S16": 0.601, "E8": 0.381, "F4": 0.218, "Champion": 0.043},
    {"team": "Iowa St.",   "region": "Midwest", "seed": 3, "R32": 0.889, "S16": 0.554, "E8": 0.341, "F4": 0.199, "Champion": 0.018},
    {"team": "Louisville", "region": "South",   "seed": 8, "R32": 0.821, "S16": 0.498, "E8": 0.312, "F4": 0.205, "Champion": 0.014},
    {"team": "Texas Tech", "region": "Midwest", "seed": 4, "R32": 0.812, "S16": 0.421, "E8": 0.221, "F4": 0.102, "Champion": 0.008},
    {"team": "Gonzaga",    "region": "Midwest", "seed": 5, "R32": 0.711, "S16": 0.298, "E8": 0.162, "F4": 0.084, "Champion": 0.004},
    {"team": "Tennessee",  "region": "East",    "seed": 2, "R32": 0.701, "S16": 0.312, "E8": 0.148, "F4": 0.042, "Champion": 0.003},
    {"team": "Kansas",     "region": "West",    "seed": 2, "R32": 0.689, "S16": 0.287, "E8": 0.141, "F4": 0.045, "Champion": 0.002},
    {"team": "Wisconsin",  "region": "East",    "seed": 3, "R32": 0.812, "S16": 0.398, "E8": 0.201, "F4": 0.058, "Champion": 0.002},
    {"team": "Nebraska",   "region": "Midwest", "seed": 6, "R32": 0.621, "S16": 0.241, "E8": 0.118, "F4": 0.065, "Champion": 0.003},
    {"team": "St. John's", "region": "Midwest", "seed": 2, "R32": 0.612, "S16": 0.231, "E8": 0.112, "F4": 0.075, "Champion": 0.002},
]

# ── Per-region bracket (most likely outcome path from simulation) ─────────────
# Each region has rounds: R64, R32, S16, E8, and a champion string.
# Each game: { a: {seed, team}, b: {seed, team}, winner: str, upset: bool }
# R32/S16/E8 slots only need the team name (no seed required).
REGIONS_DATA = {
    "East": {
        "R64": [
            {"a": {"seed": 1,  "team": "Duke"},        "b": {"seed": 16, "team": "Vermont"},      "winner": "Duke",        "upset": False},
            {"a": {"seed": 8,  "team": "Miss. St."},   "b": {"seed": 9,  "team": "Boise St."},    "winner": "Boise St.",   "upset": True},
            {"a": {"seed": 5,  "team": "Oregon"},      "b": {"seed": 12, "team": "Liberty"},      "winner": "Oregon",      "upset": False},
            {"a": {"seed": 4,  "team": "Arizona"},     "b": {"seed": 13, "team": "High Point"},   "winner": "Arizona",     "upset": False},
            {"a": {"seed": 6,  "team": "Illinois"},    "b": {"seed": 11, "team": "VCU"},          "winner": "Illinois",    "upset": False},
            {"a": {"seed": 3,  "team": "Wisconsin"},   "b": {"seed": 14, "team": "Morehead St."}, "winner": "Wisconsin",   "upset": False},
            {"a": {"seed": 7,  "team": "Marquette"},   "b": {"seed": 10, "team": "New Mexico"},   "winner": "Marquette",   "upset": False},
            {"a": {"seed": 2,  "team": "Tennessee"},   "b": {"seed": 15, "team": "Wofford"},      "winner": "Tennessee",   "upset": False},
        ],
        "R32": [
            {"a": {"team": "Duke"},      "b": {"team": "Boise St."}, "winner": "Duke",      "upset": False},
            {"a": {"team": "Oregon"},    "b": {"team": "Arizona"},   "winner": "Arizona",   "upset": False},
            {"a": {"team": "Illinois"},  "b": {"team": "Wisconsin"}, "winner": "Wisconsin", "upset": True},
            {"a": {"team": "Marquette"}, "b": {"team": "Tennessee"}, "winner": "Tennessee", "upset": False},
        ],
        "S16": [
            {"a": {"team": "Duke"},      "b": {"team": "Arizona"},   "winner": "Duke",      "upset": False},
            {"a": {"team": "Wisconsin"}, "b": {"team": "Tennessee"}, "winner": "Tennessee", "upset": False},
        ],
        "E8": [
            {"a": {"team": "Duke"}, "b": {"team": "Tennessee"}, "winner": "Duke", "upset": False},
        ],
        "champion": "Duke",
    },
    "West": {
        "R64": [
            {"a": {"seed": 1,  "team": "Michigan"},    "b": {"seed": 16, "team": "Sac. Heart"},   "winner": "Michigan",    "upset": False},
            {"a": {"seed": 8,  "team": "Dayton"},      "b": {"seed": 9,  "team": "Creighton"},    "winner": "Creighton",   "upset": True},
            {"a": {"seed": 5,  "team": "Clemson"},     "b": {"seed": 12, "team": "UC San Diego"}, "winner": "Clemson",     "upset": False},
            {"a": {"seed": 4,  "team": "Texas A&M"},   "b": {"seed": 13, "team": "Troy"},         "winner": "Texas A&M",   "upset": False},
            {"a": {"seed": 6,  "team": "BYU"},         "b": {"seed": 11, "team": "Drake"},        "winner": "BYU",         "upset": False},
            {"a": {"seed": 3,  "team": "Kentucky"},    "b": {"seed": 14, "team": "Lipscomb"},     "winner": "Kentucky",    "upset": False},
            {"a": {"seed": 7,  "team": "St. John's"},  "b": {"seed": 10, "team": "Vanderbilt"},   "winner": "St. John's",  "upset": False},
            {"a": {"seed": 2,  "team": "Kansas"},      "b": {"seed": 15, "team": "Winthrop"},     "winner": "Kansas",      "upset": False},
        ],
        "R32": [
            {"a": {"team": "Michigan"},   "b": {"team": "Creighton"}, "winner": "Michigan",  "upset": False},
            {"a": {"team": "Clemson"},    "b": {"team": "Texas A&M"}, "winner": "Texas A&M", "upset": False},
            {"a": {"team": "BYU"},        "b": {"team": "Kentucky"},  "winner": "Kentucky",  "upset": False},
            {"a": {"team": "St. John's"}, "b": {"team": "Kansas"},    "winner": "Kansas",    "upset": False},
        ],
        "S16": [
            {"a": {"team": "Michigan"}, "b": {"team": "Texas A&M"}, "winner": "Michigan", "upset": False},
            {"a": {"team": "Kentucky"}, "b": {"team": "Kansas"},    "winner": "Kansas",   "upset": False},
        ],
        "E8": [
            {"a": {"team": "Michigan"}, "b": {"team": "Kansas"}, "winner": "Michigan", "upset": False},
        ],
        "champion": "Michigan",
    },
    "South": {
        "R64": [
            {"a": {"seed": 1,  "team": "Auburn"},     "b": {"seed": 16, "team": "Ala. St."},   "winner": "Auburn",     "upset": False},
            {"a": {"seed": 8,  "team": "Louisville"}, "b": {"seed": 9,  "team": "Colo. St."},  "winner": "Louisville", "upset": False},
            {"a": {"seed": 5,  "team": "Memphis"},    "b": {"seed": 12, "team": "UNLV"},        "winner": "Memphis",    "upset": False},
            {"a": {"seed": 4,  "team": "Maryland"},   "b": {"seed": 13, "team": "Bryant"},      "winner": "Maryland",   "upset": False},
            {"a": {"seed": 6,  "team": "Missouri"},   "b": {"seed": 11, "team": "Pittsburgh"},  "winner": "Missouri",   "upset": False},
            {"a": {"seed": 3,  "team": "Florida"},    "b": {"seed": 14, "team": "Rider"},       "winner": "Florida",    "upset": False},
            {"a": {"seed": 7,  "team": "UCLA"},       "b": {"seed": 10, "team": "Utah St."},    "winner": "Utah St.",   "upset": True},
            {"a": {"seed": 2,  "team": "Tennessee"},  "b": {"seed": 15, "team": "NJIT"},        "winner": "Tennessee",  "upset": False},
        ],
        "R32": [
            {"a": {"team": "Auburn"},   "b": {"team": "Louisville"}, "winner": "Auburn",    "upset": False},
            {"a": {"team": "Memphis"},  "b": {"team": "Maryland"},   "winner": "Maryland",  "upset": False},
            {"a": {"team": "Missouri"}, "b": {"team": "Florida"},    "winner": "Florida",   "upset": False},
            {"a": {"team": "Utah St."}, "b": {"team": "Tennessee"},  "winner": "Tennessee", "upset": False},
        ],
        "S16": [
            {"a": {"team": "Auburn"},  "b": {"team": "Maryland"},  "winner": "Auburn",  "upset": False},
            {"a": {"team": "Florida"}, "b": {"team": "Tennessee"}, "winner": "Florida", "upset": False},
        ],
        "E8": [
            {"a": {"team": "Auburn"}, "b": {"team": "Florida"}, "winner": "Florida", "upset": False},
        ],
        "champion": "Florida",
    },
    "Midwest": {
        "R64": [
            {"a": {"seed": 1,  "team": "Houston"},     "b": {"seed": 16, "team": "SIUE"},           "winner": "Houston",    "upset": False},
            {"a": {"seed": 8,  "team": "Mississippi"}, "b": {"seed": 9,  "team": "Georgia"},        "winner": "Georgia",    "upset": True},
            {"a": {"seed": 5,  "team": "Gonzaga"},     "b": {"seed": 12, "team": "Louisiana"},      "winner": "Gonzaga",    "upset": False},
            {"a": {"seed": 4,  "team": "Texas Tech"},  "b": {"seed": 13, "team": "Colgate"},        "winner": "Texas Tech", "upset": False},
            {"a": {"seed": 6,  "team": "Nebraska"},    "b": {"seed": 11, "team": "San Diego St."},  "winner": "Nebraska",   "upset": False},
            {"a": {"seed": 3,  "team": "Iowa St."},    "b": {"seed": 14, "team": "N. Iowa"},        "winner": "Iowa St.",   "upset": False},
            {"a": {"seed": 7,  "team": "Cincinnati"},  "b": {"seed": 10, "team": "Xavier"},         "winner": "Xavier",     "upset": True},
            {"a": {"seed": 2,  "team": "St. John's"},  "b": {"seed": 15, "team": "B-Cookman"},      "winner": "St. John's", "upset": False},
        ],
        "R32": [
            {"a": {"team": "Houston"},  "b": {"team": "Georgia"},    "winner": "Houston",    "upset": False},
            {"a": {"team": "Gonzaga"},  "b": {"team": "Texas Tech"}, "winner": "Texas Tech", "upset": False},
            {"a": {"team": "Nebraska"}, "b": {"team": "Iowa St."},   "winner": "Iowa St.",   "upset": False},
            {"a": {"team": "Xavier"},   "b": {"team": "St. John's"}, "winner": "St. John's", "upset": False},
        ],
        "S16": [
            {"a": {"team": "Houston"},  "b": {"team": "Texas Tech"}, "winner": "Houston",  "upset": False},
            {"a": {"team": "Iowa St."}, "b": {"team": "St. John's"}, "winner": "Iowa St.", "upset": False},
        ],
        "E8": [
            {"a": {"team": "Houston"}, "b": {"team": "Iowa St."}, "winner": "Houston", "upset": False},
        ],
        "champion": "Houston",
    },
}

# ── Final Four & Championship ─────────────────────────────────────────────────
# F4_DATA[0] = South vs West  (left F4 game  — feeds Championship slot A)
# F4_DATA[1] = East vs Midwest (right F4 game — feeds Championship slot B)
F4_DATA = [
    {"a": {"team": "Florida"}, "b": {"team": "Michigan"}, "winner": "Michigan"},  # South vs West
    {"a": {"team": "Duke"},    "b": {"team": "Houston"},  "winner": "Duke"},       # East vs Midwest
]

CHAMP_DATA = {"a": {"team": "Michigan"}, "b": {"team": "Duke"}, "winner": "Michigan"}

CHAMPION = "Michigan"