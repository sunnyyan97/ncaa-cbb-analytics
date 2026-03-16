import json
import os
from pathlib import Path

import bracket_data as bd


# Resolve template path relative to this file so imports work regardless of
# where the Streamlit process is launched from.
_TEMPLATE_PATH = Path(__file__).parent / "bracket_template.html"


def render_bracket_html() -> str:
    """
    Read bracket_template.html and inject all data from bracket_data.py.
    Returns a fully resolved HTML string ready for st.components.v1.html().
    """
    template = _TEMPLATE_PATH.read_text(encoding="utf-8")

    # ── Data injections ───────────────────────────────────────────────────────
    # json.dumps handles correct JS literals for all types:
    #   - Python dicts/lists  → JS objects/arrays
    #   - Python str          → quoted JS string  e.g. "Michigan"
    #   - Python bool         → lowercase true/false (JS-compatible)
    #   - Python int/float    → JS numbers
    replacements = {
        "__SIM_DATA__":     json.dumps(bd.SIM_DATA),
        "__REGIONS_DATA__": json.dumps(bd.REGIONS_DATA),
        "__F4_DATA__":      json.dumps(bd.F4_DATA),
        "__CHAMP_DATA__":   json.dumps(bd.CHAMP_DATA),
        "__CHAMPION__":     json.dumps(bd.CHAMPION),       # renders as "Michigan"
        "__SIM_META__":     json.dumps(bd.SIM_META),
        # Plain-text substitutions used in the model note footer
        "__N_TRIALS__":     str(bd.SIM_META["n_trials"]),
        "__MODEL__":        bd.SIM_META["model"],
    }

    for token, value in replacements.items():
        template = template.replace(token, value)

    return template


def write_resolved_html(output_path: str | Path) -> None:
    """
    Optional helper: write the fully resolved HTML to a file.
    Useful for local debugging — open the output in a browser to verify
    the bracket renders correctly before wiring it into Streamlit.

    Usage:
        python render_bracket.py
    """
    html = render_bracket_html()
    Path(output_path).write_text(html, encoding="utf-8")
    print(f"Resolved HTML written to: {output_path}")


if __name__ == "__main__":
    # Running this file directly writes a debug-ready HTML file you can
    # open in a browser to visually verify the bracket before Sunday.
    out = Path(__file__).parent / "bracket_resolved_debug.html"
    write_resolved_html(out)