import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

load_dotenv()

# ─── Page Config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NCAA Hoops Analytics",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─── Styling ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:wght@300;400;500;600&display=swap');

:root {
    --navy:   #0a0e1a;
    --court:  #c8a96e;
    --white:  #f0ede6;
    --dim:    #6b7280;
    --accent: #e8532a;
    --card:   #111827;
    --border: #1f2937;
}

html, body, .stApp {
    background-color: var(--navy) !important;
    color: var(--white) !important;
    font-family: 'DM Sans', sans-serif !important;
}

/* Header */
.hero {
    padding: 2.5rem 0 1.5rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 2rem;
}
.hero h1 {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 4rem;
    letter-spacing: 0.08em;
    color: var(--white);
    line-height: 1;
    margin: 0;
}
.hero h1 span { color: var(--court); }
.hero p {
    color: var(--dim);
    font-size: 0.95rem;
    margin-top: 0.5rem;
    font-weight: 300;
}

/* Section headers */
.section-label {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 1.6rem;
    letter-spacing: 0.1em;
    color: var(--court);
    margin-bottom: 0.25rem;
}
.section-sub {
    color: var(--dim);
    font-size: 0.85rem;
    margin-bottom: 1.25rem;
}

/* Metric cards */
.metric-row { display: flex; gap: 1rem; margin-bottom: 2rem; flex-wrap: wrap; }
.metric-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem 1.5rem;
    flex: 1;
    min-width: 140px;
}
.metric-card .val {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 2.2rem;
    color: var(--court);
    line-height: 1;
}
.metric-card .lbl {
    font-size: 0.75rem;
    color: var(--dim);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 0.2rem;
}

/* Divider */
.divider {
    border: none;
    border-top: 1px solid var(--border);
    margin: 2.5rem 0;
}

/* Tab styling overrides */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    gap: 0.5rem;
    border-bottom: 1px solid var(--border) !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: var(--dim) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.85rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    border: none !important;
    padding: 0.5rem 1rem !important;
}
.stTabs [aria-selected="true"] {
    color: var(--court) !important;
    border-bottom: 2px solid var(--court) !important;
}

/* Dataframe */
.stDataFrame { border: 1px solid var(--border) !important; border-radius: 8px; }

/* Selectbox / slider */
.stSelectbox label, .stSlider label {
    color: var(--dim) !important;
    font-size: 0.8rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.06em !important;
}
</style>
""", unsafe_allow_html=True)

# ─── Snowflake Connection ────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "CBB_ANALYTICS"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "DEV_MARTS"),
    )

@st.cache_data(ttl=3600)
def load_data():
    conn = get_connection()
    query = """
        SELECT
            team_name, conference, record, season,
            consensus_adj_em, kenpom_adj_em, torvik_adj_em,
            kenpom_off_efficiency, kenpom_def_efficiency,
            torvik_off_efficiency, torvik_def_efficiency,
            barthag, barthag_rank, torvik_rank,
            wins_above_bubble, wins_above_bubble_rank,
            sos, non_conf_sos, proj_sos,
            proj_wins, proj_losses,
            qual_barthag, qual_games,
            kenpom_tempo, torvik_tempo,
            top_player_name, top_player_pts, top_player_bpm,
            top_player_position, top_player_eligibility,
            avg_bpm, avg_obpm, avg_dbpm,
            avg_pts, avg_reb, avg_ast,
            avg_ts_pct, avg_efg_pct, avg_three_pt_pct,
            experienced_players
        FROM CBB_ANALYTICS.DEV_MARTS.FCT_TOURNAMENT_PROFILE
        WHERE season = 2026
        ORDER BY consensus_adj_em DESC NULLS LAST
    """
    df = pd.read_sql(query, conn)
    df.columns = [c.lower() for c in df.columns]
    return df

# ─── Load Data ───────────────────────────────────────────────────────────────
try:
    df = load_data()
except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.stop()

# ─── Hero Header ─────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <h1>NCAA HOOPS <span>ANALYTICS</span></h1>
    <p>2025–26 Season · KenPom + BartTorvik · Updated Daily</p>
</div>
""", unsafe_allow_html=True)

# ─── Top Metrics ─────────────────────────────────────────────────────────────
top_team = df.iloc[0]
avg_em = df["consensus_adj_em"].mean()
n_teams = len(df)

st.markdown(f"""
<div class="metric-row">
    <div class="metric-card">
        <div class="val">#{1}</div>
        <div class="lbl">Top Ranked</div>
        <div style="color:#f0ede6;font-size:0.9rem;margin-top:0.3rem">{top_team['team_name']}</div>
    </div>
    <div class="metric-card">
        <div class="val">{top_team['consensus_adj_em']:+.1f}</div>
        <div class="lbl">Best AdjEM</div>
        <div style="color:#6b7280;font-size:0.8rem;margin-top:0.3rem">{top_team['team_name']}</div>
    </div>
    <div class="metric-card">
        <div class="val">{top_team['barthag']:.3f}</div>
        <div class="lbl">Top Barthag</div>
        <div style="color:#6b7280;font-size:0.8rem;margin-top:0.3rem">{top_team['team_name']}</div>
    </div>
    <div class="metric-card">
        <div class="val">{n_teams}</div>
        <div class="lbl">Teams Tracked</div>
        <div style="color:#6b7280;font-size:0.8rem;margin-top:0.3rem">D1 2025–26</div>
    </div>
    <div class="metric-card">
        <div class="val">{avg_em:+.1f}</div>
        <div class="lbl">Avg AdjEM</div>
        <div style="color:#6b7280;font-size:0.8rem;margin-top:0.3rem">All Teams</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ─── Plotly Theme ─────────────────────────────────────────────────────────────
PLOT_LAYOUT = dict(
    paper_bgcolor="#0a0e1a",
    plot_bgcolor="#111827",
    font=dict(family="DM Sans", color="#f0ede6"),
    title_font=dict(family="Bebas Neue", size=20, color="#c8a96e"),
    xaxis=dict(gridcolor="#1f2937", zerolinecolor="#374151"),
    margin=dict(t=50, b=40, l=20, r=20),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#1f2937"),
    hoverlabel=dict(bgcolor="#1f2937", font_color="#f0ede6", bordercolor="#374151"),
)

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📊  Efficiency Rankings",
    "⚡  Offense vs Defense",
    "👥  Starting Five",
    "🎯  WAB vs SOS",
])

# ── Tab 1: Efficiency Bar Chart ───────────────────────────────────────────────
with tab1:
    st.markdown('<div class="section-label">EFFICIENCY RANKINGS</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Consensus Adjusted Efficiency Margin — average of KenPom and BartTorvik</div>', unsafe_allow_html=True)

    col_ctrl1, col_ctrl2 = st.columns([1, 3])
    with col_ctrl1:
        n_teams_show = st.slider("Teams to show", 10, 50, 25, key="bar_n")
        conf_filter = st.selectbox(
            "Filter by conference",
            ["All"] + sorted(df["conference"].dropna().unique().tolist()),
            key="bar_conf"
        )

    filtered = df.copy()
    if conf_filter != "All":
        filtered = filtered[filtered["conference"] == conf_filter]
    filtered = filtered.head(n_teams_show)

    # Color scale by rank
    colors = [f"rgba(200,169,110,{max(0.3, 1 - i/n_teams_show)})" for i in range(len(filtered))]

    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(
        x=filtered["consensus_adj_em"],
        y=filtered["team_name"],
        orientation="h",
        marker_color=colors,
        text=filtered["consensus_adj_em"].apply(lambda x: f"{x:+.1f}"),
        textposition="outside",
        textfont=dict(size=11, color="#f0ede6"),
        customdata=filtered[["record", "conference", "wins_above_bubble", "barthag"]].values,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "AdjEM: %{x:+.2f}<br>"
            "Record: %{customdata[0]}<br>"
            "Conference: %{customdata[1]}<br>"
            "WAB: %{customdata[2]:.1f}<br>"
            "Barthag: %{customdata[3]:.3f}<extra></extra>"
        )
    ))

    fig_bar.update_layout(
        **PLOT_LAYOUT,
        height=max(400, n_teams_show * 28),
        xaxis_title="Consensus Adjusted Efficiency Margin",
        yaxis=dict(autorange="reversed", gridcolor="#1f2937"),
        showlegend=False,
        title="TOP TEAMS BY CONSENSUS ADJUSTER EFFICIENCY MARGIN",
    )

    st.plotly_chart(fig_bar, use_container_width=True)

    # Side by side source comparison
    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">SOURCE COMPARISON</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">KenPom AdjEM vs BartTorvik AdjEM for top 25 — large gaps indicate disagreement between models</div>', unsafe_allow_html=True)

    top25 = df.head(25).copy()
    fig_compare = go.Figure()
    fig_compare.add_trace(go.Scatter(
        x=top25["kenpom_adj_em"],
        y=top25["team_name"],
        mode="markers",
        name="KenPom",
        marker=dict(color="#c8a96e", size=10, symbol="circle"),
        hovertemplate="<b>%{y}</b><br>KenPom AdjEM: %{x:+.2f}<extra></extra>"
    ))
    fig_compare.add_trace(go.Scatter(
        x=top25["torvik_adj_em"],
        y=top25["team_name"],
        mode="markers",
        name="BartTorvik",
        marker=dict(color="#e8532a", size=10, symbol="diamond"),
        hovertemplate="<b>%{y}</b><br>Torvik AdjEM: %{x:+.2f}<extra></extra>"
    ))
    fig_compare.update_layout(
        **PLOT_LAYOUT,
        height=600,
        title="KENPOM VS BARTTORVIK — TOP 25",
        xaxis_title="Adjusted Efficiency Margin",
        yaxis=dict(autorange="reversed", gridcolor="#1f2937"),
    )
    st.plotly_chart(fig_compare, use_container_width=True)

# ── Tab 2: AdjO vs AdjD Scatter ───────────────────────────────────────────────
with tab2:
    st.markdown('<div class="section-label">OFFENSE VS DEFENSE</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Adjusted Offensive Efficiency vs Adjusted Defensive Efficiency — better teams are top-right (high offense, low defense allowed)</div>', unsafe_allow_html=True)

    col_s1, col_s2 = st.columns([1, 3])
    with col_s1:
        source = st.selectbox("Data source", ["KenPom", "BartTorvik"], key="scatter_src")
        highlight_conf = st.selectbox(
            "Highlight conference",
            ["None"] + sorted(df["conference"].dropna().unique().tolist()),
            key="scatter_conf"
        )
        min_wab = st.slider("Min wins above bubble", -20, 10, -5, key="scatter_wab")

    scatter_df = df[df["wins_above_bubble"] >= min_wab].copy()

    if source == "KenPom":
        x_col, y_col = "kenpom_off_efficiency", "kenpom_def_efficiency"
        x_label, y_label = "KenPom Adj. Offensive Efficiency", "KenPom Adj. Defensive Efficiency"
    else:
        x_col, y_col = "torvik_off_efficiency", "torvik_def_efficiency"
        x_label, y_label = "BartTorvik Adj. Offensive Efficiency", "BartTorvik Adj. Defensive Efficiency"

    scatter_df["color"] = scatter_df["conference"].apply(
        lambda c: "#c8a96e" if (highlight_conf != "None" and c == highlight_conf) else "#374151"
    )
    scatter_df["size"] = scatter_df["barthag"].apply(lambda b: max(6, b * 20))
    scatter_df["opacity"] = scatter_df["conference"].apply(
        lambda c: 1.0 if (highlight_conf == "None" or c == highlight_conf) else 0.3
    )

    x_mid = scatter_df[x_col].median()
    y_mid = scatter_df[y_col].median()

    fig_scatter = go.Figure()

    # Quadrant shading
    fig_scatter.add_shape(type="rect",
        x0=x_mid, x1=scatter_df[x_col].max()+2,
        y0=scatter_df[y_col].min()-2, y1=y_mid,
        fillcolor="rgba(200,169,110,0.04)", line_width=0)
    fig_scatter.add_shape(type="line",
        x0=x_mid, x1=x_mid,
        y0=scatter_df[y_col].min()-2, y1=scatter_df[y_col].max()+2,
        line=dict(color="#374151", dash="dot", width=1))
    fig_scatter.add_shape(type="line",
        x0=scatter_df[x_col].min()-2, x1=scatter_df[x_col].max()+2,
        y0=y_mid, y1=y_mid,
        line=dict(color="#374151", dash="dot", width=1))

    fig_scatter.add_trace(go.Scatter(
        x=scatter_df[x_col],
        y=scatter_df[y_col],
        mode="markers+text",
        text=scatter_df["team_name"].apply(lambda t: t if len(t) <= 12 else t.split()[0]),
        textposition="top center",
        textfont=dict(size=8, color="#9ca3af"),
        marker=dict(
            color=scatter_df["color"],
            size=scatter_df["size"],
            opacity=scatter_df["opacity"],
            line=dict(color="#0a0e1a", width=1)
        ),
        customdata=scatter_df[["team_name", "conference", "record", "consensus_adj_em", "wins_above_bubble"]].values,
        hovertemplate=(
            "<b>%{customdata[0]}</b> (%{customdata[1]})<br>"
            "Record: %{customdata[2]}<br>"
            f"{x_label.split('.')[0]}: %{{x:.1f}}<br>"
            f"{y_label.split('.')[0]}: %{{y:.1f}}<br>"
            "Consensus AdjEM: %{customdata[3]:+.2f}<br>"
            "WAB: %{customdata[4]:.1f}<extra></extra>"
        )
    ))

    # Quadrant labels
    for text, x, y in [
        ("ELITE", scatter_df[x_col].max()-1, scatter_df[y_col].min()+0.5),
        ("OFFENSIVE", x_mid+1, scatter_df[y_col].max()-0.5),
        ("DEFENSIVE", scatter_df[x_col].min()+1, scatter_df[y_col].min()+0.5),
        ("REBUILDING", scatter_df[x_col].min()+1, scatter_df[y_col].max()-0.5),
    ]:
        fig_scatter.add_annotation(
            x=x, y=y, text=text,
            showarrow=False,
            font=dict(family="Bebas Neue", size=13, color="#374151"),
        )

    fig_scatter.update_layout(
        **PLOT_LAYOUT,
        height=650,
        title=f"{source.upper()} — OFFENSE VS DEFENSE",
        xaxis_title=x_label,
        yaxis_title=y_label,
        yaxis=dict(autorange="reversed", gridcolor="#1f2937"),
        showlegend=False,
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

# ── Tab 3: Starting Five Table ────────────────────────────────────────────────
with tab3:
    st.markdown('<div class="section-label">STARTING FIVE ANALYSIS</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Average stats across each team\'s top 5 players by minutes — min 10 games played</div>', unsafe_allow_html=True)

    col_t1, col_t2 = st.columns([1, 3])
    with col_t1:
        sort_col = st.selectbox("Sort by", [
            "consensus_adj_em", "avg_bpm", "avg_pts",
            "avg_ts_pct", "avg_efg_pct", "experienced_players"
        ], key="tbl_sort")
        conf_t = st.selectbox(
            "Filter by conference",
            ["All"] + sorted(df["conference"].dropna().unique().tolist()),
            key="tbl_conf"
        )
        top_n = st.slider("Teams to show", 10, len(df), 30, key="tbl_n")

    tbl = df.copy()
    if conf_t != "All":
        tbl = tbl[tbl["conference"] == conf_t]
    tbl = tbl.sort_values(sort_col, ascending=False).head(top_n)

    display_cols = {
        "team_name": "Team",
        "conference": "Conf",
        "record": "Record",
        "consensus_adj_em": "AdjEM",
        "top_player_name": "Top Player",
        "top_player_pts": "Top Pts",
        "avg_bpm": "Avg BPM",
        "avg_obpm": "Avg OBPM",
        "avg_dbpm": "Avg DBPM",
        "avg_pts": "Avg Pts",
        "avg_reb": "Avg Reb",
        "avg_ast": "Avg Ast",
        "avg_ts_pct": "Avg TS%",
        "avg_efg_pct": "Avg eFG%",
        "avg_three_pt_pct": "Avg 3P%",
        "experienced_players": "Veterans",
    }

    tbl_display = tbl[list(display_cols.keys())].rename(columns=display_cols)

    # Format percentages
    for col in ["Avg TS%", "Avg eFG%", "Avg 3P%"]:
        tbl_display[col] = (tbl_display[col] * 100).round(1).astype(str) + "%"

    for col in ["AdjEM", "Avg BPM", "Avg OBPM", "Avg DBPM"]:
        tbl_display[col] = tbl_display[col].apply(lambda x: f"{x:+.2f}" if pd.notna(x) else "—")

    for col in ["Avg Pts", "Avg Reb", "Avg Ast", "Top Pts"]:
        tbl_display[col] = tbl_display[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "—")

    st.dataframe(
        tbl_display,
        use_container_width=True,
        height=600,
        hide_index=True,
    )

    # Experience breakdown
    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">EXPERIENCE INDEX</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Number of veterans (Sr/Gr) in the top 5 by minutes — higher experience correlates with tournament success</div>', unsafe_allow_html=True)

    exp_df = df.nlargest(25, "experienced_players")[["team_name", "experienced_players", "consensus_adj_em", "wins_above_bubble"]].copy()

    fig_exp = go.Figure(go.Bar(
        x=exp_df["experienced_players"],
        y=exp_df["team_name"],
        orientation="h",
        marker_color="#c8a96e",
        marker_opacity=0.85,
        text=exp_df["experienced_players"],
        textposition="outside",
        textfont=dict(color="#f0ede6", size=11),
        customdata=exp_df[["consensus_adj_em", "wins_above_bubble"]].values,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Veterans in Top 5: %{x}<br>"
            "AdjEM: %{customdata[0]:+.2f}<br>"
            "WAB: %{customdata[1]:.1f}<extra></extra>"
        )
    ))
    fig_exp.update_layout(
        **PLOT_LAYOUT,
        height=600,
        title="MOST EXPERIENCED STARTING FIVES",
        xaxis_title="Veterans (Sr/Gr) in Top 5 by Minutes",
        yaxis=dict(autorange="reversed", gridcolor="#1f2937"),
        showlegend=False,
    )
    st.plotly_chart(fig_exp, use_container_width=True)

# ── Tab 4: WAB vs SOS ────────────────────────────────────────────────────────
with tab4:
    st.markdown('<div class="section-label">WINS ABOVE BUBBLE vs STRENGTH OF SCHEDULE</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Teams in the top-right earned their ranking against tough competition. Top-left teams have a strong record against weak schedules.</div>', unsafe_allow_html=True)

    col_w1, col_w2 = st.columns([1, 3])
    with col_w1:
        sos_type = st.selectbox("SOS metric", ["Overall SOS", "Non-Conf SOS", "Projected SOS"], key="wab_sos")
        highlight_conf_w = st.selectbox(
            "Highlight conference",
            ["None"] + sorted(df["conference"].dropna().unique().tolist()),
            key="wab_conf"
        )
        min_games_w = st.slider("Min quality games", 0, 20, 5, key="wab_qual")

    sos_map = {
        "Overall SOS": "sos",
        "Non-Conf SOS": "non_conf_sos",
        "Projected SOS": "proj_sos"
    }
    sos_col = sos_map[sos_type]

    wab_df = df[df["qual_games"] >= min_games_w].copy()
    wab_df = wab_df.dropna(subset=["wins_above_bubble", sos_col])

    wab_df["color"] = wab_df["conference"].apply(
        lambda c: "#c8a96e" if (highlight_conf_w != "None" and c == highlight_conf_w) else "#374151"
    )
    wab_df["opacity"] = wab_df["conference"].apply(
        lambda c: 1.0 if (highlight_conf_w == "None" or c == highlight_conf_w) else 0.25
    )
    wab_df["size"] = wab_df["barthag"].apply(lambda b: max(6, b * 18))

    wab_x_mid = wab_df[sos_col].median()
    wab_y_mid = wab_df["wins_above_bubble"].median()

    fig_wab = go.Figure()

    # Quadrant shading — top right is best
    fig_wab.add_shape(type="rect",
        x0=wab_x_mid, x1=wab_df[sos_col].max()+0.02,
        y0=wab_y_mid, y1=wab_df["wins_above_bubble"].max()+1,
        fillcolor="rgba(200,169,110,0.05)", line_width=0)

    # Median lines
    fig_wab.add_shape(type="line",
        x0=wab_x_mid, x1=wab_x_mid,
        y0=wab_df["wins_above_bubble"].min()-1, y1=wab_df["wins_above_bubble"].max()+1,
        line=dict(color="#374151", dash="dot", width=1))
    fig_wab.add_shape(type="line",
        x0=wab_df[sos_col].min()-0.02, x1=wab_df[sos_col].max()+0.02,
        y0=0, y1=0,
        line=dict(color="#e8532a", dash="solid", width=1, ))
    fig_wab.add_annotation(
        x=wab_df[sos_col].max(), y=0.4,
        text="BUBBLE LINE",
        showarrow=False,
        font=dict(family="Bebas Neue", size=11, color="#e8532a"),
        xanchor="right"
    )

    fig_wab.add_trace(go.Scatter(
        x=wab_df[sos_col],
        y=wab_df["wins_above_bubble"],
        mode="markers+text",
        text=wab_df["team_name"].apply(lambda t: t if len(t) <= 12 else t.split()[0]),
        textposition="top center",
        textfont=dict(size=8, color="#9ca3af"),
        marker=dict(
            color=wab_df["color"],
            size=wab_df["size"],
            opacity=wab_df["opacity"],
            line=dict(color="#0a0e1a", width=1)
        ),
        customdata=wab_df[["team_name", "conference", "record", "consensus_adj_em", "qual_games", "wins_above_bubble_rank"]].values,
        hovertemplate=(
            "<b>%{customdata[0]}</b> (%{customdata[1]})<br>"
            "Record: %{customdata[2]}<br>"
            f"{sos_type}: %{{x:.3f}}<br>"
            "Wins Above Bubble: %{y:+.1f}<br>"
            "WAB Rank: #%{customdata[5]}<br>"
            "Consensus AdjEM: %{customdata[3]:+.2f}<br>"
            "Quality Games: %{customdata[4]}<extra></extra>"
        )
    ))

    # Quadrant labels
    x_min = wab_df[sos_col].min()
    x_max = wab_df[sos_col].max()
    y_min = wab_df["wins_above_bubble"].min()
    y_max = wab_df["wins_above_bubble"].max()

    for text, x, y in [
        ("RÉSUMÉ BUILDERS", x_max - 0.005, y_max - 0.5),
        ("SOFT SCHEDULE", x_min + 0.005, y_max - 0.5),
        ("TOUGH & STRUGGLING", x_max - 0.005, y_min + 0.5),
        ("REBUILDING", x_min + 0.005, y_min + 0.5),
    ]:
        fig_wab.add_annotation(
            x=x, y=y, text=text,
            showarrow=False,
            font=dict(family="Bebas Neue", size=12, color="#374151"),
            xanchor="right" if "RÉSUMÉ" in text or "TOUGH" in text else "left"
        )

    fig_wab.update_layout(
        **PLOT_LAYOUT,
        height=650,
        title=f"WINS ABOVE BUBBLE vs {sos_type.upper()}",
        xaxis_title=sos_type,
        yaxis_title="Wins Above Bubble",
        showlegend=False,
    )
    st.plotly_chart(fig_wab, use_container_width=True)

    # WAB leaderboard below chart
    st.markdown('<hr class="divider">', unsafe_allow_html=True)
    st.markdown('<div class="section-label">WAB LEADERBOARD</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Top 20 teams by wins above bubble</div>', unsafe_allow_html=True)

    wab_board = df.nlargest(20, "wins_above_bubble")[
        ["team_name", "conference", "record", "wins_above_bubble", "wins_above_bubble_rank", "sos", "non_conf_sos", "consensus_adj_em"]
    ].copy().rename(columns={
        "team_name": "Team", "conference": "Conf", "record": "Record",
        "wins_above_bubble": "WAB", "wins_above_bubble_rank": "WAB Rank",
        "sos": "SOS", "non_conf_sos": "NC SOS", "consensus_adj_em": "AdjEM"
    })
    wab_board["WAB"] = wab_board["WAB"].apply(lambda x: f"{x:+.1f}")
    wab_board["AdjEM"] = wab_board["AdjEM"].apply(lambda x: f"{x:+.2f}")
    wab_board["SOS"] = wab_board["SOS"].round(3)
    wab_board["NC SOS"] = wab_board["NC SOS"].round(3)

    st.dataframe(wab_board, use_container_width=True, hide_index=True, height=450)

# ─── Footer ───────────────────────────────────────────────────────────────────
st.markdown("""
<hr class="divider">
<p style="color:#374151;font-size:0.75rem;text-align:center">
    Data: KenPom · BartTorvik &nbsp;·&nbsp; Pipeline: Python → Snowflake → dbt &nbsp;·&nbsp; Updated daily via GitHub Actions
</p>
""", unsafe_allow_html=True)
