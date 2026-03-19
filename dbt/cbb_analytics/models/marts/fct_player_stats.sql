with players as (
    select * from {{ ref('stg_torvik__player_stats') }}
),

teams as (
    select * from {{ ref('dim_teams') }}
),

joined as (
    select
        -- surrogate key
        p.player_season_id,

        -- identifiers
        p.player_id,
        p.player_name,
        p.jersey_number,
        p.team_name,
        p.conference,
        p.season,

        -- player info
        p.eligibility,
        p.height,
        p.position,

        -- usage and role
        p.games,
        p.minutes_pct,
        p.usage_pct,

        -- efficiency
        p.off_rating,
        p.efg_pct,
        p.ts_pct,

        -- shooting splits
        p.ft_pct,
        p.two_pt_pct,
        p.three_pt_pct,
        p.ft_made,
        p.ft_att,
        p.two_pt_made,
        p.two_pt_att,
        p.three_pt_made,
        p.three_pt_att,
        p.ft_rate,

        -- advanced rates
        p.orb_pct,
        p.drb_pct,
        p.ast_pct,
        p.to_pct,
        p.blk_pct,
        p.stl_pct,

        -- box plus minus
        p.obpm,
        p.dbpm,
        p.bpm,

        -- per game stats
        p.pts_per_game,
        p.reb_per_game,
        p.ast_per_game,
        p.stl_per_game,
        p.blk_per_game,
        p.oreb_per_game,
        p.dreb_per_game,

        -- team context from dim_teams
        t.torvik_name,
        t.kenpom_name

    from players p
    left join teams t
        on p.team_name = t.team_name
        and p.season = t.season
)

select * from joined