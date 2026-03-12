with team_ratings as (
    select * from {{ ref('fct_team_ratings') }}
),

player_stats as (
    select * from {{ ref('fct_player_stats') }}
),

-- rank players by minutes percentage, minimum 10 games played
top_players as (
    select
        team_name,
        season,
        player_name,
        position,
        eligibility,
        bpm,
        obpm,
        dbpm,
        pts_per_game,
        reb_per_game,
        ast_per_game,
        ts_pct,
        usage_pct,
        efg_pct,
        three_pt_pct,
        row_number() over (
            partition by team_name, season
            order by minutes_pct desc
        ) as minutes_rank
    from player_stats
    where games >= 10
),

-- aggregate stats across top 5 players by minutes
starting_five as (
    select
        team_name,
        season,

        -- best player (most minutes)
        max(case when minutes_rank = 1 then player_name end)    as top_player_name,
        max(case when minutes_rank = 1 then position end)        as top_player_position,
        max(case when minutes_rank = 1 then eligibility end)     as top_player_eligibility,
        max(case when minutes_rank = 1 then bpm end)             as top_player_bpm,
        max(case when minutes_rank = 1 then pts_per_game end)    as top_player_pts,

        -- averaged across top 5 by minutes
        round(avg(case when minutes_rank <= 5 then bpm end), 2)           as avg_bpm,
        round(avg(case when minutes_rank <= 5 then obpm end), 2)          as avg_obpm,
        round(avg(case when minutes_rank <= 5 then dbpm end), 2)          as avg_dbpm,
        round(avg(case when minutes_rank <= 5 then pts_per_game end), 2)  as avg_pts,
        round(avg(case when minutes_rank <= 5 then reb_per_game end), 2)  as avg_reb,
        round(avg(case when minutes_rank <= 5 then ast_per_game end), 2)  as avg_ast,
        round(avg(case when minutes_rank <= 5 then ts_pct end), 4)        as avg_ts_pct,
        round(avg(case when minutes_rank <= 5 then efg_pct end), 4)       as avg_efg_pct,
        round(avg(case when minutes_rank <= 5 then three_pt_pct end), 4)  as avg_three_pt_pct,
        round(avg(case when minutes_rank <= 5 then usage_pct end), 2)     as avg_usage_pct,

        -- experience metric — useful tournament predictor
        count(case when minutes_rank <= 5
              and eligibility in ('Jr', 'Sr', 'Gr') then 1 end)        as experienced_players

    from top_players
    where minutes_rank <= 5
    group by team_name, season
),

final as (
    select
        -- identifiers
        t.team_season_id,
        t.team_name,
        t.conference,
        t.season,
        t.record,

        -- tournament placeholders (to be filled after Selection Sunday)
        null::integer        as tournament_seed,
        null::varchar        as tournament_region,
        false                as is_tournament_team,

        -- kenpom metrics
        t.kenpom_adj_em,
        t.kenpom_off_efficiency,
        t.kenpom_off_efficiency_rank,
        t.kenpom_def_efficiency,
        t.kenpom_def_efficiency_rank,
        t.kenpom_tempo,

        -- torvik metrics
        t.torvik_adj_em,
        t.torvik_off_efficiency,
        t.torvik_def_efficiency,
        t.barthag,
        t.barthag_rank,
        t.torvik_rank,
        t.torvik_tempo,

        -- consensus efficiency margin (average of both sources)
        round(
            (t.kenpom_adj_em + t.torvik_adj_em) / 2,
            2
        )                    as consensus_adj_em,

        -- strength of schedule
        t.sos,
        t.non_conf_sos,
        t.proj_sos,

        -- tournament metrics
        t.wins_above_bubble,
        t.wins_above_bubble_rank,
        t.proj_wins,
        t.proj_losses,

        -- quality metrics
        t.qual_barthag,
        t.qual_games,

        -- top player (most minutes)
        p.top_player_name,
        p.top_player_position,
        p.top_player_eligibility,
        p.top_player_bpm,
        p.top_player_pts,

        -- starting five averages
        p.avg_bpm,
        p.avg_obpm,
        p.avg_dbpm,
        p.avg_pts,
        p.avg_reb,
        p.avg_ast,
        p.avg_ts_pct,
        p.avg_efg_pct,
        p.avg_three_pt_pct,
        p.avg_usage_pct,
        p.experienced_players

    from team_ratings t
    left join starting_five p
        on t.team_name = p.team_name
        and t.season = p.season
)

select * from final