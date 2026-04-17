{{
    config(
        materialized='incremental',
        unique_key='game_team_id',
        incremental_strategy='merge'
    )
}}

with games as (
    select * from {{ ref('stg_torvik__game_results') }}

    {% if is_incremental() %}
        where game_date > (select max(game_date) from {{ this }})
    {% endif %}
),

team_ratings as (
    select
        team_name,
        season,
        rank() over (order by (kenpom_adj_em + torvik_adj_em) / 2 desc) as consensus_rank
    from {{ ref('fct_team_ratings') }}
),

joined as (
    select
        -- keys
        g.game_team_id,
        g.game_id,
        g.season,

        -- game dimensions
        g.game_date,
        g.game_location,
        g.season_type,

        -- teams
        g.team_name,
        g.opponent_name,
        opp_r.consensus_rank                        as opponent_rank,

        -- scores
        g.team_score,
        g.opp_score,
        g.score_margin,
        g.team_win,

        -- derived result string for display
        case when g.team_win then 'W' else 'L' end
            || ' ' || g.team_score
            || '-' || g.opp_score                   as result,

        -- metadata
        g.loaded_at

    from games g
    left join team_ratings opp_r
        on opp_r.team_name = g.opponent_name
        and opp_r.season   = g.season
)

select * from joined