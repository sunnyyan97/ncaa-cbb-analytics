with source as (
    select * from {{ source('torvik', 'torvik_player_stats') }}
),

renamed as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['pid', 'season']) }} as player_season_id,

        -- identifiers
        cast(pid as integer)            as player_id,
        player_name,
        team                            as team_name,
        conf                            as conference,
        yr                              as eligibility,
        ht                              as height,
        role                            as position,
        cast(season as integer)         as season,

        -- usage and role
        cast(gp as integer)             as games,
        cast(min_per as float)          as minutes_pct,
        cast(ortg as float)             as off_rating,
        cast(usg as float)              as usage_pct,

        -- shooting efficiency
        cast(efg as float)              as efg_pct,
        cast(ts_per as float)           as ts_pct,
        cast(ft_per as float)           as ft_pct,
        cast(two_p_per as float)        as two_pt_pct,
        cast(tp_per as float)           as three_pt_pct,

        -- shot attempts
        cast(ftm as integer)            as ft_made,
        cast(fta as integer)            as ft_att,
        cast(two_pm as integer)         as two_pt_made,
        cast(two_pa as integer)         as two_pt_att,
        cast(tpm as integer)            as three_pt_made,
        cast(tpa as integer)            as three_pt_att,
        cast(ftr as float)              as ft_rate,

        -- advanced rates
        cast(orb_per as float)          as orb_pct,
        cast(drb_per as float)          as drb_pct,
        cast(ast_per as float)          as ast_pct,
        cast(to_per as float)           as to_pct,
        cast(blk_per as float)          as blk_pct,
        cast(stl_per as float)          as stl_pct,

        -- box plus minus
        cast(bpm as float)              as bpm,
        cast(obpm as float)             as obpm,
        cast(dbpm as float)             as dbpm,

        -- per game stats
        cast(pts as float)              as pts_per_game,
        cast(treb as float)             as reb_per_game,
        cast(ast as float)              as ast_per_game,
        cast(stl as float)              as stl_per_game,
        cast(blk as float)              as blk_per_game,
        cast(oreb as float)             as oreb_per_game,
        cast(dreb as float)             as dreb_per_game

    from source
)

select * from renamed