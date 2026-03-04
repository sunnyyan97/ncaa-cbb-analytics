with source as (
    select * from {{ source('torvik', 'torvik_player_stats') }}
),

renamed as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['col_32', 'season']) }} as player_season_id,

        -- identifiers and dimensions
        cast(col_32 as integer)         as player_id,
        col_0                           as player_name,
        col_1                           as team_name,
        col_2                           as conference,
        col_25                          as eligibility,
        col_26                          as height,
        col_33                          as hometown,
        col_64                          as position,
        cast(season as integer)         as season,

        -- usage and efficiency
        cast(col_3 as integer)          as games,
        cast(col_4 as float)            as minutes_pct,
        cast(col_5 as float)            as off_rating,
        cast(col_6 as float)            as usage_pct,

        -- shooting
        cast(col_7 as float)            as efg_pct,
        cast(col_8 as float)            as ts_pct,
        cast(col_15 as float)           as ft_pct,
        cast(col_18 as float)           as two_pt_pct,
        cast(col_21 as float)           as three_pt_pct,

        -- shot attempts
        cast(col_13 as integer)         as ft_made,
        cast(col_14 as integer)         as ft_att,
        cast(col_16 as integer)         as two_pt_made,
        cast(col_17 as integer)         as two_pt_att,
        cast(col_19 as integer)         as three_pt_made,
        cast(col_20 as integer)         as three_pt_att,

        -- advanced rates
        cast(col_9 as float)            as orb_pct,
        cast(col_10 as float)           as drb_pct,
        cast(col_11 as float)           as ast_pct,
        cast(col_12 as float)           as to_pct,
        cast(col_22 as float)           as blk_pct,
        cast(col_23 as float)           as stl_pct,
        cast(col_24 as float)           as ft_rate,

        -- box plus minus
        cast(col_50 as float)           as obpm,
        cast(col_52 as float)           as dbpm,
        cast(col_53 as float)           as bpm,

        -- per game stats
        cast(col_57 as float)           as pts_per_game,
        cast(col_58 as float)           as reb_per_game,
        cast(col_60 as float)           as ast_per_game,
        cast(col_61 as float)           as stl_per_game,
        cast(col_62 as float)           as blk_per_game

    from source
)

select * from renamed