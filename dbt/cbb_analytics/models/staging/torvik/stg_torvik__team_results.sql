with source as (
    select * from {{ source('torvik', 'torvik_team_results') }}
),

renamed as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['col_1', 'season']) }} as team_season_id,

        -- identifiers and dimensions
        col_1                           as team_name,
        col_2                           as conference,
        col_3                           as record,
        cast(season as integer)         as season,

        -- core efficiency metrics
        cast(col_0 as integer)          as torvik_rank,
        cast(col_4 as float)            as adj_off_efficiency,
        cast(col_5 as integer)          as adj_off_efficiency_rank,
        cast(col_6 as float)            as adj_def_efficiency,
        cast(col_7 as integer)          as adj_def_efficiency_rank,
        cast(col_8 as float)            as barthag,
        cast(col_9 as integer)          as barthag_rank,
        cast(col_44 as float)           as adj_tempo,

        -- projected record
        cast(col_10 as float)           as proj_wins,
        cast(col_11 as float)           as proj_losses,

        -- strength of schedule
        cast(col_15 as float)           as sos,
        cast(col_16 as float)           as non_conf_sos,
        cast(col_17 as float)           as conf_sos,
        cast(col_18 as float)           as proj_sos,

        -- wins above bubble
        cast(col_41 as float)           as wins_above_bubble,
        cast(col_42 as integer)         as wins_above_bubble_rank,

        -- quality metrics
        cast(col_29 as float)           as qual_off_efficiency,
        cast(col_30 as float)           as qual_def_efficiency,
        cast(col_31 as float)           as qual_barthag,
        cast(col_32 as integer)         as qual_games

    from source

    -- filter out the header row that got loaded as data
    where col_1 != 'team'
)

select * from renamed