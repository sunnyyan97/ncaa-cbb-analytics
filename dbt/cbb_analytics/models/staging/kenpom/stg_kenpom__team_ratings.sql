with source as (
    select * from {{ source('kenpom', 'kenpom_team_ratings') }}
),

renamed as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['team', 'season']) }} as team_season_id,

        -- identifiers and dimensions
        team                                as team_name,
        conference                          as conference,
        cast(season as integer)             as season,

        -- tempo
        cast(tempo_adj as float)            as tempo_adj,
        cast(tempo_adj_rank as integer)     as tempo_adj_rank,
        cast(tempo_raw as float)            as tempo_raw,
        cast(tempo_raw_rank as integer)     as tempo_raw_rank,

        -- possession length
        cast(avg__poss_length_offense as float)         as avg_poss_length_offense,
        cast(avg__poss_length_offense_rank as integer)  as avg_poss_length_offense_rank,
        cast(avg__poss_length_defense as float)         as avg_poss_length_defense,
        cast(avg__poss_length_defense_rank as integer)  as avg_poss_length_defense_rank,

        -- offensive efficiency
        cast(off__efficiency_adj as float)          as off_efficiency_adj,
        cast(off__efficiency_adj_rank as integer)   as off_efficiency_adj_rank,
        cast(off__efficiency_raw as float)          as off_efficiency_raw,
        cast(off__efficiency_raw_rank as integer)   as off_efficiency_raw_rank,

        -- defensive efficiency
        cast(def__efficiency_adj as float)          as def_efficiency_adj,
        cast(def__efficiency_adj_rank as integer)   as def_efficiency_adj_rank,
        cast(def__efficiency_raw as float)          as def_efficiency_raw,
        cast(def__efficiency_raw_rank as integer)   as def_efficiency_raw_rank

    from source
)

select * from renamed