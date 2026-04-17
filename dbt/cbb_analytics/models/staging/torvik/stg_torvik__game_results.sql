with source as (
    select * from {{ source('torvik', 'cbb_game_results') }}
),

renamed as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['game_id', 'team_torvik']) }} as game_team_id,

        -- identifiers
        game_id,
        cast(season as integer)                     as season,

        -- game dimensions
        try_to_date(game_day, 'MM/DD/YY')           as game_date,
        game_location,
        season_type,

        -- teams
        team_torvik                                 as team_name,
        opponent_torvik                             as opponent_name,

        -- scores
        cast(team_score as integer)                 as team_score,
        cast(opp_score as integer)                  as opp_score,
        cast(team_score as integer)
            - cast(opp_score as integer)            as score_margin,
        team_win,

        -- metadata
        loaded_at

    from source
)

select * from renamed