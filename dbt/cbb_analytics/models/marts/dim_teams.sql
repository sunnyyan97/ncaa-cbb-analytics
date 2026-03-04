with kenpom_teams as (
    select distinct
        team_name,
        conference,
        season
    from {{ ref('stg_kenpom__team_ratings') }}
),

torvik_teams as (
    select distinct
        team_name,
        conference,
        season
    from {{ ref('stg_torvik__team_results') }}
),

-- map kenpom names to torvik names
kenpom_normalized as (
    select
        season,
        conference,
        team_name as kenpom_name,
        case team_name
            when 'CSUN'               then 'Cal St. Northridge'
            when 'Kansas City'        then 'UMKC'
            when 'McNeese'            then 'McNeese St.'
            when 'Nicholls'           then 'Nicholls St.'
            when 'SIUE'               then 'SIU Edwardsville'
            when 'Southeast Missouri' then 'Southeast Missouri St.'
            else team_name
        end as canonical_name
    from kenpom_teams
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['k.canonical_name', 'k.season']) }} as team_season_id,
        k.canonical_name    as team_name,
        k.kenpom_name,
        t.team_name         as torvik_name,
        k.conference,
        k.season
    from kenpom_normalized k
    left join torvik_teams t
        on k.canonical_name = t.team_name
        and k.season = t.season
)

select * from joined