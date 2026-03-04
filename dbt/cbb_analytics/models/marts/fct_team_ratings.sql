with dim_teams as (
    select * from {{ ref('dim_teams') }}
),

kenpom as (
    select * from {{ ref('stg_kenpom__team_ratings') }}
),

torvik as (
    select * from {{ ref('stg_torvik__team_results') }}
),

joined as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['d.team_name', 'd.season']) }} as team_season_id,

        -- identifiers
        d.team_name,
        d.conference,
        d.season,
        t.record,

        -- kenpom efficiency
        k.off_efficiency_adj            as kenpom_off_efficiency,
        k.off_efficiency_adj_rank       as kenpom_off_efficiency_rank,
        k.def_efficiency_adj            as kenpom_def_efficiency,
        k.def_efficiency_adj_rank       as kenpom_def_efficiency_rank,
        k.off_efficiency_adj - k.def_efficiency_adj as kenpom_adj_em,

        -- kenpom tempo
        k.tempo_adj                     as kenpom_tempo,
        k.tempo_adj_rank                as kenpom_tempo_rank,

        -- torvik efficiency
        t.adj_off_efficiency            as torvik_off_efficiency,
        t.adj_off_efficiency_rank       as torvik_off_efficiency_rank,
        t.adj_def_efficiency            as torvik_def_efficiency,
        t.adj_def_efficiency_rank       as torvik_def_efficiency_rank,
        t.adj_off_efficiency - t.adj_def_efficiency as torvik_adj_em,
        t.barthag,
        t.barthag_rank,
        t.torvik_rank,

        -- torvik tempo
        t.adj_tempo                     as torvik_tempo,

        -- projected record
        t.proj_wins,
        t.proj_losses,

        -- strength of schedule
        t.sos,
        t.non_conf_sos,
        t.conf_sos,
        t.proj_sos,

        -- tournament metrics
        t.wins_above_bubble,
        t.wins_above_bubble_rank,

        -- quality metrics
        t.qual_off_efficiency,
        t.qual_def_efficiency,
        t.qual_barthag,
        t.qual_games

    from dim_teams d
    left join kenpom k
        on d.kenpom_name = k.team_name
        and d.season = k.season
    left join torvik t
        on d.torvik_name = t.team_name
        and d.season = t.season
)

select * from joined