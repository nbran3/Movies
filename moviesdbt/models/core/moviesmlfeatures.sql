{{
    config(
        materialized='table',
    )
}}


with moviefull as (
    select *
    from {{ ref('moviefull') }}
),
is_star as (
    select *
    from {{ ref('is_star') }}
),

director_info as (
    select
        mc.movie_id,
        max(td.is_top_director) as is_top_director,
        max(td.num_movies)      as director_movie_count
    from {{ ref('movie_cast') }} mc
    join {{ ref('top_director') }} td on mc.person_id = td.person_id
    where mc.person_role = 'director'
    group by mc.movie_id
),

movie_performance as (
    select movie_id, is_flop, budget_adj, is_recession
    from {{ ref('movie_performance') }}
),

season_release as (
    select movie_id,
        case
            when extract(month from release_date) in (11, 12) then '1'
            when extract(month from release_date) in (3, 4, 5)  then '2'
            when extract(month from release_date) in (6, 7, 8)  then '3'
            else '4'
        end as release_season
    from moviefull
),

studio_metrics as (
    select
        mf.movie_id,
        avg(sf.studio_flop_rate)      as studio_flop_rate,
        max(sf.movie_count)           as studio_movie_count,
        max(sf.avg_movies_per_year)   as studio_avg_movies_per_year
    from moviefull mf
    cross join unnest(split(mf.production_companies, ',')) as studio
    join {{ ref('studio_fact') }} sf
        on trim(studio) = sf.studio
        and length(trim(studio)) < 100
        and trim(studio) != ''
    group by mf.movie_id
),

genre_encoding as (
    select movie_id,
        case when contains_substr(genres, 'Action') then 1 else 0 end as is_action,
        case when contains_substr(genres, 'Adventure') then 1 else 0 end as is_adventure,
        case when contains_substr(genres, 'Animation') then 1 else 0 end as is_animation,
        case when contains_substr(genres, 'Comedy') then 1 else 0 end as is_comedy,
        case when contains_substr(genres, 'Crime') then 1 else 0 end as is_crime,
        case when contains_substr(genres, 'Documentary') then 1 else 0 end as is_documentary,
        case when contains_substr(genres, 'Drama') then 1 else 0 end as is_drama,
        case when contains_substr(genres, 'Family') then 1 else 0 end as is_family,
        case when contains_substr(genres, 'Fantasy')then 1 else 0 end as is_fantasy,
        case when contains_substr(genres, 'Horror') then 1 else 0 end as is_horror,
        case when contains_substr(genres, 'Music') then 1 else 0 end as is_music,
        case when contains_substr(genres, 'Mystery') then 1 else 0 end as is_mystery,
        case when contains_substr(genres, 'Romance') then 1 else 0 end as is_romance,
        case when contains_substr(genres, 'Science Fiction') then 1 else 0 end as is_scifi,
        case when contains_substr(genres, 'Thriller') then 1 else 0 end as is_thriller,
        case when contains_substr(genres, 'War') then 1 else 0 end as is_war,
        case when contains_substr(genres, 'Western') then 1 else 0 end as is_western
    from moviefull
)

select
    mf.movie_id,
    mp.budget_adj,
    mf.runtime,
    coalesce(di.is_top_director, 0) as is_top_director,
    coalesce(s.star_actor_count, 0) as star_actor_count,
    sr.release_season,
    mp.is_flop,
    mp.is_recession,
    coalesce(di.director_movie_count, 0) as director_movie_count,
    ge.is_action,
    ge.is_adventure,
    ge.is_animation,
    ge.is_comedy,
    ge.is_crime,
    ge.is_documentary,
    ge.is_drama,
    ge.is_family,
    ge.is_fantasy,
    ge.is_horror,
    ge.is_music,
    ge.is_mystery,
    ge.is_romance,
    ge.is_scifi,
    ge.is_thriller,
    ge.is_war,
    ge.is_western,
    sm.studio_flop_rate,
    sm.studio_movie_count,
    sm.studio_avg_movies_per_year
from moviefull as mf
join movie_performance as mp on mf.movie_id = mp.movie_id
left join is_star as s on mf.movie_id = s.movie_id
left join director_info as di on mf.movie_id = di.movie_id
left join season_release as sr on mf.movie_id = sr.movie_id
left join genre_encoding as ge on mf.movie_id = ge.movie_id
left join studio_metrics as sm on mf.movie_id = sm.movie_id
where extract(year from mf.release_date) > 1946 and runtime > 0
