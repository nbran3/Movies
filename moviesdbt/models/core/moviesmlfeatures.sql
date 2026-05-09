with moviefull as (
    select *
    from {{ ref('moviefull') }}
),

is_star as (
    select *
    from {{ ref('is_star') }}
),

director_movie as (
    select mc.movie_id, max(td.is_top_director) as is_top_director
    from {{ ref('movie_cast') }} mc
    join {{ ref('top_director') }} td on mc.person_id = td.person_id
    where mc.person_role = 'director'
    group by mc.movie_id
),

movie_performance as (
    select movie_id, is_flop
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
)

select
    mf.movie_id,
    mf.budget,
    mf.runtime,
    coalesce(dm.is_top_director, 0) as is_top_director,
    coalesce(s.star_actor_count, 0) as star_actor_count,
    sr.release_season,
    mp.is_flop
from moviefull as mf
join movie_performance as mp on mf.movie_id = mp.movie_id
left join is_star as s on mf.movie_id = s.movie_id
left join director_movie as dm on mf.movie_id = dm.movie_id
left join season_release as sr on mf.movie_id = sr.movie_id
