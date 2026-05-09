with all_studio_movies as (
    select
        mf.movie_id,
        extract(year from mf.release_date) as release_year,
        trim(studio) as studio
    from {{ ref('moviefull') }} mf
    cross join unnest(split(mf.production_companies, ',')) as studio
    where mf.release_date is not null
      and length(trim(studio)) < 100
      and trim(studio) != ''
),

financial_studio_movies as (
    select
        trim(studio) as studio,
        mp.is_flop
    from {{ ref('movie_performance') }} mp
    join {{ ref('moviefull') }} mf on mp.movie_id = mf.movie_id
    cross join unnest(split(mf.production_companies, ',')) as studio
    where mf.release_date is not null
      and length(trim(studio)) < 100
      and trim(studio) != ''
),

yearly_counts as (
    select studio, release_year, count(*) as movies_that_year
    from all_studio_movies
    group by studio, release_year
)

select
    asm.studio,
    count(*)                                            as movie_count,
    safe_divide(sum(fsm.is_flop), count(fsm.is_flop)) as studio_flop_rate,
    avg(yc.movies_that_year)                           as avg_movies_per_year
from all_studio_movies asm
left join financial_studio_movies fsm on asm.studio = fsm.studio
join yearly_counts yc on asm.studio = yc.studio and asm.release_year = yc.release_year
group by asm.studio
