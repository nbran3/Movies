with movie_cast as (
    select person_name, person_id, movie_id, person_role
    from {{ ref('movie_cast') }}
    where person_role = 'director'
),
moviefull as (
    select movie_id, revenue, budget, popularity, imdb_rating 
    from {{ ref('moviefull') }}
)

select person_name, person_id, count(mf.movie_id) as num_movies, sum(revenue) as total_revenue, avg(revenue) as avg_revenue, avg(imdb_rating) as avg_rating, case when avg(imdb_rating) > 6.25 and count(mf.movie_id) >= 10 then 1 else 0 end as is_top_director
from movie_cast as mc
join moviefull as mf
on mc.movie_id = mf.movie_id
group by person_name, person_id
order by total_revenue desc
