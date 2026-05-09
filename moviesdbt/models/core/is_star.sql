with movie_cast as (
    select person_name, person_id, movie_id, person_role
    from {{ ref('movie_cast') }}
    where person_role = 'actor'
),

stars as (
    select lower(Name) as name
    from {{ ref('top_actors') }}
),

star_flag as (
    select mc.movie_id, mc.person_id, mc.person_name, mc.person_role,
           case when s.name is not null then 1 else 0 end as is_known_star
    from movie_cast mc
    left join stars s on lower(mc.person_name) = s.name
)

select movie_id, sum(is_known_star) as star_actor_count
from star_flag
group by movie_id