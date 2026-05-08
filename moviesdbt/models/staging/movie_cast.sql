with actors as (
    select id as movie_id, trim(actor) as name, 'actor' as role
    from {{ source('movies_raw', 'actors') }}
    where actor is not null and trim(actor) != '' and actor not like '%"%'
),

producers as (
    select id as movie_id, trim(producer) as name, 'producer' as role
    from {{ source('movies_raw', 'producers') }}
    where producer is not null and trim(producer) != '' and producer not like '%"%'
),

directors as (
    select id as movie_id, trim(director) as name, 'director' as role
    from {{ source('movies_raw', 'directors') }}
    where director is not null and trim(director) != '' and director not like '%"%'
),

all_cast as (
    select * from actors
    union all
    select * from producers
    union all
    select * from directors
),

people as (
    select * from {{ ref('people_dim') }}
)

select
    c.movie_id,
    p.person_id,
    c.name as person_name,
    c.role as person_role
from all_cast c
join people p on c.name = p.name
