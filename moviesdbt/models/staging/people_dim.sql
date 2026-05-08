with actors_data as (
    select trim(actor) as name, 'actor' as role, id as movie_id
    from {{ source('movies_raw', 'actors') }}
    where actor is not null
      and trim(actor) != ''
      and actor not like '%"%'
),

producers_data as (
    select trim(producer) as name, 'producer' as role, id as movie_id
    from {{ source('movies_raw', 'producers') }}
    where producer is not null
      and trim(producer) != ''
      and producer not like '%"%'
),

directors_data as (
    select trim(director) as name, 'director' as role, id as movie_id
    from {{ source('movies_raw', 'directors') }}
    where director is not null
      and trim(director) != ''
      and director not like '%"%'
),

all_people as (
    select distinct name from (
        select name from actors_data
        union all
        select name from producers_data
        union all
        select name from directors_data
    ) combined
)

select
    abs(farm_fingerprint(name)) as person_id,
    name
from all_people