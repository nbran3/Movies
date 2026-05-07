with actors_data as (
    select trim(actor) as name, 'actor' as role
    from {{ source('movies_raw', 'actors') }}
    where actor is not null
      and trim(actor) != ''
      and actor not like '%"%'
),

producers_data as (
    select trim(producer) as name, 'producer' as role
    from {{ source('movies_raw', 'producers') }}
    where producer is not null
      and trim(producer) != ''
      and producer not like '%"%'
),

directors_data as (
    select trim(director) as name, 'director' as role
    from {{ source('movies_raw', 'directors') }}
    where director is not null
      and trim(director) != ''
      and director not like '%"%'
),

all_people as (
    select name, role from actors_data
    union all
    select name, role from producers_data
    union all
    select name, role from directors_data
)

select
    row_number() over (order by name) as person_id,
    name,
    role
from all_people