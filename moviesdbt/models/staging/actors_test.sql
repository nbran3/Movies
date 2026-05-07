with actors_data as(
    select *
    from {{ source('movies_raw', 'actors') }}
)
select actor as actor_name
from actors_data