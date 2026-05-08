with directors_data as(
    select *
    from {{ source('movies_raw', 'directors') }}
)
select id as movie_id, director as director_name
from directors_data