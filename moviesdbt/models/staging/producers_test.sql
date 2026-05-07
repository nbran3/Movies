with producers_data as(
    select *
    from {{ source('movies_raw', 'producers') }}
)
select producer as producer_name
from producers_data