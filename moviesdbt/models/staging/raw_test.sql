with raw_data as (
    select *
    from {{ source('movies_raw', 'raw') }}
)
select *
from raw_data