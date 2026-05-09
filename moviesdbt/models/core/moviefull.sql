with raw_data as (
    select *
    from {{ source('movies_raw', 'raw') }}
)

select
    id as movie_id,
    title,
    safe_cast(release_date as date) as release_date,
    safe_cast(runtime as float64) as runtime,
    safe_cast(revenue as float64) as revenue,
    safe_cast(budget as float64) as budget,
    safe_cast(popularity as float64) as popularity,
    safe_cast(vote_count as int64) as vote_count,
    safe_cast(vote_average as float64) as vote_average,
    safe_cast(imdb_rating as float64) as imdb_rating,
    genres,
    production_companies,
    production_countries,
    original_language,
    
    status
from raw_data
