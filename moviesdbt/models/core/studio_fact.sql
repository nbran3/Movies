with all_studio_movies as (
    select
        mf.movie_id,
        lower(regexp_replace(trim(studio), r'\s+', ' ')) as studio_key,
        any_value(regexp_replace(trim(studio), r'\s+', ' ')) as studio
    from {{ ref('moviefull') }} mf
    cross join unnest(split(mf.production_companies, ',')) as studio
    where mf.release_date is not null
      and lower(regexp_replace(trim(studio), r'\s+', ' ')) is not null
      and lower(regexp_replace(trim(studio), r'\s+', ' ')) != ''
      and length(lower(regexp_replace(trim(studio), r'\s+', ' '))) between 2 and 100
      and not regexp_contains(lower(regexp_replace(trim(studio), r'\s+', ' ')), r'^\d+$')
      and not regexp_contains(lower(regexp_replace(trim(studio), r'\s+', ' ')), r'^(action|adventure|animation|comedy|crime|documentary|drama|family|fantasy|history|horror|music|mystery|romance|science fiction|tv movie|thriller|war|western)$')
    group by mf.movie_id, studio_key
),

studio_counts as (
    select
        studio_key,
        any_value(studio) as studio,
        count(distinct movie_id) as movie_count
    from all_studio_movies
    group by studio_key
),

ranked_studios as (
    select
        *,
        row_number() over (order by movie_count desc, studio_key) as movie_count_rank
    from studio_counts
)

select
    studio_key,
    studio,
    movie_count,
    case
        when regexp_contains(
            studio_key,
            r'(walt disney|disney|pixar|marvel|lucasfilm|warner bros|new line cinema|universal pictures|paramount pictures|columbia pictures|sony pictures|20th century|twentieth century|fox|metro-goldwyn-mayer|mgm|lionsgate|dreamworks|netflix|amazon studios|united artists)'
        ) then 1
        else 0
    end as is_major_studio,
    case when movie_count_rank <= 10 then 1 else 0 end as is_top_10_studio
from ranked_studios
