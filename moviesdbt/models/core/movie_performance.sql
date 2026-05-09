with moviefull as (
    select *
    from {{ ref('moviefull') }}
    where budget > 0 and revenue > 0
),
econ as (
    select
        extract(year from observation_date) as year,
        avg(CPIAUCSL) as cpi,
        max(JHDUSRGDPBR) as is_recession
    from {{ ref('fredgraph') }}
    group by 1
)


select
    movie_id,
    title,
    release_date,
    budget,
    revenue,
    revenue - budget as profit,
    safe_divide(revenue, budget) as roi,
    econ.cpi,
    econ.is_recession,
    budget * (320.0 / econ.cpi) as budget_adj,
    case
        when revenue < budget then 'Flop'
        when safe_divide(revenue, budget) between 1.0 and 1.5 then 'Break Even'
        when safe_divide(revenue, budget) between 1.5 and 3.0 then 'Hit'
        else 'Blockbuster'
    end                          as performance_tier,
    case when revenue < budget then 1 else 0 end as is_flop,
from moviefull
join econ on extract(year from moviefull.release_date) = econ.year
