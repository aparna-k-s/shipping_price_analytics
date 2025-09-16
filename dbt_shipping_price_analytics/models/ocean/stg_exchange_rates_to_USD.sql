{{
    config(
        materialized='table',
        tags=['daily', "currency_ex"]
    )
}}

WITH deduped as (
    SELECT *, RANK() OVER (PARTITION BY DAY, CURRENCY  ORDER BY DAY DESC) AS ranking
        FROM {{ source('raw', 'raw_exchange_rates_to_USD') }}
        WHERE RATE > 0
    )

select *
from deduped
where ranking = 1