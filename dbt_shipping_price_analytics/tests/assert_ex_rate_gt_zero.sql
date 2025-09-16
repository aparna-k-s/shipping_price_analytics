{{config(
    tags=["currency_ex"]
)}}

WITH bad_data AS (
    SELECT *
    FROM {{ source('raw', 'raw_exchange_rates_to_USD') }}
    WHERE RATE < 0
    ORDER BY RANDOM()
    LIMIT 10
)
SELECT *
FROM bad_data