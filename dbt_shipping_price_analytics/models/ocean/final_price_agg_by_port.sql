{{
    config(
        materialized='table',
        tags=['daily', 'datapoints']
    )
}}

WITH dp_agg as (
    SELECT
            ORIGIN_PID,
            DESTINATION_PID,
            VALID_DATE,
            SUPPLIER_ID,
            COMPANY_ID,
            EQUIPMENT_ID,
            AVG(PRICE_USD) AS AVG_PRICE_USD,
            MEDIAN(PRICE_USD) AS MEDIAN_PRICE_USD
    FROM {{ ref("stg_datapoints_price_USD") }} dp
    GROUP BY 1,2,3,4,5,6
)

SELECT *
FROM dp_agg