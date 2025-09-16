{{
    config(
        materialized='table',
        tags=['daily', 'datapoints']
    )
}}

WITH count_agg as (
    SELECT
            ORIGIN_PID,
            DESTINATION_PID,
            EQUIPMENT_ID,
            VALID_DATE,
            COUNT(DISTINCT SUPPLIER_ID) AS NUM_SUPPLIERS,
            COUNT(DISTINCT COMPANY_ID) AS NUM_COMPANIES
    FROM {{ ref("stg_datapoints_price_USD") }} dp
    GROUP BY 1,2,3,4
)

SELECT *
FROM count_agg