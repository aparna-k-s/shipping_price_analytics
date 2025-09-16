{{
    config(
        materialized='table',
        schema = 'staging',
        tags=['daily', 'datapoints']
    )
}}

WITH
datapoints as (
    SELECT
           dp.D_ID,
           VALID_DATE
    FROM {{ ref('stg_datapoints') }} dp
    JOIN {{ ref('stg_datapoints_with_valid_dates') }} dp_date
    ON dp.d_id = dp_date.d_id
    )
,
charges_w_USD as (
    SELECT  c.D_ID,
            dp.VALID_DATE,
            c.CURRENCY AS ORIGINAL_CURRENCY,
            c.CHARGE_VALUE AS ORIGINAL_CHARGE_VALUE,
            CASE WHEN c.CURRENCY = 'USD' THEN CHARGE_VALUE
                 ELSE CHARGE_VALUE / ex.RATE
            END AS CHARGE_VALUE_USD
    FROM {{ ref('stg_charges_original') }} c
    JOIN datapoints dp
        ON c.D_ID = dp.D_ID
    LEFT JOIN {{ ref('stg_exchange_rates_to_USD') }} ex
        ON c.CURRENCY = ex.CURRENCY
        AND dp.VALID_DATE = ex.DAY

)

SELECT D_ID, VALID_DATE, SUM(CHARGE_VALUE_USD) AS TOTAL_CHARGE_VALUE_USD
    FROM charges_w_USD
    GROUP BY 1,2