{{
    config(
        materialized='table',
        schema = 'staging',
        tags=['daily', 'datapoints']
    )
}}

WITH dp_charges as (
    SELECT  dp.D_ID,
            ORIGIN_PID,
            DESTINATION_PID,
            VALID_DATE,
            COMPANY_ID,
            SUPPLIER_ID,
            EQUIPMENT_ID,
            TOTAL_CHARGE_VALUE_USD AS PRICE_USD
    FROM {{ ref('stg_charges_USD_agg') }} c
    JOIN {{ ref('stg_datapoints') }} dp
        ON c.D_ID = dp.D_ID
)

SELECT * FROM dp_charges