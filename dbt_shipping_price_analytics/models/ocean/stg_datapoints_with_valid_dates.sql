{{
    config(
        materialized='table',
        tags=['daily', 'datapoints']
    )
}}

WITH RECURSIVE contract_valid_dates as (
    SELECT D_ID,
           VALID_FROM,
           VALID_TO,
           VALID_FROM AS VALID_DATE
    FROM {{ source('raw', 'raw_datapoints') }}

    UNION ALL

    SELECT D_ID,
           VALID_FROM,
           VALID_TO,
           VALID_DATE + INTERVAL '1 day' AS VALID_DATE
    FROM contract_valid_dates
    WHERE VALID_DATE + INTERVAL '1 day' <= VALID_TO
)

SELECT *
    FROM contract_valid_dates
