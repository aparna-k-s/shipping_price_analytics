{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='false',
        full_refresh = false,
        tags=['daily', 'datapoints']
    )
}}

WITH charges as (
    SELECT
            D_ID,
            CURRENCY,
            CHARGE_VALUE,
            FILE_NAME,
           CURRENT_TIMESTAMP AS INSERTED_AT
    FROM {{ source('raw', 'raw_charges') }} charges
    WHERE D_ID IS NOT NULL

    {% if is_incremental() %}
        AND FILE_NAME NOT IN (
            SELECT DISTINCT FILE_NAME
            FROM {{ this }}
        )
    {% endif %}

)
SELECT * FROM charges