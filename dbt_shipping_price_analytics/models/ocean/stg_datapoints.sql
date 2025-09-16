{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail',
        full_refresh = false,
        tags=['daily', 'datapoints']
    )
}}

WITH raw as (
    SELECT D_ID,
           CREATED,
           ORIGIN_PID,
           DESTINATION_PID,
           VALID_FROM,
           VALID_TO,
           COMPANY_ID,
           SUPPLIER_ID,
           EQUIPMENT_ID,
           FILE_NAME,
           CURRENT_TIMESTAMP AS INSERTED_AT

    FROM {{ source('raw', 'raw_datapoints') }}
    WHERE ORIGIN_PID IS NOT NULL
        AND DESTINATION_PID IS NOT NULL
        AND EQUIPMENT_ID IS NOT NULL

    {% if is_incremental() %}
        AND FILE_NAME NOT IN (
            SELECT DISTINCT FILE_NAME
            FROM {{ this }}
        )
    {% endif %}

    )

SELECT *
    FROM raw