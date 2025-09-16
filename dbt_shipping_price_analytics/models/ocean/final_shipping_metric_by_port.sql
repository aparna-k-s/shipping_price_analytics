{{
    config(
        materialized='table',
        tags=['daily', 'datapoints']
    )
}}

WITH metrics_by_port as (
    SELECT
            dp.ORIGIN_PID,
            dp.DESTINATION_PID,
            dp.VALID_DATE,
            dp.SUPPLIER_ID,
            dp.COMPANY_ID,
            dp.EQUIPMENT_ID,
            dp.AVG_PRICE_USD,
            dp.MEDIAN_PRICE_USD,
            concat(originpt.CODE, '-', destpt.CODE) AS SHIPPING_LANE,
            originpt.CODE AS ORIGIN_PORT_CODE,
            originpt.SLUG AS ORIGIN_SLUG,
            originpt.NAME AS ORIGIN_NAME,
            originpt.COUNTRY AS ORIGIN_COUNTRY,
            destpt.CODE AS DESTINATION_PORT_CODE,
            destpt.SLUG AS DESTINATION_SLUG,
            destpt.NAME AS DESTINATION_NAME,
            destpt.COUNTRY AS DESTINATION_COUNTRY,
            CASE WHEN dq.NUM_SUPPLIERS >= 2 AND dq.NUM_COMPANIES >= 5 THEN TRUE ELSE FALSE END AS DQ_OK
    FROM {{ ref("final_price_agg_by_port") }} dp
    JOIN {{ ref("final_price_dq_check") }} dq
        ON  dp.ORIGIN_PID = dq.ORIGIN_PID
        AND dp.DESTINATION_PID = dq.DESTINATION_PID
        AND dp.VALID_DATE = dq.VALID_DATE
        AND dp.EQUIPMENT_ID = dq.EQUIPMENT_ID
    LEFT JOIN {{ source('staging', 'ports') }} originpt
        ON dp.ORIGIN_PID = originpt.PID
    LEFT JOIN {{ source('staging', 'ports') }} destpt
        ON dp.DESTINATION_PID = destpt.PID
    GROUP BY ALL
)

SELECT *
FROM metrics_by_port