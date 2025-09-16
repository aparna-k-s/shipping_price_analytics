{{
    config(
        materialized='table',
        tags=['daily', 'datapoints']
    )
}}

WITH metrics_by_region as (
    SELECT
            dp.EQUIPMENT_ID,
            dp.VALID_DATE,
            origrgn.PARENT_REGION_SLUG AS ORIGIN_SLUG,
            origname.NAME AS ORIGIN_NAME,
            destrgn.PARENT_REGION_SLUG AS DESTINATION_SLUG,
            destname.NAME AS DESTINATION_NAME,
            AVG(PRICE_USD) AS AVG_PRICE_USD,
            MEDIAN(PRICE_USD) AS MEDIAN_PRICE_USD,
            CASE WHEN dq.NUM_SUPPLIERS >= 2 AND dq.NUM_COMPANIES >= 5 THEN TRUE ELSE FALSE END AS DQ_OK
    FROM {{ ref("stg_datapoints_price_USD") }} dp
    JOIN {{ ref("final_price_dq_check") }} dq
        ON  dp.ORIGIN_PID = dq.ORIGIN_PID
        AND dp.DESTINATION_PID = dq.DESTINATION_PID
        AND dp.VALID_DATE = dq.VALID_DATE
        AND dp.EQUIPMENT_ID = dq.EQUIPMENT_ID
    LEFT JOIN {{ ref("stg_port_region_map") }} origrgn
        ON dp.ORIGIN_PID = origrgn.PID
    LEFT JOIN {{ source('staging', 'regions') }} origname
        ON origrgn.PARENT_REGION_SLUG = origname.SLUG
    LEFT JOIN {{ ref("stg_port_region_map") }} destrgn
        ON dp.DESTINATION_PID = destrgn.PID
    LEFT JOIN {{ source('staging', 'regions') }} destname
        ON destrgn.PARENT_REGION_SLUG = destname.SLUG
    GROUP BY ALL
)

SELECT *
FROM metrics_by_region