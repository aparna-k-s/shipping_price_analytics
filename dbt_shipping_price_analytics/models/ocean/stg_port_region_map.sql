{{
    config(
        materialized='table',
        tags=["port", "region"]
    )
}}

WITH ports_with_all_parent_regions as (
    SELECT
            PID,
            CODE,
            SLUG AS PORT_REGION_SLUG,
            NAME,
            COUNTRY,
            r.REGION_SLUG AS PARENT_REGION_SLUG
    FROM {{ source('staging', 'ports') }} p
    JOIN {{ ref("stg_region_subregion_map") }} r
    ON p.SLUG = r.SUB_REGION_SLUG

)

SELECT *
    FROM ports_with_all_parent_regions