{{
    config(
        materialized='view',
        tags=["port", "region"]
    )
}}

WITH RECURSIVE sub_regions as (
    SELECT SLUG,NAME,PARENT, [SLUG] as SUB_SLUG_LIST
        FROM {{ source('staging', 'regions') }}

    UNION ALL

    SELECT r.SLUG, r.NAME, r.PARENT, list_append(SUB_SLUG_LIST, r.SLUG) as SUB_SLUG_LIST
    FROM sub_regions sr
    JOIN {{ source('staging', 'regions') }} r
    ON sr.PARENT = r.SLUG

    )

SELECT
    SLUG AS REGION_SLUG,
    unnest(array_agg(distinct val order by val)) as SUB_REGION_SLUG
from sub_regions, unnest(SUB_SLUG_LIST) as t(val)
GROUP BY SLUG
ORDER BY SLUG