{{
    config(
        materialized='view'
    )
}}

WITH
  valid_dates AS (
    SELECT generate_series::date AS date
    FROM generate_series(
            '2021-01-01'::DATE, '2022-06-01'::DATE, '1 day'::INTERVAL)
  )
SELECT *
FROM valid_dates
