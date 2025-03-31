-- models/day_of_week_dimensional_table.sql

{{ config(
    materialized='table'
) }}

SELECT
  DATE(Date) AS date,
  EXTRACT(YEAR FROM Date) AS year,
  EXTRACT(MONTH FROM Date) AS month,
  EXTRACT(DAY FROM Date) AS day,
  (EXTRACT(DAYOFWEEK FROM Date) - 1) AS weekday
FROM
  {{ ref('nasdaq_100_stock_data_partitioned_clustered') }}
GROUP BY
  date, year, month, day, weekday
ORDER BY
  date
