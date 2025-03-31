-- models/stock_with_day_of_week.sql

{{ config(
    materialized='table',
    partition_by={'field': 'Date', 'data_type': 'DATE'},
    cluster_by=['ticker']
) }}

SELECT
  s.ticker,
  s.company_name,
  s.sector,
  s.sub_industry,
  d.date,
  d.year,
  d.month,
  d.day,
  d.weekday,
  s.daily_return
FROM
  {{ ref('nasdaq_100_stock_data_partitioned_clustered') }} s
JOIN
  {{ ref('day_of_week_dimensional_table') }} d
ON
  DATE(s.Date) = d.date
  