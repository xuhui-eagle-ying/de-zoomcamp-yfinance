-- models/big_seven_vs_index.sql

{{ config(
    materialized='table',
    partition_by={'field': 'Date', 'data_type': 'DATE'},
) }}

WITH
  nasdaq_100_avg_return AS (
    SELECT
      Date,
      AVG(relative_return) AS index_return
    FROM
      {{ ref('nasdaq_100_stock_data_partitioned_clustered') }}
    GROUP BY
      date
  )
  
-- Combine Big Seven stocks data and Nasdaq 100 index return
SELECT
  DISTINCT
  b.Date,
  b.company_name,
  b.ticker,
  b.relative_return
FROM
  {{ ref('big_seven_stock_data_partitioned_clustered') }} b

UNION ALL

SELECT
  DISTINCT
  n.Date,
  'NASDAQ 100 Index' AS company_name,  -- Specify a label for the "company" (NASDAQ 100 Index)
  'index' AS ticker,  -- Specify a label for the "ticker"
  n.index_return AS relative_return
FROM
  nasdaq_100_avg_return n
