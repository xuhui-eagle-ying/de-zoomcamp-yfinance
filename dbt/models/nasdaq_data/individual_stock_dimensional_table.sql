-- models/individual_stock_dimensional_table.sql

{{ config(
    materialized='table'
) }}

SELECT
  ticker,
  company_name,
  sector,
  sub_industry
FROM
  {{ ref('nasdaq_100_stock_data_partitioned_clustered') }}
GROUP BY
  ticker, company_name, sector, sub_industry
ORDER BY
  ticker
