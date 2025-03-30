CREATE OR REPLACE TABLE `yahoo-finance-455223.yfinance_data.individual_stock_dimensional_table` AS
SELECT
  ticker,
  company_name,
  sector,
  sub_industry
FROM
  `yahoo-finance-455223.yfinance_data.nasdaq_100_stock_data_partitioned_clustered`
GROUP BY
  ticker, company_name, sector, sub_industry
ORDER BY
  ticker;