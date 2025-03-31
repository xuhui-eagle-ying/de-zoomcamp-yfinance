CREATE OR REPLACE TABLE `yahoo-finance-455223.yfinance_data.stock_with_day_of_week` 
PARTITION BY date
CLUSTER BY ticker
AS
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
  `yahoo-finance-455223.yfinance_data.nasdaq_100_stock_data_partitioned_clustered` s
JOIN
  `yahoo-finance-455223.yfinance_data.day_of_week_dimensional_table` d
ON
  DATE(s.Date) = d.date;