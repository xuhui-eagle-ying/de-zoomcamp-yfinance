CREATE OR REPLACE TABLE `yahoo-finance-455223.yfinance_data.day_of_week_dimensional_table` AS
SELECT
  DATE(Date) AS date,
  EXTRACT(YEAR FROM Date) AS year,
  EXTRACT(MONTH FROM Date) AS month,
  EXTRACT(DAY FROM Date) AS day,
  (EXTRACT(DAYOFWEEK FROM Date) - 1) AS weekday
FROM
  `yahoo-finance-455223.yfinance_data.nasdaq_100_stock_data_partitioned_clustered`
GROUP BY
  date, year, month, day, weekday
ORDER BY
  date;