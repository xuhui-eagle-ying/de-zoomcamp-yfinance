CREATE OR REPLACE TABLE `yahoo-finance-455223.yfinance_data.nasdaq_100_stock_data_partitioned_clustered` 
PARTITION BY Date
CLUSTER BY Ticker
AS
SELECT * FROM `yahoo-finance-455223.yfinance_data.nasdaq_100_stock_data`;