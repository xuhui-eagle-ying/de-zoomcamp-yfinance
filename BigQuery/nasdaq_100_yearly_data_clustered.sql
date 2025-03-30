CREATE OR REPLACE TABLE `yahoo-finance-455223.yfinance_data.nasdaq_100_yearly_data_clustered` 
CLUSTER BY Ticker
AS
SELECT * FROM `yahoo-finance-455223.yfinance_data.nasdaq_100_yearly_data`;