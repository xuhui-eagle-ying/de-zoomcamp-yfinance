-- models/nasdaq_100_yearly_data_clustered.sql

{{ config(
    materialized='table',
    cluster_by=['Ticker']
) }}

SELECT * 
FROM `yahoo-finance-455223.yfinance_data.nasdaq_100_yearly_data`
