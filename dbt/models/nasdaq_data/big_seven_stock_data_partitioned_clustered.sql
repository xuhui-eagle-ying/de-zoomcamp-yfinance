-- models/big_seven_stock_data_partitioned_clustered.sql

{{ config(
    materialized='table',
    partition_by={'field': 'Date', 'data_type': 'DATE'},
    cluster_by=['Ticker']
) }}

SELECT * 
FROM `yahoo-finance-455223.yfinance_data.big_seven_stock_data`
