#!/usr/bin/env python
# coding: utf-8

# Step1: Read yfinance data from gcs data lake

# !pip install google-cloud-storage
# !pip install google-cloud-bigquery

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\nbyin\\.gc\\credentials\\google_credentials.json"

client = storage.Client()
buckets = list(client.list_buckets())
print("Available Buckets:", [bucket.name for bucket in buckets])

spark = SparkSession.builder \
    .appName("StockAnalysis") \
    .config("spark.jars", "C:\\Users\\nbyin\\5_spark\\spark-3.3.2-bin-hadoop3\\jars\\spark-3.3-bigquery-0.42.1.jar") \
    .getOrCreate()

wiki_path = "gs://yfinance-data-lake/nasdaq_100_data/*.csv"
nasdaq_100_df = spark.read.option("header", "true").csv(wiki_path)

ticker_list = [row["Ticker"] for row in nasdaq_100_df.select("Ticker").distinct().collect()]
print(ticker_list)

stocks_path = "gs://yfinance-data-lake/stocks/*/*.parquet"
yfinance_data = spark.read.parquet(stocks_path)

yfinance_with_ticker = yfinance_data.withColumn(
    "ticker", regexp_extract(input_file_name(), r".*/stocks/([^/]+)/.*", 1)
)

yfinance_with_ticker = yfinance_with_ticker.withColumn("Date", F.to_date("Date"))

df_spark_appl = yfinance_with_ticker.filter(col("ticker") == "AAPL")

# Step2: Join yfinance data with nasdaq_100_data from wikipedia

yfinance_with_ticker = yfinance_with_ticker.withColumnRenamed("ticker", "ticker_yf")

combined_df = yfinance_with_ticker.join(nasdaq_100_df, yfinance_with_ticker["ticker_yf"] == nasdaq_100_df["Ticker"], how="inner")

data = combined_df.select(
    col("Company").alias("company_name"),
    col("Ticker"), 
    col("Date"), 
    col("Open"), 
    col("Close"), 
    col("GICS Sector").alias("sector"),
    col("GICS Sub-Industry").alias("sub_industry")
)

# Step3: Group by sector & sub-industry

sector_count = data.groupBy("sector").count()

distinct_sectors = data.select("sector").distinct()

sub_industry_count = data.groupBy("sub_industry").count()

distinct_industry = data.select("sub_industry").distinct()

sector_daily_price = data.groupBy("Date", "sector").agg(
    F.avg("Open").alias("avg_open"),
    F.avg("Close").alias("avg_close")
)

sub_industry_daily_price = data.groupBy("Date", "sub_industry").agg(
    F.avg("Open").alias("avg_open"),
    F.avg("Close").alias("avg_close")
)

# Step4: Daily return and moving average

data = data.withColumn("daily_return", (col("Close") - col("Open")) / col("Open"))
data = data.withColumn("rolling_50_day_avg", F.avg("Close").over(Window.partitionBy("ticker").orderBy("Date").rowsBetween(-50, 0)))
data = data.withColumn("rolling_100_day_avg", F.avg("Close").over(Window.partitionBy("ticker").orderBy("Date").rowsBetween(-100, 0)))
data = data.withColumn("rolling_200_day_avg", F.avg("Close").over(Window.partitionBy("ticker").orderBy("Date").rowsBetween(-200, 0)))

sector_daily_price = sector_daily_price.withColumn("daily_return", (col("avg_close") - col("avg_open")) / col("avg_open"))

sub_industry_daily_price = sub_industry_daily_price.withColumn("daily_return", (col("avg_close") - col("avg_open")) / col("avg_open"))

# Step4: calculate relative return starting Jan 2015

base_date = "2015-01-02"

base_data = data.filter(col("Date") == base_date)
base_data = base_data.select("ticker", "Close").withColumnRenamed("Close", "base_Close")

data = data.join(base_data, on="ticker", how="left")
data = data.withColumn("relative_return", (col("Close") - col("base_Close")) / col("base_Close") * 100)
data = data.drop("base_Close")

# Step6: Get Big Seven dataset

# The Big 7 Tech Players:

# Apple Inc. - AAPL
# Microsoft Corporation - MSFT
# Amazon.com, Inc. - AMZN
# Alphabet Inc. (Google) - GOOGL
# Meta Platforms, Inc. (Facebook) - META
# NVIDIA Corporation - NVDA
# Tesla, Inc. - TSLA

big_seven_tickers = ['AAPL', 'MSFT', 'AMZN', 'AMZN', 'GOOGL', 'META', 'NVDA', 'TSLA']

big_seven_df = data.filter(col("ticker").isin(big_seven_tickers))

# Step7: Annualized Rate of Return

data_yearly = data.groupBy("ticker", F.year("Date").alias("year")).agg(F.avg("daily_return").alias("annual_avg_return"))

big_seven_yearly = big_seven_df.groupBy("ticker", F.year("Date").alias("year")).agg(F.avg("daily_return").alias("annual_avg_return"))

sector_yearly_return = sector_daily_price.groupBy("sector", F.year("Date").alias("year")).agg(F.avg("daily_return").alias("annual_avg_return"))

sub_industry_yearly_return = sub_industry_daily_price.groupBy("sub_industry", F.year("Date").alias("year")).agg(F.avg("daily_return").alias("annual_avg_return"))

# Step8：Load to BigQuery

client = bigquery.Client()

spark.conf.set('temporaryGcsBucket', 'your-gcs-bucket-name')

table_id_1 = "yahoo-finance-455223.yfinance_data.nasdaq_100_stock_data"
pandas_df = data.toPandas()
client.load_table_from_dataframe(pandas_df, table_id_1)

table_id_2 = "yahoo-finance-455223.yfinance_data.big_seven_stock_data"
pandas_df = big_seven_df.toPandas()
client.load_table_from_dataframe(pandas_df, table_id_2)

table_id_3 = "yahoo-finance-455223.yfinance_data.nasdaq_100_yearly_data"
pandas_df = data_yearly.toPandas()
client.load_table_from_dataframe(pandas_df, table_id_3)

table_id_4 = "yahoo-finance-455223.yfinance_data.big_seven_yearly_data"
pandas_df = big_seven_yearly.toPandas()
client.load_table_from_dataframe(pandas_df, table_id_4)

table_id_5 = "yahoo-finance-455223.yfinance_data.sector_yearly_return"
pandas_df = sector_yearly_return.toPandas()
client.load_table_from_dataframe(pandas_df, table_id_5)

table_id_6 = "yahoo-finance-455223.yfinance_data.sub_industry_yearly_return"
pandas_df = sub_industry_yearly_return.toPandas()
client.load_table_from_dataframe(pandas_df, table_id_6)
