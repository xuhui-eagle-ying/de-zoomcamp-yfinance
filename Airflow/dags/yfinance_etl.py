from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage
import yfinance as yf
import os
import pandas as pd
from datetime import datetime

# GCS Bucket name
BUCKET_NAME = "yfinance-data-lake"

# Fetch NASDAQ-100 stock tickers from Wikipedia
def fetch_nasdaq_100_tickers():
    url = 'https://en.wikipedia.org/wiki/NASDAQ-100'
    tables = pd.read_html(url)
    
    # Extract the tickers from the fourth table
    nasdaq_100 = tables[4]
    tickers = nasdaq_100['Ticker'].tolist()
    return tickers

# Fetch Wikipedia data and upload it to GCS as CSV
def fetch_and_upload_wikipedia_data():
    url = 'https://en.wikipedia.org/wiki/NASDAQ-100'
    tables = pd.read_html(url)
    
    # Extract the data from the fourth table
    nasdaq_100 = tables[4]
    
    # Save the data to a CSV file locally
    local_csv_path = '/opt/airflow/tmp/nasdaq_100.csv'
    nasdaq_100.to_csv(local_csv_path, index=False)

    # Upload the CSV file to Google Cloud Storage (GCS)
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    blob = bucket.blob('nasdaq_100_data/nasdaq_100.csv')
    blob.upload_from_filename(local_csv_path)

    # Optionally, delete the local CSV file after uploading
    os.remove(local_csv_path)

# Fetch stock data from Yahoo Finance and upload to GCS as Parquet (one file per year)
def fetch_and_upload_yfinance_data(stock: str, year: str):
    os.makedirs('/opt/airflow/tmp', exist_ok=True)
    
    # Fetch stock data from Yahoo Finance
    ticker = yf.Ticker(stock)
    data = ticker.history(period="1d", start=f"{year}-01-01", end=f"{year}-12-31")
    
    # Process data
    data.reset_index(inplace=True)
    data["Date"] = data["Date"].astype(str)

    # Save the data as a Parquet file
    df_parquet_path = f"/opt/airflow/tmp/{stock}_{year}.parquet"
    data.to_parquet(df_parquet_path, engine='pyarrow')

    # Upload the Parquet file to Google Cloud Storage (GCS)
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    blob = bucket.blob(f"stocks/{stock}/{stock}_{year}.parquet")
    blob.upload_from_filename(df_parquet_path)
    
    # Optionally, delete the local Parquet file after uploading
    os.remove(df_parquet_path)

# Fetch NASDAQ-100 tickers and create a task for each stock to process yearly data
def fetch_and_upload_data_for_stock(stock: str, years: list):
    previous_task = None  # This will keep track of the last task to set dependencies

    with TaskGroup(group_id=f'{stock}_group') as tg:
        for year in years:
            task = PythonOperator(
                task_id=f'fetch_and_upload_{stock}_{year}',
                python_callable=fetch_and_upload_yfinance_data,
                op_args=[stock, year],
            )
            
            # If there was a previous task, set dependency (previous task >> current task)
            if previous_task:
                previous_task >> task
            
            # Update previous_task to be the current task for the next iteration
            previous_task = task

    return tg

# Create the Airflow DAG
with DAG(
    "nasdaq_100_etl",
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
    },
    schedule_interval="@daily",  # To run every day
    catchup=False,
) as dag:

    # Fetch NASDAQ-100 tickers
    tickers = fetch_nasdaq_100_tickers()
    years = [str(year) for year in range(2015, 2025)]  # Specify the years range

    # Task to upload Wikipedia data to GCS
    upload_wikipedia_data_task = PythonOperator(
        task_id='fetch_and_upload_wikipedia_data',
        python_callable=fetch_and_upload_wikipedia_data,
        dag=dag,
    )

    # For each stock in NASDAQ-100, create a task group to process yearly data
    for stock in tickers:
        fetch_and_upload_data_for_stock(stock, years)

    # Define task dependencies
    upload_wikipedia_data_task  # This will run first