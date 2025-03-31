# Nasdaq 100 Stock Analysis - ELT Pipeline & Visualization

This project analyzes the historical data of Nasdaq 100 index components over the past decade. The ELT data pipeline is orchestrated using Airflow, loads data into a GCS data lake, processes it with PySpark, stores it in BigQuery, transforms it with dbt, and finally visualizes it using Metabase to explore patterns in Nasdaq 100 stock prices.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Technologies Used](#technologies-used)
3. [Data Sources](#data-sources)
4. [Setting Up GCP Environment](#setting-up-gcp-environment)
5. [Data Orchestration](#data-orchestration-with-airflow)
6. [Data Transformation](#data-transformation)
7. [Data Warehouse](#data-warehouse)
8. [Data Visualization](#data-visualization)

## Project Overview
The pipeline follows these steps:
- **Extract**: Retrieve data from Wikipedia (Nasdaq 100 component info) and Yahoo Finance (historical stock prices).
- **Load**: Store raw data in Google Cloud Storage (GCS) as CSV and Parquet.
- **Transform**: Use PySpark to clean and structure data, then load it into BigQuery.
- **Model**: Apply dbt for table partitioning, clustering, and dimensional modeling.
- **Visualize**: Use Metabase to explore trends in Nasdaq 100 stock performance.

## Technologies Used
- **Docker**: Containerization for local development.
- **Terraform**: Automates GCP infrastructure setup.
- **Airflow**: Orchestrates data ingestion workflows.
- **PySpark**: Processes large-scale stock data efficiently.
- **BigQuery**: Stores and structures data for querying.
- **dbt**: Automates SQL transformations.
- **Metabase**: Provides interactive data visualization.

## Data Sources
- **Wikipedia**: Scraped to obtain Nasdaq 100 component stock symbols, company names, sectors, and sub-industries.
- **Yahoo Finance API**: Used to fetch daily stock price data from 2015 to present.

## Setting Up GCP Environment
First, ensure you have Terraform installed:
```bash
terraform --version
```

Navigate to your Terraform directory and initialize the setup:
```bash
cd /path/to/Terraform
terraform init
terraform plan
terraform apply
```

This automatically creates:
- **Google Storage Bucket** (GCS data lake)
- **BigQuery Dataset**

Manual setup required:
- **GCP Project ID**
- **Service Account with necessary permissions**

To clean up resources after the project, use:
```bash
terraform destroy
```

## Data Orchestration
To set up Airflow:
```bash
cd /path/to/Airflow
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Start Airflow using Docker:
```bash
docker-compose build --no-cache
docker-compose up airflow-init
docker-compose up -d
```

Airflow processes:
1. Scrapes Wikipedia for Nasdaq 100 stock metadata.
2. Fetches daily stock prices from Yahoo Finance.
3. Saves data into GCS in CSV (Wikipedia) and Parquet (Yahoo Finance).

Each stock gets its own folder in GCS:
```plaintext
GCS Bucket
 ├── AAPL/
 │   ├── 2015.parquet
 │   ├── 2016.parquet
 │   └── ...
 ├── MSFT/
 │   ├── 2015.parquet
 │   ├── 2016.parquet
 │   └── ...
```

## Data Transformation
PySpark processes the stock data:
```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("NasdaqAnalysis").getOrCreate()
data = spark.read.parquet("gs://your_bucket/AAPL/*.parquet")

# Calculate daily returns and moving averages
data = data.withColumn("daily_return", (F.col("Close") - F.col("Open")) / F.col("Open"))
data = data.withColumn("rolling_50_day_avg", F.avg("Close").over(Window.partitionBy("ticker").orderBy("Date").rowsBetween(-50, 0)))
data.write.format("bigquery").option("table", "your_project.dataset.stock_data").save()
```

Using dbt to automate SQL transformations:
```bash
dbt init
dbt debug
dbt clean
dbt run
```

## Data Warehouse
BigQuery processes:
- **Partitioning & Clustering**: Optimize storage and query performance.
- **Dimensional Modeling**: Create lookup tables for stock metadata.
- **Aggregations**: Compute annual returns and moving averages.

SQL example to calculate annual returns:
```sql
SELECT ticker, EXTRACT(YEAR FROM Date) AS year, AVG(daily_return) AS annual_avg_return
FROM `your_project.dataset.stock_data`
GROUP BY ticker, year;
```

## Data Visualization
Run Metabase in Docker:
```bash
docker pull metabase/metabase:latest
docker run -d -p 3000:3000 --name metabase metabase/metabase
```

Access Metabase at `localhost:3000` and connect it to BigQuery.

### Dashboard Insights:
1. **Weekly Stock Trends**: Average daily returns for each weekday.
2. **Monthly Trends**: Average returns for each month.
3. **Top 10 Sub-industries**: Highest annual return sectors.
4. **Sector Distribution**: Breakdown of Nasdaq 100 companies by sector.
5. **Relative Returns**: Compare top 7 Nasdaq stocks vs. the index.
6. **200-Day Moving Averages**: Long-term trends of top Nasdaq stocks.

## Future Work
- Analyze correlation between top 7 stocks' trends.
- Implement LookML in Looker for deeper insights.
- Expand analysis to other indices (S&P 500, Dow Jones).

Feel free to contribute or suggest improvements!
