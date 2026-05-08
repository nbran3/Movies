import time
import requests
import os
import zipfile
from pyspark.sql import SparkSession, functions as F
from dotenv import load_dotenv

load_dotenv()

google_credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
dataset = os.getenv('DATASET_NAME')
raw_table_name = os.getenv('RAW_TABLE_NAME')
actors_table_name = os.getenv('ACTORS_TABLE_NAME')
directors_table_name = os.getenv('DIRECTORS_TABLE_NAME')
producers_table_name = os.getenv('PRODUCERS_TABLE_NAME')

# Define the source and destination
url = "https://www.kaggle.com/api/v1/datasets/download/alanvourch/tmdb-movies-daily-updates"
folder_path = os.path.expanduser("~/Python/Movies")
file_path = os.path.join(folder_path, "tmdb-all_movies.zip")

# Ensure the directory exists
os.makedirs(folder_path, exist_ok=True)

# Download the file
print(f"Downloading dataset to {file_path}...")
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print("Download complete!")
else:
    print(f"Failed to download. Status code: {response.status_code}")


zip_path = './tmdb-all_movies.zip'
extract_path = './Data'

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Movies_to_BigQuery") \
     .config("spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.37.0,"
            "javax.inject:javax.inject:1") \
    .config("spark.sql.debug.maxToStringFields", 100) \
    .getOrCreate()

# Verify the connection
print(f"Spark version: {spark.version}")

main_df = spark.read.option("header", "true").csv("./Data/TMDB_all_movies.csv")

starttime = time.time()

def ingest_to_bigquery(df, table_name):
    df.write \
        .format("bigquery") \
        .option("writeMethod", "direct") \
        .option("credentialsFile", google_credentials_path) \
        .option("table", f"{dataset}.{table_name}") \
        .option("batchsize", "50000") \
        .mode("overwrite") \
        .save()


def actors_table_to_bgq(df, dataset, table_name):
    actors_df = (
        df.select("id",F.explode(F.split(F.col("cast"), ", ")).alias("actor"))
    )

    actors_df.write \
        .format("bigquery") \
        .option("writeMethod", "direct") \
        .option("credentialsFile", google_credentials_path) \
        .option("table", f"{dataset}.{table_name}") \
        .option("batchsize", "50000") \
        .mode("overwrite") \
        .save()

def producers_table_to_bgq(df, dataset, table_name):
    producers_df = (
        df.select("id", F.explode(F.split(F.col("producers"), ", ")).alias("producer"))
    )

    producers_df.write \
        .format("bigquery") \
        .option("writeMethod", "direct") \
        .option("credentialsFile", google_credentials_path) \
        .option("table", f"{dataset}.{table_name}") \
        .option("batchsize", "50000") \
        .mode("overwrite") \
        .save()

def directors_table_to_bgq(df, dataset, table_name):
    directors_df = (
        df.select("id", F.explode(F.split(F.col("director"), ", ")).alias("director"))
    )

    directors_df.write \
        .format("bigquery") \
        .option("writeMethod", "direct") \
        .option("credentialsFile", google_credentials_path) \
        .option("table", f"{dataset}.{table_name}") \
        .option("batchsize", "50000") \
        .mode("overwrite") \
        .save()


ingest_to_bigquery(main_df, raw_table_name)
print(f"Ingested raw data to BigQuery table: {dataset}.{raw_table_name}")

actors_table_to_bgq(main_df, dataset, actors_table_name)
print(f"Ingested actors data to BigQuery table: {dataset}.{actors_table_name}")

producers_table_to_bgq(main_df, dataset, producers_table_name)
print(f"Ingested producers data to BigQuery table: {dataset}.{producers_table_name}")

directors_table_to_bgq(main_df, dataset, directors_table_name)
print(f"Ingested directors data to BigQuery table: {dataset}.{directors_table_name}")

endingtime = time.time()
total_time = endingtime - starttime
print(f"Total time taken to ingest data: {total_time} seconds")

spark.stop()
print("Stopped spark")