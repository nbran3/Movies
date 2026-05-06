import requests
import os
import zipfile
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

google_credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
dataset = os.getenv('DATASET_NAME')
table_name = os.getenv('TABLE_NAME')


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

df = spark.read.option("header", "true").csv("/Users/noahbrannon/Python/Movies/Data/TMDB_all_movies.csv")


def ingest_to_bigquery(df, table_name):
    df.write \
        .format("bigquery") \
        .option("writeMethod", "direct") \
        .option("table", f"{dataset}.{table_name}") \
        .mode("overwrite") \
        .save()


ingest_to_bigquery(df, table_name)

spark.stop
print("Stoped spark")