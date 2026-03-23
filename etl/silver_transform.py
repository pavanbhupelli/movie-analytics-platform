from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import IntegerType
import boto3
import os

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder \
    .appName("Movie Analytics Silver Layer") \
    .getOrCreate()

# ---------------- CONFIG ----------------
bucket = "movie-analytics-platform-data"
s3 = boto3.client("s3")

# ---------------- HELPER FUNCTION ----------------

def standardize_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower().strip().replace(" ", "_"))
    return df


# ---------------- CLEAN FUNCTIONS ----------------

def clean_ott(df, platform_name):
    df = standardize_columns(df)

    cols = [
        "title", "director", "cast",
        "release_year", "duration",
        "rating", "listed_in"
    ]

    df = df.select([c for c in df.columns if c in cols])

    df = df.fillna({
        "title": "Unknown",
        "director": "Unknown",
        "cast": "Unknown",
        "duration": "Unknown",
        "rating": "Unknown",
        "listed_in": "Unknown"
    })

    df = df.withColumn(
        "release_year",
        col("release_year").cast(IntegerType())
    ).fillna({"release_year": 0})

    df = df.withColumn("platform", lit(platform_name))

    return df


def clean_movies(df):
    df = standardize_columns(df)

    df = df.withColumnRenamed("movie_title", "title") \
           .withColumnRenamed("director_name", "director") \
           .withColumnRenamed("actor_1_name", "cast") \
           .withColumnRenamed("genres", "listed_in")

    cols = ["title", "director", "cast", "listed_in"]
    df = df.select([c for c in df.columns if c in cols])

    df = df.withColumn("duration", lit("Unknown")) \
           .withColumn("rating", lit("Unknown")) \
           .withColumn("release_year", lit(0)) \
           .fillna("Unknown")

    df = df.withColumn("platform", lit("Movies"))

    return df


def clean_location(df):
    df = standardize_columns(df)

    df = df.withColumnRenamed("locations", "location") \
           .withColumnRenamed("actor_1", "actor1") \
           .withColumnRenamed("actor_2", "actor2") \
           .withColumnRenamed("actor_3", "actor3")

    cols = [
        "title", "release_year", "location",
        "fun_facts", "production_company",
        "director", "writer",
        "actor1", "actor2", "actor3"
    ]

    df = df.select([c for c in df.columns if c in cols])

    df = df.fillna("Unknown")

    df = df.withColumn(
        "release_year",
        col("release_year").cast(IntegerType())
    ).fillna({"release_year": 0})

    return df


def clean_theatre(df):
    df = standardize_columns(df)

    cols = [
        "theatre_name", "city", "type",
        "theatre_chain",
        "average_ticket_price",
        "total_seats",
        "no_screens"
    ]

    df = df.select([c for c in df.columns if c in cols])

    df = df.fillna({
        "theatre_name": "Unknown",
        "city": "Unknown",
        "type": "Unknown",
        "theatre_chain": "Unknown"
    })

    df = df.withColumn("average_ticket_price", col("average_ticket_price").cast(IntegerType())) \
           .withColumn("total_seats", col("total_seats").cast(IntegerType())) \
           .withColumn("no_screens", col("no_screens").cast(IntegerType())) \
           .fillna(0)

    return df


# ---------------- PROCESS ----------------

ott_data = []
location_data = []
theatre_data = []

files = [
    ("netflix_titles.csv", "Netflix"),
    ("amazon_prime_titles.csv", "Amazon"),
    ("disney_plus_titles.csv", "Disney"),
    ("movie_metadata.csv", "Movies"),
    ("film_locations.csv", "location"),
    ("theatre_list.csv", "theatre")
]

for file, dtype in files:

    print(f"\nProcessing {file}")

    local_file = file

    # Download from S3
    s3.download_file(bucket, f"bronze1/{file}", local_file)

    # Read CSV with Spark
    df = spark.read.option("header", True).option("inferSchema", True).csv(local_file)

    # -------- PROCESS --------
    if dtype in ["Netflix", "Amazon", "Disney"]:
        df = clean_ott(df, dtype)
        ott_data.append(df)

    elif dtype == "Movies":
        df = clean_movies(df)
        ott_data.append(df)

    elif dtype == "location":
        df = clean_location(df)
        location_data.append(df)

    elif dtype == "theatre":
        df = clean_theatre(df)
        theatre_data.append(df)


# ---------------- MERGE ----------------

from functools import reduce

ott_df = reduce(lambda a, b: a.unionByName(b), ott_data)
location_df = reduce(lambda a, b: a.unionByName(b), location_data)
theatre_df = reduce(lambda a, b: a.unionByName(b), theatre_data)


# ---------------- SAVE ----------------

ott_df.write.mode("overwrite").parquet("ott.parquet")
location_df.write.mode("overwrite").parquet("location.parquet")
theatre_df.write.mode("overwrite").parquet("theatre.parquet")

# Upload to S3
s3.upload_file("ott.parquet", bucket, "silver1/ott/ott.parquet")
s3.upload_file("location.parquet", bucket, "silver1/location/location.parquet")
s3.upload_file("theatre.parquet", bucket, "silver1/theatre/theatre.parquet")

print("\n✅ Silver Layer Completed with PySpark 🚀")