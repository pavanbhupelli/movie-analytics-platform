from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split, monotonically_increasing_id
import boto3
from datetime import datetime

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder \
    .appName("Gold Layer Transformation") \
    .getOrCreate()

# ---------------- CONFIG ----------------
bucket = "movie-analytics-platform-data"
s3 = boto3.client("s3")
today = datetime.today().strftime("%Y-%m-%d")

# ---------------- LOAD SILVER ----------------

def load(file):
    local = f"{file}.parquet"
    s3.download_file(bucket, f"silver1/{file}/{file}.parquet", local)
    return spark.read.parquet(local)

ott = load("ott")
location = load("location")
theatre = load("theatre")

print("✅ Silver data loaded")

# ---------------- SCD TYPE 2 ----------------

def apply_scd2(df, key):
    df = df.dropDuplicates([key])

    return df.withColumn("start_date", lit(today)) \
             .withColumn("end_date", lit(None)) \
             .withColumn("is_current", lit(True))

# ---------------- DIM PLATFORM ----------------

dim_platform = ott.select("platform") \
    .dropna() \
    .dropDuplicates() \
    .withColumnRenamed("platform", "platform_name") \
    .withColumn("platform_id", monotonically_increasing_id())

# ---------------- DIM MOVIES ----------------

dim_movies = ott.select(
    "title", "director", "cast",
    "listed_in", "duration", "rating"`
).withColumnRenamed("listed_in", "genre")

dim_movies = apply_scd2(dim_movies, "title") \
    .withColumn("movie_id", monotonically_increasing_id())

# ---------------- DIM LOCATION ----------------

dim_location = location.select(
    "title", "location", "director", "production_company"
)

dim_location = dim_location.withColumn(
    "city",
    split(col("location"), ",").getItem(0)
)

dim_location = apply_scd2(dim_location, "title") \
    .withColumn("location_id", monotonically_increasing_id())

# ---------------- DIM THEATRE ----------------

dim_theatre = theatre.select(
    "theatre_name", "city", "type", "theatre_chain"
)

dim_theatre = apply_scd2(dim_theatre, "theatre_name") \
    .withColumn("theatre_id", monotonically_increasing_id())

# ---------------- FACT STREAMING ----------------

fact_streaming = ott.join(
    dim_movies.select("title", "movie_id"),
    on="title",
    how="left"
)

fact_streaming = fact_streaming.join(
    dim_platform,
    fact_streaming["platform"] == dim_platform["platform_name"],
    "left"
)

fact_streaming = fact_streaming.select(
    "movie_id", "platform_id", "release_year"
).withColumn("fact_id", monotonically_increasing_id())

# ---------------- FACT THEATRE ----------------

fact_theatre = theatre.join(
    dim_theatre.select("theatre_name", "theatre_id"),
    on="theatre_name",
    how="left"
)

fact_theatre = fact_theatre.select(
    "theatre_id",
    "city",
    "average_ticket_price",
    "total_seats",
    "no_screens",
    "type"
)

# ---------------- SAVE FUNCTION ----------------

def save_to_s3(df, name):
    output_path = f"{name}.parquet"
    
    df.write.mode("overwrite").parquet(output_path)

    s3.upload_file(output_path, bucket, f"gold1/{name}/{name}.parquet")

# ---------------- SAVE ALL ----------------

save_to_s3(dim_movies, "dim_movies")
save_to_s3(dim_platform, "dim_platform")
save_to_s3(dim_location, "dim_location")
save_to_s3(dim_theatre, "dim_theatre")
save_to_s3(fact_streaming, "fact_streaming")
save_to_s3(fact_theatre, "fact_theatre")

print("\n🚀 Gold Layer Completed (PySpark + SCD2 + Star Schema)")