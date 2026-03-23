import pandas as pd
import boto3

s3 = boto3.client("s3")

bucket = "movie-analytics-platform-data"

# download gold table
s3.download_file(
    bucket,
    "gold1/fact_streaming_performance/fact_streaming_performance.parquet",
    "fact_streaming.parquet"
)

df = pd.read_parquet("fact_streaming.parquet")

print(df.groupby("platform_id").size())

s3.download_file(
    bucket,
    "gold1/dim_theatre/dim_theatre.parquet",
    "theatre.parquet"
)

theatre = pd.read_parquet("theatre.parquet")

print(theatre.groupby("city").size().head())