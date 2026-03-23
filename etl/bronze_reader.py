

import boto3

bucket = "movie-analytics-platform-data"

files = [
    "netflix_titles.csv",
    "amazon_prime_titles.csv",
    "disney_plus_titles.csv",
    "movie_metadata.csv",
    "film_locations.csv",
    "theatre_list.csv"
]

s3 = boto3.client("s3")

for file in files:
    s3.copy_object(
        Bucket=bucket,
        CopySource=f"{bucket}/raw1/{file}",
        Key=f"bronze1/{file}"
    )

print("Bronze completed ✅")