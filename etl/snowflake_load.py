import snowflake.connector

conn = snowflake.connector.connect(
    user="YOUR USERNAME",
    password="PASSWORD",
    account="ACCOUNT IDENTIFIER",
    warehouse="COMPUTE_WH",
    database="MOVIE_DB",
    schema="MOVIE_SCHEMA"
)

cur = conn.cursor()

# ---------------- LOAD DIM TABLES ----------------

cur.execute("""
COPY INTO dim_movies
FROM @movie_stage/dim_movies/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
""")

cur.execute("""
COPY INTO dim_platform
FROM @movie_stage/dim_platform/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
""")

cur.execute("""
COPY INTO dim_location
FROM @movie_stage/dim_location/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
""")

cur.execute("""
COPY INTO dim_theatre
FROM @movie_stage/dim_theatre/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
""")

# ---------------- LOAD FACT TABLES ----------------

cur.execute("""
COPY INTO fact_streaming
FROM @movie_stage/fact_streaming/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
""")

cur.execute("""
COPY INTO fact_theatre
FROM @movie_stage/fact_theatre/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
""")

print("✅ Snowflake Load Completed")

cur.close()
conn.close()