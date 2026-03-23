# 🎬 Movie Analytics & Media Content Analysis Platform  

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![PySpark](https://img.shields.io/badge/PySpark-Processing-orange)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-red)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-blue)
![Architecture](https://img.shields.io/badge/Data%20Model-Star%20Schema-green)
![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)

---

## 📌 Project Overview  

This project is a complete end-to-end Data Engineering platform designed to analyze movie data (Box Office + OTT) and media content (YouTube analytics).

It integrates multiple datasets, processes them using PySpark ETL pipelines, and builds a Star Schema Data Warehouse for efficient analytics. Apache Airflow is used for orchestration, Snowflake for storage, and Streamlit for visualization.

---

## 🎯 Objectives  

- Build a scalable end-to-end ETL pipeline  
- Process multi-source datasets  
- Implement Medallion Architecture  
- Design a Star Schema Data Model  
- Generate business insights  
- Develop interactive dashboards  

---

## 🏗️ Architecture Overview  

### 🔷 Medallion Architecture  

- **Bronze Layer** → Raw data ingestion (CSV files into AWS S3)  
- **Silver Layer** → Data cleaning, transformation, standardization using PySpark  
- **Gold Layer** → Aggregated analytics-ready datasets  

All layers are stored in S3 as Parquet and loaded into Snowflake.

---

### 🔄 Data Flow  



---

📌 Project Overview

This project is a complete end-to-end Data Engineering Platform designed to analyze movie data (Box Office + OTT) and media content analytics.

The system integrates multiple datasets, processes them using PySpark ETL pipelines, and builds a Star Schema Data Warehouse for efficient analytics. Apache Airflow is used for orchestration, Snowflake for storage, and Streamlit for visualization.

---

🎯 Objectives

- Build a scalable end-to-end ETL pipeline
- Process multi-source datasets
- Implement Medallion Architecture
- Design Star Schema Data Model
- Generate business insights
- Develop interactive dashboards

---

🏗️ Architecture Overview

🔷 Medallion Architecture

- Bronze Layer → Raw data ingestion (CSV files into AWS S3)
- Silver Layer → Data cleaning, transformation, standardization using PySpark
- Gold Layer → Aggregated analytics-ready datasets

All layers are stored in S3 as Parquet and loaded into Snowflake.

---

🔄 Data Flow

Raw Data → Bronze → Silver → Gold → Snowflake → Streamlit Dashboard → Insights

---

⚙️ Tech Stack

- Python
- PySpark
- Apache Airflow
- AWS S3
- Snowflake
- Streamlit
- Pandas
- SQL

---

📂 Project Structure

project/
│── airflow/dags/movie_pipeline.py
│── etl/
│   ├── bronze_reader.py
│   ├── silver_pyspark.py
│   ├── gold_pyspark.py
│   └── snowflake_load.py
│── data/bronze/
│── data/silver/
│── data/gold/
│── dashboard/app.py
│── run_pipeline.py
│── requirements.txt

---

🔄 ETL Pipeline (Airflow DAG)

1. Bronze Layer

- Reads CSV files from S3
- Uses boto3 copy
- No transformation

2. Silver Layer (PySpark)

- Column standardization
- Null handling
- Data cleaning

3. Gold Layer

- Star schema creation
- SCD Type 2 implementation
- Aggregations

4. Snowflake Load

- Uses COPY INTO
- Loads Parquet data into warehouse

Pipeline Flow:
Bronze → Silver → Gold → Snowflake

---

⭐ Star Schema (Movie Analytics)

Dimension Tables

dim_movies

- movie_id (Primary Key)
- title
- genre
- rating

dim_platform

- platform_id (Primary Key)
- platform_name

dim_location

- location_id (Primary Key)
- city

dim_theatre

- theatre_id (Primary Key)
- theatre_name
- city
- start_date
- is_current

---

Fact Tables

fact_streaming

- fact_id (Primary Key)
- movie_id (Foreign Key)
- platform_id (Foreign Key)
- release_year

fact_theatre

- theatre_id (Foreign Key)
- city
- avg_ticket_price
- total_seats

Grain: One row per movie/platform or theatre record

---

⭐ Star Schema (Media / YouTube Analytics)

Dimension Tables

dim_channel

- channel_id (Primary Key)
- channel_title

dim_category

- category_id (Primary Key)
- category_name

---

Fact Table

fact_youtube

- video_id (Primary Key)
- channel_id (Foreign Key)
- category_id (Foreign Key)
- published_date
- view_count
- like_count
- comment_count
- engagement_score

Grain: One row per video

---

⭐ Star Schema Model

dim_channel → fact_youtube ← dim_category

✔ Central fact table
✔ Surrounding dimension tables
✔ Optimized for analytics queries

---

📊 Data Marts

- mart_top_videos → Top videos by views
- mart_channel_performance → Channel performance metrics
- mart_movie_performance → Movie analytics insights

---

❄️ Snowflake Data Warehouse

Configuration:

- Database → MOVIE_DB
- Schema → MOVIE_SCHEMA
- Warehouse → COMPUTE_WH
- File Format → PARQUET

Tables Loaded:

- dim_movies
- dim_platform
- dim_location
- dim_theatre
- fact_streaming
- fact_theatre

Load Method:
COPY INTO tables FROM S3 stage

---

📊 Dashboard (Streamlit)

Features:

- Movies trend analysis
- Genre distribution
- Ratings analysis
- OTT platform comparison
- Theatre insights
- KPI metrics (movies, platforms, cities)

---

🔐 Security & Best Practices

- No hardcoded credentials
- .env configuration used
- .gitignore for sensitive files
- Modular ETL design
- Scalable architecture
- Star schema optimization

---

🚀 How to Run

1. Install Dependencies
   pip install -r requirements.txt

2. Configure Environment
   Add Snowflake credentials
   Add AWS credentials

3. Run Airflow
   airflow standalone

4. Run ETL Pipeline
   python run_pipeline.py

5. Run Dashboard
   streamlit run dashboard/app.py

---

🎯 Key Highlights

- End-to-end ETL pipeline
- Medallion Architecture implementation
- Star Schema Data Warehouse
- PySpark-based transformations
- Airflow automation
- Snowflake integration
- Interactive dashboard

---

📌 Future Enhancements

- Real-time streaming (Kafka)
- ML recommendation system
- Docker deployment
- CI/CD pipeline

---

👤 Author

Pavan Kumar Bhupelli

---

⭐ Support

If you like this project:

- Star the repository
- Fork and contribute
- Share with others
