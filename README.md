🎬 Movie Data Engineering Pipeline (Airflow + Snowflake + Streamlit)

📌 Project Overview

This project is an end-to-end Data Engineering Pipeline that processes movie datasets using a modern architecture. It implements ETL pipelines, data warehousing, and interactive dashboards.

The pipeline follows a Medallion Architecture (Bronze → Silver → Gold) and uses Apache Airflow for orchestration, Snowflake for storage, and Streamlit for visualization.

---

🏗️ Architecture

- Bronze Layer → Raw data ingestion (CSV files)
- Silver Layer → Cleaned and transformed data
- Gold Layer → Aggregated and analytics-ready data
- Airflow DAG → Automates ETL pipeline
- Snowflake → Data warehouse
- Streamlit → Dashboard for insights

---

⚙️ Tech Stack

- Python
- Apache Airflow
- Snowflake
- Streamlit
- Pandas
- Matplotlib / Plotly
- Docker (optional)

---

📂 Project Structure

project/
│── airflow/
│   ├── dags/
│   │   └── movie_pipeline_dag.py
│   ├── logs/
│
│── dashboard/
│   └── app.py
│
│── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│
│── etl/
│   ├── bronze_reader.py
│   ├── silver_transform.py
│   ├── gold_transform.py
│   ├── snowflake_load.py
│
│── requirements.txt
│── docker-compose.yaml
│── README.md

---

🔄 ETL Pipeline Flow

1. Bronze Layer

- Reads raw CSV files (Netflix, Amazon Prime, Disney+)
- Stores raw data without transformation

2. Silver Layer

- Removes null values and duplicates
- Cleans and standardizes columns

3. Gold Layer

- Performs aggregations (genre, ratings, release year)
- Prepares analytics-ready datasets

4. Snowflake Loading

- Loads transformed data into Snowflake tables
- Uses warehouse: "COMPUTE_WH"

5. Airflow Automation

- DAG schedules and runs pipeline automatically
- Handles task dependencies

---

📊 Dashboard (Streamlit)

- Interactive visualizations
- Movie trends and analytics
- Genre-based insights
- Platform comparison (Netflix vs Prime vs Disney+)

Run dashboard:

streamlit run dashboard/app.py

---

🚀 How to Run the Project

1. Clone Repository

git clone https://github.com/your-username/movie-data-pipeline.git
cd movie-data-pipeline

2. Create Virtual Environment

python -m venv venv
venv\Scripts\activate   # Windows

3. Install Dependencies

pip install -r requirements.txt

4. Run Airflow

airflow standalone

5. Trigger DAG

- Open Airflow UI
- Enable "movie_pipeline_dag"
- Run the pipeline

6. Run Dashboard

streamlit run dashboard/app.py

---

❗ Important Notes

- Credentials are masked for security
- Data files (CSV/Parquet) are ignored using ".gitignore"
- Replace credentials before running locally

---

📈 Key Features

- End-to-end ETL pipeline
- Automated workflows using Airflow
- Scalable cloud data warehouse (Snowflake)
- Interactive dashboard with Streamlit
- Clean Medallion Architecture

---

🎯 Use Cases

- Data Engineering Projects
- ETL Pipeline Demonstration
- Dashboard Development
- Interview / Portfolio Project

---

👨‍💻 Author

Pavan Kumar

---

⭐ If you like this project

Give it a star ⭐ on GitHub
