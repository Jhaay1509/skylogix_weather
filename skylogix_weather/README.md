Skylogix 🌍

Weather Data Engineering Pipeline

Skylogix is a production-style data engineering pipeline that collects real-time weather data from the OpenWeather API, stages raw data in MongoDB, transforms and normalizes it, and loads analytics-ready datasets into PostgreSQL. The entire workflow is orchestrated using Apache Airflow.

The project focuses on practical data engineering concepts such as reliable ingestion, idempotent writes, data modeling, and workflow orchestration.

🎯 Project Goals

Ingest live weather data for selected African cities

Preserve raw data for traceability and reprocessing

Transform nested API payloads into relational formats

Load analytics-ready data into PostgreSQL

Orchestrate and monitor the pipeline using Airflow

Support downstream analytics and operational insights

🏗️ High-Level Architecture
OpenWeather API
   → Python Ingestion
   → MongoDB (Raw Data)
   → Airflow Orchestration
   → Data Transformation
   → PostgreSQL (Analytics)
   → Dashboards / Queries

🛠️ Tech Stack

Python 3.10+

OpenWeather API

MongoDB (staging layer)

PostgreSQL (analytics warehouse)

Apache Airflow

Pandas & SQLAlchemy

Docker (optional)

📂 Repository Layout
skylogix/
├── dags/                 # Airflow DAG definitions
│   ├── weather_etl.py
│   └── utils.py
├── extract_staging.py    # API → MongoDB
├── stage_transform.py    # MongoDB → cleaned dataset
├── load.py               # PostgreSQL loader
├── sql/                  # Table definitions
├── .env.example
├── requirements.txt
└── README.md

🔄 Pipeline Overview
Extract

Fetches weather data from OpenWeather API

Generates deterministic record IDs

Performs idempotent upserts into MongoDB

Transform

Reads raw documents from MongoDB

Flattens and normalizes nested structures

Prepares data for relational storage

Load

Loads transformed data into PostgreSQL

Optimized for time-series and analytical queries

🗄️ Data Storage
MongoDB (Raw Layer)

Stores immutable weather API responses

Enables replay and auditability

PostgreSQL (Analytics Layer)

Table: weather_readings

Contains city metadata, weather metrics, and timestamps

DDL is provided in:

sql/weather_readings.sql

📊 Example Analytics

Daily Temperature Trends

SELECT city, DATE(observed_at), AVG(temp_c)
FROM weather_readings
GROUP BY city, DATE(observed_at);


Extreme Weather Detection

SELECT *
FROM weather_readings
WHERE wind_speed_ms > 15 OR rain_1h_mm > 20;

⏱️ Airflow Orchestration

DAG Name: daily_weather_etl

Schedule: Daily (06:00 WAT)

Tasks

Extract weather data

Transform and normalize records

Load into PostgreSQL

Airflow handles retries, task ordering, and observability.

⚙️ Configuration

Create a .env file from .env.example:

API_KEY=your_openweather_api_key

MONGO_URI=mongodb://user:password@localhost:27017/weather_raw
POSTGRES_STRING=postgresql+psycopg2://user:password@localhost:5432/weather_dw

CITIES=Nairobi,KE,Lagos,NG,Accra,GH,Johannesburg,ZA

▶️ Running the Project
pip install -r requirements.txt
airflow db init
airflow scheduler
airflow webserver


Enable and trigger the DAG from the Airflow UI.

🔍 Design Decisions

MongoDB is used strictly as a raw staging layer

PostgreSQL is optimized for analytics workloads

Airflow manages scheduling, retries, and task isolation

Raw data is immutable once ingested

👤 Author

Julius Idowu
Data Engineering