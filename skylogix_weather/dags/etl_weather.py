from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.timezone import datetime
import os

from utils import ingest_weather_data, transform, load


def show_env():
    """Print DB connection string for quick debugging."""
    print("Postgres string:", os.getenv("POSTGRES_STRING"))


args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    "weather_etl_pipeline",
    default_args=args,
    description="Weather ETL pipeline (hourly)",
    schedule="@hourly",
    catchup=False,
    tags=["etl", "weather"],
) as dag:

    extract = PythonOperator(
        task_id="extract_weather_data",
        python_callable=ingest_weather_data,
        do_xcom_push=False,
    )

    transform_data = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform,
        do_xcom_push=True,
    )

    env_check = PythonOperator(
        task_id="debug_env",
        python_callable=show_env,
    )

    load_data = PythonOperator(
        task_id="load_weather_data",
        python_callable=load,
        op_kwargs={
            "data_path": "{{ ti.xcom_pull(task_ids='transform_weather_data') }}"
        },
    )

    extract >> transform_data >> env_check >> load_data