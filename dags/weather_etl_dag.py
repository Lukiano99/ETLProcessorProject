import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow runs inside /opt/airflow in the container — add it to path so we can import our src modules
sys.path.insert(0, "/opt/airflow")

from src.extract.weather_api import ExtractionConfig, extract_all
from src.transform.weather_transform import transform_all
from src.load.db_loader import get_engine, create_tables, load_raw_weather, load_weather_hourly, compute_daily_summary

# Default settings applied to every task in this DAG
default_args = {
    "owner": "airflow",
    "retries": 2,                          # retry failed tasks up to 2 times
    "retry_delay": timedelta(seconds=10),  # wait 10s between retries
}

# Paths inside the Docker container (mapped via docker-compose volumes)
RAW_DIR = Path("/opt/airflow/data/raw")
PROCESSED_DIR = Path("/opt/airflow/data/processed")


# -- Task functions: each one becomes a node in the DAG graph --

def task_extract(**context):
    """Pull weather data from Open-Meteo API for the last 7 days."""
    # context["ds"] is the execution date Airflow passes to each task (e.g. "2026-06-04")
    execution_date = context["ds"]
    end_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
    start_date = end_date - timedelta(days=6)

    config = ExtractionConfig(
        start_date=start_date,
        end_date=end_date,
        output_dir=RAW_DIR,
    )
    files = extract_all(config)
    return [str(f) for f in files]


def task_transform(**context):
    """Read raw JSON files, clean/enrich data, output CSV."""
    df = transform_all(RAW_DIR, PROCESSED_DIR)
    row_count = len(df)
    return row_count


def task_create_tables():
    """Create PostgreSQL tables if they don't exist yet."""
    engine = get_engine()
    create_tables(engine)


def task_load(**context):
    """Load the most recent transformed CSV into PostgreSQL."""
    import pandas as pd

    csv_files = list(PROCESSED_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("No transformed CSV files found")

    # Pick the most recently modified CSV (in case multiple exist)
    latest_csv = max(csv_files, key=lambda f: f.stat().st_mtime)
    df = pd.read_csv(latest_csv)

    engine = get_engine()
    raw_count = load_raw_weather(engine, df)
    hourly_count = load_weather_hourly(engine, df)
    return {"raw_rows": raw_count, "hourly_rows": hourly_count}


def task_daily_summary():
    """Aggregate hourly data into daily summaries (avg/min/max temp, total precip, etc.)."""
    engine = get_engine()
    count = compute_daily_summary(engine)
    return count


def task_quality_check():
    """Validate that data was actually loaded — fail the pipeline if tables are empty."""
    from sqlalchemy import text

    engine = get_engine()
    with engine.connect() as conn:
        hourly = conn.execute(text("SELECT COUNT(*) FROM weather_hourly")).scalar()
        daily = conn.execute(text("SELECT COUNT(*) FROM daily_weather_summary")).scalar()
        cities = conn.execute(text("SELECT COUNT(DISTINCT city) FROM weather_hourly")).scalar()

    if hourly == 0:
        raise ValueError("Quality check failed: weather_hourly is empty")
    if daily == 0:
        raise ValueError("Quality check failed: daily_weather_summary is empty")
    if cities == 0:
        raise ValueError("Quality check failed: no cities found")

    return {"hourly_rows": hourly, "daily_rows": daily, "cities": cities}


# -- DAG definition --

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for European weather data from Open-Meteo API",
    schedule_interval="@daily",            # runs once per day at midnight
    start_date=datetime(2026, 6, 1),       # DAG is valid from this date onward
    catchup=False,                         # don't backfill missed runs
    tags=["weather", "etl"],
) as dag:

    # Each PythonOperator wraps one of our task functions as an Airflow task
    extract = PythonOperator(task_id="extract_data", python_callable=task_extract)
    transform = PythonOperator(task_id="transform_data", python_callable=task_transform)
    create_db = PythonOperator(task_id="create_tables", python_callable=task_create_tables)
    load = PythonOperator(task_id="load_to_postgres", python_callable=task_load)
    daily = PythonOperator(task_id="compute_daily_summary", python_callable=task_daily_summary)
    quality = PythonOperator(task_id="quality_check", python_callable=task_quality_check)

    # Define execution order: each task waits for the previous one to succeed
    extract >> transform >> create_db >> load >> daily >> quality
