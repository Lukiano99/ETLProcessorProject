import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from src.config import DATABASE_URL

logger = logging.getLogger(__name__)


def get_engine():
    return create_engine(DATABASE_URL)


def create_tables(engine):
    sql_path = Path("sql/create_tables.sql")
    sql = sql_path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))

    logger.info("Database tables created")


def load_raw_weather(engine, df: pd.DataFrame) -> int:
    raw_cols = [
        "city", "country", "latitude", "longitude", "elevation",
        "time", "temperature_celsius", "humidity_percent",
        "precipitation_mm", "wind_speed_kmh", "pressure_hpa", "cloud_cover_percent",
    ]
    raw_df = df[raw_cols].copy()
    raw_df = raw_df.rename(columns={
        "temperature_celsius": "temperature_2m",
        "humidity_percent": "relative_humidity_2m",
        "precipitation_mm": "precipitation",
        "wind_speed_kmh": "wind_speed_10m",
        "pressure_hpa": "pressure_msl",
        "cloud_cover_percent": "cloud_cover",
    })

    rows = raw_df.to_sql(
        "raw_weather",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    logger.info("Loaded %d rows into raw_weather", rows or len(raw_df))
    return rows or len(raw_df)


def load_weather_hourly(engine, df: pd.DataFrame) -> int:
    hourly_cols = [
        "city", "country", "latitude", "longitude", "elevation",
        "time", "date", "hour", "day_of_week", "month", "year", "is_daytime",
        "temperature_celsius", "temperature_fahrenheit",
        "humidity_percent", "precipitation_mm", "precipitation_category",
        "wind_speed_kmh", "wind_category",
        "pressure_hpa", "cloud_cover_percent",
    ]
    hourly_df = df[hourly_cols].copy()
    hourly_df["wind_category"] = hourly_df["wind_category"].astype(str)
    hourly_df["precipitation_category"] = hourly_df["precipitation_category"].astype(str)

    rows = hourly_df.to_sql(
        "weather_hourly",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    logger.info("Loaded %d rows into weather_hourly", rows or len(hourly_df))
    return rows or len(hourly_df)


def compute_daily_summary(engine) -> int:
    query = text("""
        INSERT INTO daily_weather_summary (
            city, country, date,
            avg_temperature, min_temperature, max_temperature,
            avg_humidity, total_precipitation,
            avg_wind_speed, max_wind_speed,
            avg_pressure, avg_cloud_cover,
            daytime_avg_temp, nighttime_avg_temp
        )
        SELECT
            city, country, date,
            AVG(temperature_celsius),
            MIN(temperature_celsius),
            MAX(temperature_celsius),
            AVG(humidity_percent),
            SUM(precipitation_mm),
            AVG(wind_speed_kmh),
            MAX(wind_speed_kmh),
            AVG(pressure_hpa),
            AVG(cloud_cover_percent),
            AVG(CASE WHEN is_daytime THEN temperature_celsius END),
            AVG(CASE WHEN NOT is_daytime THEN temperature_celsius END)
        FROM weather_hourly
        GROUP BY city, country, date
        ON CONFLICT (city, date) DO UPDATE SET
            avg_temperature = EXCLUDED.avg_temperature,
            min_temperature = EXCLUDED.min_temperature,
            max_temperature = EXCLUDED.max_temperature,
            avg_humidity = EXCLUDED.avg_humidity,
            total_precipitation = EXCLUDED.total_precipitation,
            avg_wind_speed = EXCLUDED.avg_wind_speed,
            max_wind_speed = EXCLUDED.max_wind_speed,
            avg_pressure = EXCLUDED.avg_pressure,
            avg_cloud_cover = EXCLUDED.avg_cloud_cover,
            daytime_avg_temp = EXCLUDED.daytime_avg_temp,
            nighttime_avg_temp = EXCLUDED.nighttime_avg_temp,
            computed_at = CURRENT_TIMESTAMP
    """)

    with engine.begin() as conn:
        result = conn.execute(query)
        rows = result.rowcount

    logger.info("Computed %d daily summary rows", rows)
    return rows


def load_all(df: pd.DataFrame):
    engine = get_engine()
    create_tables(engine)
    load_raw_weather(engine, df)
    load_weather_hourly(engine, df)
    compute_daily_summary(engine)
    logger.info("Load complete")
