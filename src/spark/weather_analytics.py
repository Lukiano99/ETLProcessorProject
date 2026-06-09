import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "weather_etl")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASSWORD = os.getenv("DB_PASSWORD", "airflow")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

JDBC_DRIVER_PATH = os.getenv(
    "JDBC_DRIVER_PATH",
    "/opt/spark/jars/postgresql-42.7.4.jar",
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("WeatherAnalytics")
        .config("spark.jars", JDBC_DRIVER_PATH)
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


def read_weather_data(spark):
    """Read weather_hourly table from PostgreSQL via JDBC."""
    df = spark.read.jdbc(
        url=JDBC_URL,
        table="weather_hourly",
        properties=JDBC_PROPERTIES,
    )
    logger.info("Read %d rows from weather_hourly", df.count())
    return df


def compute_daily_city_stats(df):
    """Aggregate hourly data to daily averages per city."""
    return df.groupBy("city", "country", "date").agg(
        F.avg("temperature_celsius").alias("avg_temperature"),
    )


def add_rolling_average(df):
    """7-day rolling average temperature per city using a window function."""
    window = (
        Window
        .partitionBy("city")
        .orderBy("date")
        .rowsBetween(-6, 0)
    )
    return df.withColumn(
        "rolling_avg_7d",
        F.round(F.avg("avg_temperature").over(window), 2),
    )


def add_temperature_rank(df):
    """Rank cities by temperature per day (1 = hottest)."""
    window = Window.partitionBy("date").orderBy(F.desc("avg_temperature"))
    return df.withColumn(
        "temperature_rank",
        F.dense_rank().over(window),
    )


def add_anomaly_detection(df):
    """Flag days where temperature deviates > 2 std from city mean."""
    city_stats_window = Window.partitionBy("city")

    df = df.withColumn(
        "city_mean_temp",
        F.round(F.avg("avg_temperature").over(city_stats_window), 2),
    )
    df = df.withColumn(
        "city_stddev_temp",
        F.round(F.stddev("avg_temperature").over(city_stats_window), 2),
    )
    df = df.withColumn(
        "is_anomaly",
        F.abs(F.col("avg_temperature") - F.col("city_mean_temp"))
        > (2 * F.col("city_stddev_temp")),
    )
    return df


def write_results(df):
    """Write analytics results back to PostgreSQL."""
    output_df = df.select(
        "city", "country", "date", "avg_temperature",
        "rolling_avg_7d", "temperature_rank",
        "city_mean_temp", "city_stddev_temp", "is_anomaly",
    )

    output_df.write.jdbc(
        url=JDBC_URL,
        table="spark_city_analytics",
        mode="overwrite",
        properties=JDBC_PROPERTIES,
    )
    logger.info("Wrote %d rows to spark_city_analytics", output_df.count())


def run_analytics():
    """Main entry point — run all Spark analytics."""
    spark = create_spark_session()
    try:
        hourly = read_weather_data(spark)
        daily = compute_daily_city_stats(hourly)
        daily = add_rolling_average(daily)
        daily = add_temperature_rank(daily)
        daily = add_anomaly_detection(daily)
        write_results(daily)
        logger.info("Spark analytics complete")
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_analytics()
