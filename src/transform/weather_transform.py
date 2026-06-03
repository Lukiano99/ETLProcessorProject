import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def load_raw_json(file_path: Path) -> dict:
    return json.loads(file_path.read_text(encoding="utf-8"))


def flatten_hourly_data(raw: dict) -> pd.DataFrame:
    hourly = raw["hourly"]
    df = pd.DataFrame(hourly)

    df["city"] = raw["city"]
    df["country"] = raw["country"]
    df["latitude"] = raw["latitude"]
    df["longitude"] = raw["longitude"]
    df["elevation"] = raw.get("elevation")

    return df


def clean_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["time"] = pd.to_datetime(df["time"])
    df = df.rename(columns={
        "temperature_2m": "temperature_celsius",
        "relative_humidity_2m": "humidity_percent",
        "precipitation": "precipitation_mm",
        "wind_speed_10m": "wind_speed_kmh",
        "pressure_msl": "pressure_hpa",
        "cloud_cover": "cloud_cover_percent",
    })

    df["date"] = df["time"].dt.date
    df["hour"] = df["time"].dt.hour
    df["day_of_week"] = df["time"].dt.day_name()
    df["month"] = df["time"].dt.month
    df["year"] = df["time"].dt.year
    df["is_daytime"] = df["hour"].between(6, 21)
    df["temperature_fahrenheit"] = (df["temperature_celsius"] * 9 / 5) + 32

    df["wind_category"] = pd.cut(
        df["wind_speed_kmh"],
        bins=[0, 5, 20, 40, 60, float("inf")],
        labels=["Calm", "Light", "Moderate", "Strong", "Storm"],
    )

    df["precipitation_category"] = pd.cut(
        df["precipitation_mm"],
        bins=[-0.1, 0, 2.5, 7.5, 50, float("inf")],
        labels=["None", "Light", "Moderate", "Heavy", "Extreme"],
    )

    return df


def transform_file(file_path: Path) -> pd.DataFrame:
    raw = load_raw_json(file_path)
    df = flatten_hourly_data(raw)
    df = clean_and_enrich(df)

    logger.info(
        "Transformed %d records for %s (%d nulls dropped)",
        len(df),
        raw["city"],
        df.isna().sum().sum(),
    )
    return df


def transform_all(
    input_dir: Path,
    output_dir: Optional[Path] = None,
) -> pd.DataFrame:
    output_dir = output_dir or Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)

    json_files = list(input_dir.glob("*.json"))
    if not json_files:
        logger.warning("No JSON files found in %s", input_dir)
        return pd.DataFrame()

    frames = []
    for f in json_files:
        try:
            frames.append(transform_file(f))
        except (json.JSONDecodeError, KeyError) as e:
            logger.error("Failed to transform %s: %s", f.name, e)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    output_path = output_dir / f"weather_transformed_{datetime.now().strftime('%Y%m%d')}.csv"
    combined.to_csv(output_path, index=False)
    logger.info("Saved %d transformed records to %s", len(combined), output_path)

    return combined


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    transform_all(Path("data/raw"))
