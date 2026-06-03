import json
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

CITIES = [
    {"name": "Belgrade", "latitude": 44.8125, "longitude": 20.4612, "country": "Serbia"},
    {"name": "Zagreb", "latitude": 45.8150, "longitude": 15.9819, "country": "Croatia"},
    {"name": "Budapest", "latitude": 47.4979, "longitude": 19.0402, "country": "Hungary"},
    {"name": "Vienna", "latitude": 48.2082, "longitude": 16.3738, "country": "Austria"},
    {"name": "Ljubljana", "latitude": 46.0569, "longitude": 14.5058, "country": "Slovenia"},
    {"name": "Bucharest", "latitude": 44.4268, "longitude": 26.1025, "country": "Romania"},
    {"name": "Sofia", "latitude": 42.6977, "longitude": 23.3219, "country": "Bulgaria"},
    {"name": "Bratislava", "latitude": 48.1486, "longitude": 17.1077, "country": "Slovakia"},
    {"name": "Prague", "latitude": 50.0755, "longitude": 14.4378, "country": "Czechia"},
    {"name": "Berlin", "latitude": 52.5200, "longitude": 13.4050, "country": "Germany"},
]

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "pressure_msl",
    "cloud_cover",
]


@dataclass(frozen=True)
class ExtractionConfig:
    start_date: date
    end_date: date
    output_dir: Path = Path("data/raw")


def fetch_city_weather(
    city: dict, start_date: date, end_date: date
) -> dict:
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "Europe/Belgrade",
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    data["city"] = city["name"]
    data["country"] = city["country"]

    logger.info(
        "Fetched %d hourly records for %s",
        len(data.get("hourly", {}).get("time", [])),
        city["name"],
    )
    return data


def save_raw_data(data: dict, output_dir: Path, city_name: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{city_name.lower()}_{date.today().isoformat()}.json"
    output_path = output_dir / filename

    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    logger.info("Saved raw data to %s", output_path)
    return output_path


def extract_all(config: ExtractionConfig) -> list[Path]:
    saved_files = []

    for city in CITIES:
        try:
            data = fetch_city_weather(city, config.start_date, config.end_date)
            path = save_raw_data(data, config.output_dir, city["name"])
            saved_files.append(path)
        except requests.RequestException as e:
            logger.error("Failed to fetch data for %s: %s", city["name"], e)

    logger.info("Extraction complete: %d/%d cities", len(saved_files), len(CITIES))
    return saved_files


def extract_last_7_days(output_dir: Optional[Path] = None) -> list[Path]:
    config = ExtractionConfig(
        start_date=date.today() - timedelta(days=7),
        end_date=date.today() - timedelta(days=1),
        output_dir=output_dir or Path("data/raw"),
    )
    return extract_all(config)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_last_7_days()
