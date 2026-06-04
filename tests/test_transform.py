import json
from pathlib import Path

import pandas as pd

from src.transform.weather_transform import (
    flatten_hourly_data,
    clean_and_enrich,
    transform_file,
    load_raw_json,
)


def _sample_raw_data():
    """Create minimal raw weather data matching API format."""
    return {
        "city": "Belgrade",
        "country": "Serbia",
        "latitude": 44.81,
        "longitude": 20.46,
        "elevation": 132.0,
        "hourly": {
            "time": [
                "2026-06-01T00:00",
                "2026-06-01T06:00",
                "2026-06-01T12:00",
                "2026-06-01T18:00",
            ],
            "temperature_2m": [15.0, 18.0, 28.0, 22.0],
            "relative_humidity_2m": [80, 65, 40, 55],
            "precipitation": [0.0, 0.0, 2.5, 0.0],
            "wind_speed_10m": [3.0, 12.0, 25.0, 8.0],
            "pressure_msl": [1013.0, 1012.0, 1010.0, 1011.0],
            "cloud_cover": [90, 50, 10, 30],
        },
    }


class TestFlattenHourlyData:
    def test_creates_dataframe_with_correct_rows(self):
        raw = _sample_raw_data()
        df = flatten_hourly_data(raw)

        assert len(df) == 4
        assert "city" in df.columns
        assert df["city"].iloc[0] == "Belgrade"

    def test_includes_metadata_columns(self):
        raw = _sample_raw_data()
        df = flatten_hourly_data(raw)

        assert df["country"].iloc[0] == "Serbia"
        assert df["latitude"].iloc[0] == 44.81
        assert df["elevation"].iloc[0] == 132.0


class TestCleanAndEnrich:
    def test_renames_columns(self):
        raw = _sample_raw_data()
        df = flatten_hourly_data(raw)
        enriched = clean_and_enrich(df)

        assert "temperature_celsius" in enriched.columns
        assert "humidity_percent" in enriched.columns
        assert "temperature_2m" not in enriched.columns

    def test_adds_date_parts(self):
        raw = _sample_raw_data()
        df = flatten_hourly_data(raw)
        enriched = clean_and_enrich(df)

        assert enriched["year"].iloc[0] == 2026
        assert enriched["month"].iloc[0] == 6
        assert enriched["hour"].iloc[0] == 0

    def test_fahrenheit_conversion(self):
        raw = _sample_raw_data()
        df = flatten_hourly_data(raw)
        enriched = clean_and_enrich(df)

        celsius = enriched["temperature_celsius"].iloc[0]
        fahrenheit = enriched["temperature_fahrenheit"].iloc[0]
        expected = (celsius * 9 / 5) + 32
        assert abs(fahrenheit - expected) < 0.01

    def test_daytime_flag(self):
        raw = _sample_raw_data()
        df = flatten_hourly_data(raw)
        enriched = clean_and_enrich(df)

        # hour 0 = nighttime, hour 12 = daytime
        assert not enriched["is_daytime"].iloc[0]
        assert enriched["is_daytime"].iloc[2]

    def test_wind_categories(self):
        raw = _sample_raw_data()
        df = flatten_hourly_data(raw)
        enriched = clean_and_enrich(df)

        categories = enriched["wind_category"].tolist()
        assert str(categories[0]) == "Calm"       # 3.0 km/h
        assert str(categories[1]) == "Light"       # 12.0 km/h
        assert str(categories[2]) == "Moderate"    # 25.0 km/h

    def test_precipitation_categories(self):
        raw = _sample_raw_data()
        df = flatten_hourly_data(raw)
        enriched = clean_and_enrich(df)

        categories = enriched["precipitation_category"].tolist()
        assert str(categories[0]) == "None"        # 0.0 mm
        assert str(categories[2]) == "Light"       # 2.5 mm


class TestTransformFile:
    def test_transforms_json_file(self, tmp_path):
        raw = _sample_raw_data()
        json_path = tmp_path / "belgrade_2026-06-01.json"
        json_path.write_text(json.dumps(raw))

        df = transform_file(json_path)

        assert len(df) == 4
        assert "temperature_celsius" in df.columns
        assert "wind_category" in df.columns
        assert "is_daytime" in df.columns
