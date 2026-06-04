import json
from datetime import date
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.extract.weather_api import (
    fetch_city_weather,
    save_raw_data,
    extract_all,
    ExtractionConfig,
    CITIES,
    HOURLY_VARIABLES,
)


def _mock_api_response(city):
    """Create a fake API response matching Open-Meteo's format."""
    return {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "elevation": 100.0,
        "hourly": {
            "time": ["2026-06-01T00:00", "2026-06-01T01:00"],
            **{var: [10.0, 11.0] for var in HOURLY_VARIABLES},
        },
    }


class TestFetchCityWeather:
    @patch("src.extract.weather_api.requests.get")
    def test_returns_data_with_city_info(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = _mock_api_response(CITIES[0])
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = fetch_city_weather(CITIES[0], date(2026, 6, 1), date(2026, 6, 1))

        assert result["city"] == "Belgrade"
        assert result["country"] == "Serbia"
        assert "hourly" in result
        assert len(result["hourly"]["time"]) == 2

    @patch("src.extract.weather_api.requests.get")
    def test_includes_all_hourly_variables(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = _mock_api_response(CITIES[0])
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = fetch_city_weather(CITIES[0], date(2026, 6, 1), date(2026, 6, 1))

        for var in HOURLY_VARIABLES:
            assert var in result["hourly"]


class TestSaveRawData:
    def test_saves_json_file(self, tmp_path):
        data = {"city": "Belgrade", "hourly": {"time": []}}
        path = save_raw_data(data, tmp_path, "Belgrade")

        assert path.exists()
        assert path.suffix == ".json"

        saved = json.loads(path.read_text())
        assert saved["city"] == "Belgrade"

    def test_creates_output_directory(self, tmp_path):
        output_dir = tmp_path / "nested" / "dir"
        data = {"city": "Zagreb", "hourly": {"time": []}}
        path = save_raw_data(data, output_dir, "Zagreb")

        assert output_dir.exists()
        assert path.exists()


class TestExtractAll:
    @patch("src.extract.weather_api.fetch_city_weather")
    @patch("src.extract.weather_api.save_raw_data")
    def test_extracts_all_cities(self, mock_save, mock_fetch):
        mock_fetch.return_value = {"city": "Test", "hourly": {"time": []}}
        mock_save.return_value = Path("/tmp/test.json")

        config = ExtractionConfig(
            start_date=date(2026, 6, 1),
            end_date=date(2026, 6, 1),
        )
        result = extract_all(config)

        assert len(result) == len(CITIES)
        assert mock_fetch.call_count == len(CITIES)

    @patch("src.extract.weather_api.fetch_city_weather")
    @patch("src.extract.weather_api.save_raw_data")
    def test_continues_on_failure(self, mock_save, mock_fetch):
        """If one city fails, the rest should still be processed."""
        import requests

        mock_fetch.side_effect = [
            requests.RequestException("API error"),
            {"city": "Zagreb", "hourly": {"time": []}},
        ] + [{"city": c["name"], "hourly": {"time": []}} for c in CITIES[2:]]
        mock_save.return_value = Path("/tmp/test.json")

        config = ExtractionConfig(
            start_date=date(2026, 6, 1),
            end_date=date(2026, 6, 1),
        )
        result = extract_all(config)

        assert len(result) == len(CITIES) - 1
