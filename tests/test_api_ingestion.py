import pytest
import requests
from unittest.mock import Mock, patch

from src.ingestion.api_ingestion import WeatherClient


def test_get_weather() -> None:
    """Vérifie la transformation de la réponse API."""

    fake_response = Mock()

    fake_response.json.return_value = {
        "current": {
            "time": "2026-09-28T14:00",
            "temperature_2m": 21.5,
        }
    }

    with patch(
        "src.ingestion.api_ingestion.requests.get",
        return_value=fake_response,
    ):
        client = WeatherClient()

        result = client.get_weather(
            city="Paris",
            latitude=48.8566,
            longitude=2.3522,
        )

    assert result["city"] == "Paris"
    assert result["measurement_time"] == "2026-09-28T14:00"
    assert result["outside_temperature"] == 21.5

def test_get_weather_http_error() -> None:
    """Vérifie qu'une erreur HTTP est bien propagée."""

    fake_response = Mock()

    fake_response.raise_for_status.side_effect = requests.HTTPError(
        "500 Server Error"
    )

    with patch(
        "src.ingestion.api_ingestion.requests.get",
        return_value=fake_response,
    ):
        client = WeatherClient()

        with pytest.raises(requests.HTTPError):
            client.get_weather(
                city="Paris",
                latitude=48.8566,
                longitude=2.3522,
            )