"""Ingestion des données météorologiques des sites industriels."""

from pathlib import Path

import pandas as pd
import requests


# Configuration du projet
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "weather.parquet"

WEATHER_SITES = {
    "Toulouse": (43.6047, 1.4442),
    "Bordeaux": (44.8378, -0.5792),
    "Paris": (48.8566, 2.3522),
}


class WeatherClient:
    """Client HTTP pour récupérer les températures depuis Open-Meteo."""

    def __init__(self, timeout: int = 10) -> None:
        self.base_url = "https://api.open-meteo.com/v1/forecast"
        self.timeout = timeout

    def get_weather(
        self,
        city: str,
        latitude: float,
        longitude: float,
    ) -> dict:
        """Récupère la température extérieure actuelle d'une ville."""

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m",
            "timezone": "Europe/Paris",
        }

        response = requests.get(
            self.base_url,
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()

        current = response.json()["current"]

        return {
            "city": city,
            "measurement_time": current["time"],
            "outside_temperature": current["temperature_2m"],
        }


def collect_weather(client: WeatherClient) -> pd.DataFrame:
    """Collecte les données météorologiques des sites configurés."""

    weather_reports = []

    for city, (latitude, longitude) in WEATHER_SITES.items():
        weather = client.get_weather(city, latitude, longitude)
        weather_reports.append(weather)

    return pd.DataFrame(weather_reports)


def save_weather(df: pd.DataFrame, output_path: Path) -> None:
    """Enregistre les données météorologiques au format Parquet."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        output_path,
        engine="pyarrow",
        index=False,
    )


def main() -> None:
    """Exécute le pipeline d'ingestion météorologique."""

    client = WeatherClient()

    df_weather = collect_weather(client)

    print("\nDonnées météorologiques :")
    print(df_weather)

    save_weather(df_weather, OUTPUT_PATH)

    print(f"\nExport Parquet terminé : {OUTPUT_PATH}")


if __name__ == "__main__":
    main()