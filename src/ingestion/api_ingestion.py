"""Ingestion des données météorologiques des sites industriels."""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pandas as pd
import requests


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_WEATHER_PATH = (
    PROJECT_ROOT
    / "data"
    / "lake"
    / "bronze"
    / "weather.parquet"
)

# ============================================================
# DATA MODEL
# ============================================================

@dataclass(frozen=True)
class WeatherSite:
    """Représente un site industriel avec ses coordonnées."""

    city: str
    latitude: float
    longitude: float


WEATHER_SITES = [
    WeatherSite(
        city="Toulouse",
        latitude=43.6047,
        longitude=1.4442,
    ),
    WeatherSite(
        city="Bordeaux",
        latitude=44.8378,
        longitude=-0.5792,
    ),
    WeatherSite(
        city="Paris",
        latitude=48.8566,
        longitude=2.3522,
    ),
]


# ============================================================
# API CLIENT
# ============================================================

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


# ============================================================
# GENERATOR
# ============================================================

def fetch_weather_reports(
    client: WeatherClient,
) -> Iterator[dict]:
    """
    Génère les rapports météo un par un.

    L'utilisation de yield évite de construire une liste
    intermédiaire contenant tous les résultats.
    """

    for site in WEATHER_SITES:
        yield client.get_weather(
            city=site.city,
            latitude=site.latitude,
            longitude=site.longitude,
        )


# ============================================================
# COLLECTION
# ============================================================

def collect_weather(
    client: WeatherClient,
) -> pd.DataFrame:
    """Collecte les rapports météo dans un DataFrame pandas."""

    weather_reports = fetch_weather_reports(client)

    return pd.DataFrame(weather_reports)


# ============================================================
# EXPORT
# ============================================================

def save_weather(
    df: pd.DataFrame,
    output_path: Path,
) -> None:
    """Enregistre les données météorologiques au format Parquet."""

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        output_path,
        engine="pyarrow",
        index=False,
    )


# ============================================================
# PIPELINE
# ============================================================

def main() -> None:
    """Exécute le pipeline d'ingestion météorologique."""

    client = WeatherClient()

    df_weather = collect_weather(client)

    print("\nDonnées météorologiques :")
    print(df_weather)

    save_weather(
    df_weather,
    BRONZE_WEATHER_PATH,
    )

    print(
    f"\nExport Bronze terminé : {BRONZE_WEATHER_PATH}"
    )


if __name__ == "__main__":
    main()