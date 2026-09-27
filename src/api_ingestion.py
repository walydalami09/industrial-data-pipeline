import requests
import pandas as pd

# # Coordonnées géographiques de Toulouse
# latitude = 43.6047
# longitude = 1.4442

# # Adresse de l'API météo
# url = "https://api.open-meteo.com/v1/forecast"

# # Paramètres transmis à l'API
# params = {
#     "latitude": latitude,
#     "longitude": longitude,
#     "current": "temperature_2m"
# }

# # Envoyer une requête HTTP GET
# response = requests.get(url, params=params, timeout=10)

# # Déclencher une erreur si la requête a échoué
# response.raise_for_status()

# # Convertir la réponse JSON en dictionnaire Python
# data = response.json()

# # Afficher les données reçues
# print(data)

# temperature = data["current"]["temperature_2m"]

# print(f"Température extérieure à Toulouse : {temperature} °C")


def get_weather(city, latitude, longitude):
    """Récupérer la température extérieure actuelle d'une ville."""

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m",
        "timezone": "Europe/Paris"
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()

    return {
        "city": city,
        "measurement_time": data["current"]["time"],
        "outside_temperature": data["current"]["temperature_2m"]
    }


# Premier test
weather_reports = []

weather_reports.append(get_weather("Toulouse", 43.6047, 1.4442))
weather_reports.append(get_weather("Bordeaux", 44.8378, -0.5792))
weather_reports.append(get_weather("Paris", 48.8566, 2.3522))


print(weather_reports)

# Convertir la liste de dictionnaires en DataFrame pandas.
df_weather = pd.DataFrame(weather_reports)

print("\nDonnées météo :")
print(df_weather)

# Exporter les données au format Parquet.
df_weather.to_parquet(
    "data/weather.parquet",
    engine="pyarrow",
    index=False
)

print("\nExport météo Parquet terminé.")