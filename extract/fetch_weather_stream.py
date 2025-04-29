import os
import json
import tempfile
import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from datetime import datetime, timezone
from minio import Minio
from minio.error import S3Error

print("Début du script de récupération des données météo...")
# Charger les variables d'environnement
load_dotenv(find_dotenv())

WM_API_KEY = os.getenv("WM_API_KEY")

print("Clé API récupérée :", os.getenv("WM_API_KEY"))  # Vérifie que ce n’est pas None ou vide

# Données NYC
# Latitude et longitude de New York
NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060


# URL de l'API OpenWeatherMap pour New York
WM_URL_TEMPLATE = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&lang=fr&appid={api_key}&units=metric"
print("URL de l'API OpenWeatherMap : ", WM_URL_TEMPLATE)

def fetch_weather_map():
    print("Récupération des données météo depuis OpenWeatherMap...")
    url = WM_URL_TEMPLATE.format(lat=NYC_LATITUDE, lon=NYC_LONGITUDE, api_key=WM_API_KEY)
    response = requests.get(url)
    if response.status_code == 200:
        print("Réponse API WeatherMap : ", response.status_code)
        return response.json()
    else:
        print(f"Erreur API WeatherMap : {response.status_code}, {response.text}")
        return None


def transform_weather_map(data):
    print("Transformation des données météo...")
    if not data:
        return None
    
    sunrise = datetime.fromtimestamp(data["sys"]["sunrise"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    sunset = datetime.fromtimestamp(data["sys"]["sunset"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    print("Heure de lever du soleil : ", sunrise)
    print("Heure de coucher du soleil : ", sunset)

    # Création du DataFrame
    df = pd.DataFrame([{  
        "description_temps": data["weather"][0]["description"],
        "temperature": data["main"]["temp"],
        "temperature_ressentie_c": data["main"]["feels_like"],
        "humidite_wm": data["main"]["humidity"],
        "pression_atmospherique_hpa": data["main"]["pressure"],
        "niveau_mer_hpa": data.get("main", {}).get("sea_level", None),
        "pression_sol_hpa": data.get("main", {}).get("grnd_level", None),
        "vitesse_vent_m_per_sec": data["wind"]["speed"],
        "direction_vent_deg": data["wind"]["deg"],
        "rafales_vent_m_per_sec": data["wind"].get("gust", 0),
        "precipitations_1h_mm_per_h": data.get("rain", {}).get("1h", 0),
        "couverture_nuageuse_perc": data["clouds"]["all"],
        "heure_lever_soleil": sunrise,
        "heure_coucher_soleil": sunset
    }])
    return df


def upload_to_minio(bucket_name, file_name, json_data):
    print(f"Upload vers MinIO : {file_name} dans le bucket {bucket_name}")
    # Configuration de MinIO
    minio_client = Minio(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    try:
        # Vérifier si le bucket existe
        if not minio_client.bucket_exists(bucket_name):
            print(f"Le bucket {bucket_name} n'existe pas. Création en cours...")
            minio_client.make_bucket(bucket_name)
            print(f"Bucket {bucket_name} créé avec succès.")

        # Sauvegarder les données JSON dans un fichier temporaire
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, file_name)
        with open(temp_file, "w", encoding="utf-8") as f:
            f.write(json_data)

        # Upload du fichier JSON
        minio_client.fput_object(
            bucket_name=bucket_name,
            object_name=file_name,
            file_path=temp_file,
        )
        print(f"Fichier {file_name} uploadé avec succès dans le bucket {bucket_name}.")
    except Exception as e:
        print(f"Erreur lors de l'upload vers MinIO : {e}")


def main():
    weather_data = fetch_weather_map()
    print("Récupération des données météo terminée.")
    print("Données météo : ", weather_data)
    weather_df = transform_weather_map(weather_data)
    print("Transformation des données météo terminée.")

    if weather_df is not None:
        try:
            # Formater les colonnes de lever et coucher du soleil en HH:MM:SS
            weather_df["heure_lever_soleil"] = pd.to_datetime(weather_df["heure_lever_soleil"]).dt.strftime('%H:%M:%S')
            weather_df["heure_coucher_soleil"] = pd.to_datetime(weather_df["heure_coucher_soleil"]).dt.strftime('%H:%M:%S')

            # Traiter les décimales des températures
            weather_df["temperature"] = weather_df["temperature"].round(2)
            weather_df["temperature_ressentie_c"] = weather_df["temperature_ressentie_c"].round(2)

            # Convertir le DataFrame en JSON
            json_data = weather_df.to_json(orient='records', force_ascii=False, lines=True)

            # Upload vers MinIO
            bucket_name = "weather-data"
            file_name = f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            upload_to_minio(bucket_name, file_name, json_data)

        except Exception as e:
            print(f"Erreur lors du traitement des données: {e}")
    else:
        print("Aucune donnée météo disponible.")


if __name__ == "__main__":
    main()