import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from datetime import datetime, timezone
import pytz
import boto3

print("Début du script de récupération des données météo...")
# Charger les variables d'environnement
load_dotenv()

WM_API_KEY = os.getenv("WM_API_KEY")


# Donnéees NYC
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
        print(f"Erreur API WeatherMap pour : {response.status_code}")
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
        "niveau_mer_hpa": data["main"]["sea_level"],
        "pression_sol_hpa": data["main"]["grnd_level"],
        "vitesse_vent_m_per_sec": data["wind"]["speed"],
        "direction_vent_deg": data["wind"]["deg"],
        "rafales_vent_m_per_sec": data["wind"].get("gust", 0),
        "precipitations_1h_mm_per_h": data.get("rain", {}).get("1h", 0),
        "couverture_nuageuse_perc": data["clouds"]["all"],
        "heure_lever_soleil": sunrise,
        "heure_coucher_soleil": sunset
    }])
    return df


def upload_to_minio(bucket_name, file_name, data):
    print(f"Upload vers MinIO : {file_name} dans le bucket {bucket_name}")
    # Configuration de MinIO
    minio_client = boto3.client(
        's3',
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )

    try:
        # Vérifier si le bucket existe
        existing_buckets = minio_client.list_buckets()
        if not any(bucket['Name'] == bucket_name for bucket in existing_buckets['Buckets']):
            print(f"Le bucket {bucket_name} n'existe pas. Création en cours...")
            minio_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} créé avec succès.")

        # Upload du fichier JSON
        minio_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=data,
            ContentType='application/json'
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