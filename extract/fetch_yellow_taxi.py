import requests
from minio import Minio
from minio.error import S3Error

# === Paramètres ===
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
local_file = "yellow_tripdata_2023-01.parquet"
bucket_name = "yellow-taxi"
object_name = "yellow_tripdata_2023-01.parquet"

# === MinIO client ===
minio_client = Minio(
    endpoint="localhost:9000",  # ou ton IP:port si c'est pas en local
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False  # True si tu utilises HTTPS
)

# === Étape 1 : Télécharger le fichier ===
def download_file():
    print("Téléchargement du fichier...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(local_file, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print("Fichier téléchargé.")

# === Étape 2 : Créer le bucket si besoin ===
def ensure_bucket():
    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' créé.")
    else:
        print(f"Bucket '{bucket_name}' déjà existant.")

# === Étape 3 : Envoyer le fichier dans MinIO ===
def upload_to_minio():
    print("Upload vers MinIO...")
    minio_client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=local_file,
    )
    print(f"Fichier uploadé sous '{bucket_name}/{object_name}'.")

# === Exécution ===
if __name__ == "__main__":
    try:
        download_file()
        ensure_bucket()
        upload_to_minio()
    except S3Error as e:
        print("Erreur MinIO :", e)
    except Exception as e:
        print("Erreur générale :", e)
