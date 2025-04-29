from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import to_timestamp, date_format
from sqlalchemy import create_engine, text
import os
import tempfile
from dotenv import load_dotenv, find_dotenv
import pyodbc
from minio import Minio

# Database connection
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DESKTOP-Q5MD7J7;"
    "DATABASE=LAB_ATL_PIPELINE;"
    "Trusted_Connection=yes;"
)
conn = pyodbc.connect(conn_str)
engine = create_engine("mssql+pyodbc:///?odbc_connect=" + conn_str)
cursor = conn.cursor()

# === 1. Charger les variables d'environnement ===
load_dotenv(find_dotenv())

minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
bucket_path = "s3a://weather-data/"

# === 2. Configuration Hadoop pour Spark sur Windows ===
os.environ["PYSPARK_PYTHON"] = 'python'

# === 3. Création de la session Spark ===
spark = SparkSession.builder \
    .appName("WeatherDataStreaming") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

# === 4. Schéma JSON météo ===
schema = StructType([
    StructField("description_temps", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("temperature_ressentie_c", DoubleType(), True),
    StructField("humidite_wm", DoubleType(), True),
    StructField("pression_atmospherique_hpa", DoubleType(), True),
    StructField("niveau_mer_hpa", DoubleType(), True),
    StructField("pression_sol_hpa", DoubleType(), True),
    StructField("vitesse_vent_m_per_sec", DoubleType(), True),
    StructField("direction_vent_deg", DoubleType(), True),
    StructField("rafales_vent_m_per_sec", DoubleType(), True),
    StructField("precipitations_1h_mm_per_h", DoubleType(), True),
    StructField("couverture_nuageuse_perc", DoubleType(), True),
    StructField("heure_lever_soleil", StringType(), True),
    StructField("heure_coucher_soleil", StringType(), True)
])

# === 5. Vérification/création de la table dans SQL Server ===
def ensure_sqlserver_table():
    conn_str = "mssql+pyodbc://@localhost/LAB_ATL_PIPELINE?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(conn_str)
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'dbo' AND table_name = 'weather_data'
            """))
            exists = result.scalar()
            if not exists:
                print("Création de la table weather_data dans SQL Server...")
                conn.execute(text("""
                    CREATE TABLE dbo.weather_data (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        description_temps VARCHAR(100),
                        temperature FLOAT,
                        temperature_ressentie_c FLOAT,
                        humidite_wm FLOAT,
                        pression_atmospherique_hpa FLOAT,
                        niveau_mer_hpa FLOAT,
                        pression_sol_hpa FLOAT,
                        vitesse_vent_m_per_sec FLOAT,
                        direction_vent_deg FLOAT,
                        rafales_vent_m_per_sec FLOAT,
                        precipitations_1h_mm_per_h FLOAT,
                        couverture_nuageuse_perc FLOAT,
                        heure_lever_soleil TIME,
                        heure_coucher_soleil TIME,
                        date_insertion DATETIME DEFAULT GETDATE()
                    )
                """))
                print("✔️ Table créée.")
            else:
                print("✔️ La table weather_data existe déjà.")
    except Exception as e:
        print(f"❌ Erreur lors de la connexion à SQL Server : {e}")

ensure_sqlserver_table()

# === 6. Lecture en streaming depuis MinIO ===
df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .load(bucket_path)

# === 7. Fonction d'écriture batch vers SQL Server ===
def write_to_sqlserver(batch_df, batch_id):
    try:
        batch_df = batch_df.withColumn(
            "heure_lever_soleil", date_format(to_timestamp("heure_lever_soleil", "HH:mm:ss"), "HH:mm:ss")
        ).withColumn(
            "heure_coucher_soleil", date_format(to_timestamp("heure_coucher_soleil", "HH:mm:ss"), "HH:mm:ss")
        )

        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:sqlserver://localhost;databaseName=LAB_ATL_PIPELINE;encrypt=true;trustServerCertificate=true;integratedSecurity=true;") \
            .option("dbtable", "dbo.weather_data") \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .mode("append") \
            .save()

        print(f"[Batch {batch_id}] ✅ Données insérées dans SQL Server.")
    except Exception as e:
        print(f"[Batch {batch_id}] ❌ Erreur d'insertion : {e}")

# === 8. Lancement du streaming ===
df.writeStream \
    .foreachBatch(write_to_sqlserver) \
    .option("checkpointLocation", tempfile.gettempdir() + "/spark-weather-checkpoint") \
    .start() \
    .awaitTermination()
