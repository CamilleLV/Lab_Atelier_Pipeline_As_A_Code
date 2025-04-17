from pyspark.sql import SparkSession
import pymysql
import sqlalchemy
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Schéma explicite du JSON
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# Charger les variables d'environnement depuis le fichier .env
load_dotenv()


# Paramètres MinIO et MySQL
minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
bucket_path = "s3a://weather-data/"


print("Test de la session Spark...")

os.environ["PYSPARK_PYTHON"] = 'python'
print(os.environ.get("HADOOP_HOME"))

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

# Création de la session Spark
spark = SparkSession.builder \
    .appName("WeatherDataStreaming") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

# Test pour vérifier si Spark démarre correctement
def test_spark_session():
    try:
        # Créer un DataFrame Spark avec des données fictives
        test_data = [("Test", 1), ("Spark", 2), ("Session", 3)]
        test_df = spark.createDataFrame(test_data, ["Name", "Value"])
        
        # Afficher le contenu du DataFrame
        print("Test Spark - Contenu du DataFrame :")
        test_df.show()
        
        print("La session Spark a démarré correctement.")
    except Exception as e:
        print(f"Erreur lors du démarrage de Spark : {e}")

# Appeler le test
test_spark_session()


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

# Vérification/Création de la table dans MySQL
def ensure_mysql_table():
    conn_str = "mssql+pyodbc://@victus_camille/LAB_ATL_PIPELINE?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(conn_str)
    try:
        conn = engine.connect()
        result = conn.execute(text(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = :schema AND table_name = :table
        """), {"schema": "LAB_ATL_PIPELINE", "table": "weather_data"})
        exists = result.scalar()
        if not exists:
            print("Création de la table MySQL...")
            conn.execute(text(f"""
                CREATE TABLE weather_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    description_temps VARCHAR(100),
                    temperature DOUBLE,
                    temperature_ressentie_c DOUBLE,
                    humidite_wm DOUBLE,
                    pression_atmospherique_hpa DOUBLE,
                    niveau_mer_hpa DOUBLE,
                    pression_sol_hpa DOUBLE,
                    vitesse_vent_m_per_sec DOUBLE,
                    direction_vent_deg DOUBLE,
                    rafales_vent_m_per_sec DOUBLE,
                    precipitations_1h_mm_per_h DOUBLE,
                    couverture_nuageuse_perc DOUBLE,
                    heure_lever_soleil TIME,
                    heure_coucher_soleil TIME,
                    date_insertion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
        else:
            print("La table MySQL existe déjà.")
    except Exception as e:
        print(f"Erreur lors de la connexion à MySQL : {e}")
ensure_mysql_table()

# Lecture en streaming depuis MinIO
df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .load(bucket_path)

# Fonction d'écriture batch -> MySQL
def write_to_mysql(batch_df, batch_id):
    # Conversion heures en type TIME si besoin (optionnel)
    from pyspark.sql.functions import to_timestamp, date_format
    batch_df = batch_df.withColumn("heure_lever_soleil", date_format(to_timestamp("heure_lever_soleil", "HH:mm:ss"), "HH:mm:ss"))
    batch_df = batch_df.withColumn("heure_coucher_soleil", date_format(to_timestamp("heure_coucher_soleil", "HH:mm:ss"), "HH:mm:ss"))

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:sqlserver://victus_camille;databaseName=LAB_ATL_PIPELINE;encrypt=true;trustServerCertificate=true;integratedSecurity=true;") \
        .option("dbtable", "weather_data") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("append") \
        .save()

# Lancement du stream
df.writeStream \
    .foreachBatch(write_to_mysql) \
    .option("checkpointLocation", "/tmp/spark-weather-checkpoint") \
    .start() \
    .awaitTermination()
