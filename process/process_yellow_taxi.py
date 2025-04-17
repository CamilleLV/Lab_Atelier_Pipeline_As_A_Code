from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, unix_timestamp, dayofweek, hour
from pyspark.sql.types import IntegerType, StringType

import pyodbc

# === 1. Création de la session Spark ===
spark = SparkSession.builder \
    .appName("Taxi Data Processing") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

# === 2. Lecture des données depuis MinIO en format Parquet ===
taxi_data = spark.read.parquet("s3a://yellow-taxi/*.parquet")

# === 3. Transformation des données ===

# Calcul de la durée du trajet (en secondes)
taxi_data = taxi_data.withColumn('trip_duration', (unix_timestamp('tpep_dropoff_datetime') - unix_timestamp('tpep_pickup_datetime')))

# Tranches de distance (en kilomètres)
def distance_category(distance):
    if distance <= 2:
        return '0-2 km'
    elif 2 < distance <= 5:
        return '2-5 km'
    else:
        return '>5 km'

distance_udf = udf(distance_category, StringType())

taxi_data = taxi_data.withColumn('distance_category', distance_udf(col('trip_distance')))

# Type de paiement (table de correspondance)
payment_mapping = {
    1: 'Credit Card',
    2: 'Cash',
    3: 'No Charge',
    4: 'Disputed',
}

payment_udf = udf(lambda x: payment_mapping.get(x, 'Unknown'), StringType())

taxi_data = taxi_data.withColumn('payment_type', payment_udf(col('payment_type')))

# Pourcentage de pourboire
taxi_data = taxi_data.withColumn('tip_percentage', col('tip_amount') / col('fare_amount'))

# Heure de prise en charge et jour de la semaine
taxi_data = taxi_data.withColumn('pickup_hour', hour(col('tpep_pickup_datetime')))
taxi_data = taxi_data.withColumn('pickup_dayofweek', dayofweek(col('tpep_pickup_datetime')))


taxi_data.printSchema()
taxi_data.show(5)
# === 4. Si table de zones est disponible ===
# Supposons qu'une table 'zones' est déjà chargée dans Spark
# zones_df = spark.read.format('jdbc').option('url', 'jdbc:sqlserver://<ssms_host>:<port>;databaseName=<db_name>') \
#     .option('dbtable', 'zones') \
#     .option('user', 'your_user') \
#     .option('password', 'your_password') \
#     .load()

# Jointure avec les données Taxi pour ajouter les informations de zone
# taxi_data = taxi_data.join(zones_df, taxi_data.pickup_location_id == zones_df.zone_id, 'left')

# === 5. Sauvegarde du résultat dans SSMS ===
# Connexion à SQL Server avec pyodbc
# conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
#                       'SERVER=your_server;'
#                       'DATABASE=your_db;'
#                       'UID=your_user;'
#                       'PWD=your_password')

# cursor = conn.cursor()

# Création de la table dans SSMS si elle n'existe pas encore
# cursor.execute('''
#     IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fact_taxi_trips' AND xtype='U')
#     CREATE TABLE fact_taxi_trips (
#         trip_id INT PRIMARY KEY,
#         trip_duration INT,
#         distance_category VARCHAR(50),
#         payment_type VARCHAR(50),
#         tip_percentage FLOAT,
#         pickup_hour INT,
#         pickup_dayofweek INT,
#         pickup_location_id INT,
#         zone_name VARCHAR(255)
#     )
# ''')
# conn.commit()

# Sauvegarde des données dans la table SSMS
# taxi_data_pandas = taxi_data.toPandas()
# for index, row in taxi_data_pandas.iterrows():
#     cursor.execute('''
#         INSERT INTO fact_taxi_trips (trip_id, trip_duration, distance_category, payment_type, tip_percentage, pickup_hour, pickup_dayofweek, pickup_location_id, zone_name)
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
#     ''', (row['trip_id'], row['trip_duration'], row['distance_category'], row['payment_type'], row['tip_percentage'], row['pickup_hour'], row['pickup_dayofweek'], row['pickup_location_id'], row['zone_name']))
#
# conn.commit()
# cursor.close()
# conn.close()

# === Fin du traitement ===
# spark.stop()
