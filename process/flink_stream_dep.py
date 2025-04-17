from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema
from pyflink.table import TableDescriptor, FormatDescriptor, Schema
import json
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Configuration MinIO
MINIO_BUCKET = "weather-data"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "your-access-key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "your-secret-key")

# Configuration MySQL avec SQLAlchemy
MYSQL_CONN_STR = f"mssql+pyodbc://@victus_camille/LAB_ATL_PIPELINE?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(MYSQL_CONN_STR)

MYSQL_TABLE = os.getenv("MYSQL_TABLE", "weather_data")

def process_weather_data(json_data):
    """
    Process JSON data and insert into MySQL using SQLAlchemy.
    """
    try:
        # Parse JSON
        weather_data = json.loads(json_data)

        # Convert JSON to a dictionary for insertion
        data_to_insert = {
            "description_temps": weather_data["weather"][0]["description"],
            "temperature": weather_data["main"]["temp"],
            "temperature_ressentie_c": weather_data["main"]["feels_like"],
            "humidite_wm": weather_data["main"]["humidity"],
            "pression_atmospherique_hpa": weather_data["main"]["pressure"],
            "niveau_mer_hpa": weather_data["main"].get("sea_level", None),
            "pression_sol_hpa": weather_data["main"].get("grnd_level", None),
            "vitesse_vent_m_per_sec": weather_data["wind"]["speed"],
            "direction_vent_deg": weather_data["wind"]["deg"],
            "rafales_vent_m_per_sec": weather_data["wind"].get("gust", 0),
            "precipitations_1h_mm_per_h": weather_data.get("rain", {}).get("1h", 0),
            "couverture_nuageuse_perc": weather_data["clouds"]["all"],
            "heure_lever_soleil": weather_data["sys"]["sunrise"],
            "heure_coucher_soleil": weather_data["sys"]["sunset"]
        }

        # Insert data into MySQL
        connection = engine.connect()
        try:
            connection.execute(
                f"""
                INSERT INTO {MYSQL_TABLE} (
                    description_temps, temperature, temperature_ressentie_c, humidite_wm,
                    pression_atmospherique_hpa, niveau_mer_hpa, pression_sol_hpa,
                    vitesse_vent_m_per_sec, direction_vent_deg, rafales_vent_m_per_sec,
                    precipitations_1h_mm_per_h, couverture_nuageuse_perc,
                    heure_lever_soleil, heure_coucher_soleil
                ) VALUES (
                    :description_temps, :temperature, :temperature_ressentie_c, :humidite_wm,
                    :pression_atmospherique_hpa, :niveau_mer_hpa, :pression_sol_hpa,
                    :vitesse_vent_m_per_sec, :direction_vent_deg, :rafales_vent_m_per_sec,
                    :precipitations_1h_mm_per_h, :couverture_nuageuse_perc,
                    :heure_lever_soleil, :heure_coucher_soleil
                )
                """,
                **data_to_insert
            )
        finally:
            connection.close()
        print("Données insérées avec succès dans MySQL.")

    except Exception as e:
        print(f"Error processing data: {e}")

def main():
    # Set up the Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)

    # Define the source (MinIO bucket)
    table_env.create_temporary_table(
        "weather_source",
        TableDescriptor.for_connector("filesystem")
        .schema(Schema.new_builder()
                .column("description_temps", DataTypes.STRING())
                .column("temperature", DataTypes.FLOAT())
                .column("temperature_ressentie_c", DataTypes.FLOAT())
                .column("humidite_wm", DataTypes.INT())
                .column("pression_atmospherique_hpa", DataTypes.INT())
                .column("niveau_mer_hpa", DataTypes.INT())
                .column("pression_sol_hpa", DataTypes.INT())
                .column("vitesse_vent_m_per_sec", DataTypes.FLOAT())
                .column("direction_vent_deg", DataTypes.INT())
                .column("rafales_vent_m_per_sec", DataTypes.FLOAT())
                .column("precipitations_1h_mm_per_h", DataTypes.FLOAT())
                .column("couverture_nuageuse_perc", DataTypes.INT())
                .column("heure_lever_soleil", DataTypes.BIGINT())
                .column("heure_coucher_soleil", DataTypes.BIGINT())
                .build())
        .format(FormatDescriptor.for_format("json")
                .option("fail-on-missing-field", "false")
                .build())
        .option("path", f"{MINIO_ENDPOINT}/{MINIO_BUCKET}")
        .build()
    )

    # Read data from the source
    weather_data = table_env.execute_sql("SELECT * FROM weather_source")

    # Process each record in the stream
    for record in weather_data.collect():
        process_weather_data(json.dumps(record))

    # Execute the Flink job
    env.execute("Weather Data Processing")

if __name__ == "__main__":
    main()