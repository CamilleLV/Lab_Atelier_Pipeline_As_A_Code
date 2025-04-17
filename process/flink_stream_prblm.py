import os 
from pyflink.datastream import StreamExecutionEnvironment 
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main(): 
    # 1. Environnement Flink 
    env = StreamExecutionEnvironment.get_execution_environment() 
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build() 
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. Configuration MinIO (S3-compatible)
    table_env.get_config().get_configuration().set_string("fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000"))
    table_env.get_config().get_configuration().set_string("fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    table_env.get_config().get_configuration().set_string("fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    table_env.get_config().get_configuration().set_string("fs.s3a.path.style.access", "true")

    # 3. Source : fichiers JSON dans MinIO (s3a://weather-data/)
    table_env.execute_sql("""
        CREATE TEMPORARY TABLE weather_staging (
            description_temps STRING,
            temperature FLOAT,
            temperature_ressentie_c FLOAT,
            humidite_wm INT,
            pression_atmospherique_hpa INT,
            niveau_mer_hpa INT,
            pression_sol_hpa INT,
            vitesse_vent_m_per_sec FLOAT,
            direction_vent_deg INT,
            rafales_vent_m_per_sec FLOAT,
            precipitations_1h_mm_per_h FLOAT,
            couverture_nuageuse_perc INT,
            heure_lever_soleil BIGINT,
            heure_coucher_soleil BIGINT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://weather-data/',
            'format' = 'json',
            'scan.startup.mode' = 'earliest',
            'scan.continuous.discovery.interval' = '30s',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # 4. Sink : base MySQL (assurez-vous que weather_data existe déjà avec AUTO_INCREMENT sur id)
    table_env.execute_sql("""
        CREATE TEMPORARY TABLE weather_data (
            id INT NOT NULL AUTO_INCREMENT,
            description_temps STRING,
            temperature FLOAT,
            temperature_ressentie_c FLOAT,
            humidite_wm INT,
            pression_atmospherique_hpa INT,
            niveau_mer_hpa INT,
            pression_sol_hpa INT,
            vitesse_vent_m_per_sec FLOAT,
            direction_vent_deg INT,
            rafales_vent_m_per_sec FLOAT,
            precipitations_1h_mm_per_h FLOAT,
            couverture_nuageuse_perc INT,
            heure_lever_soleil BIGINT,
            heure_coucher_soleil BIGINT,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://mysql:3306/weatherdb',
            'table-name' = 'weather_data',
            'username' = 'root',
            'password' = 'rootpassword',
            'driver' = 'com.mysql.cj.jdbc.Driver'
        )
    """)

    table = table_env.from_path("weather_staging")
    table.execute().print()

    # 5. Insertion des données dans MySQL
    table_env.execute_sql("""
        INSERT INTO weather_data
        SELECT
            description_temps,
            temperature,
            temperature_ressentie_c,
            humidite_wm,
            pression_atmospherique_hpa,
            niveau_mer_hpa,
            pression_sol_hpa,
            vitesse_vent_m_per_sec,
            direction_vent_deg,
            rafales_vent_m_per_sec,
            precipitations_1h_mm_per_h,
            couverture_nuageuse_perc,
            heure_lever_soleil,
            heure_coucher_soleil
        FROM weather_staging
    """)

if __name__ == 'main': 
    main()