from pyflink.datastream import StreamExecutionEnvironment 
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main(): 
    env = StreamExecutionEnvironment.get_execution_environment() 
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build() 
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Ajout des config pour MinIO (si nécessaire)
    table_env.get_config().get_configuration().set_string("fs.s3a.access.key", "minioadmin")
    table_env.get_config().get_configuration().set_string("fs.s3a.secret.key", "minioadmin")
    table_env.get_config().get_configuration().set_string("fs.s3a.endpoint", "http://127.0.0.1:9000")
    table_env.get_config().get_configuration().set_string("fs.s3a.path.style.access", "true")

    # Table source depuis MinIO (via S3A ou FILESYSTEM selon ton setup)
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
            'path' = 's3a://weather-data/',  -- Assure-toi que s3a est bien configuré
            'format' = 'json',
            'scan.startup.mode' = 'earliest',
            'scan.continuous.discovery.interval' = '30s'
        )
    """)

    # Table cible MySQL
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
            'url' = 'jdbc:mysql://mysql-host:3306/your_database',
            'table-name' = 'weather_data',
            'username' = 'your_user',
            'password' = 'your_password',
            'driver' = 'com.mysql.cj.jdbc.Driver'
        )
    """)

    # Insertion avec transformation (par exemple : ajouter un filtre ou enrichir)
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
