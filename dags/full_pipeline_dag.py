
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Définition des tâches pour extraire, traiter, et charger les données
def extract_taxi_data():
    # Ici, tu peux appeler un script ou une fonction pour extraire les données de taxi
    subprocess.run(["python", "extract/fetch_yellow_trip.py"])

def extract_weather_data():
    # Ici, tu peux appeler un script ou une fonction pour extraire les données météo
    subprocess.run(["python", "extract/fetch_weather_stream.py"])

def process_data():
    # Appel à un script ou fonction pour traiter les données
    subprocess.run(["python", "process/spark_stream.py"])

def load_to_dbt():
    # Lancer la commande DBT pour charger les données dans la base de données
    subprocess.run(["dbt", "run"])

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),  # Utilisez la date de début appropriée
    'retries': 1,
}

dag = DAG(
    'full_pipeline_dag',
    default_args=default_args,
    description='DAG pour le pipeline de données complet',
    schedule_interval='@daily',  # Planification quotidienne, ajustez selon le besoin
)

# Définition des tâches dans le DAG
task_extract_taxi = PythonOperator(
    task_id='extract_taxi_data',
    python_callable=extract_taxi_data,
    dag=dag,
)

task_extract_weather = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    dag=dag,
)

task_process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

task_load_to_dbt = PythonOperator(
    task_id='load_to_dbt',
    python_callable=load_to_dbt,
    dag=dag,
)

# Dépendances entre les tâches
task_extract_taxi >> task_extract_weather >> task_process_data >> task_load_to_dbt
