from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd
import os

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações a partir do .env
CREDENTIALS_PATH = "/mnt/c/Temp/desafiolh-445818-3cb0f62cb9ef.json"
RAW_DATA_TABLE = f"{os.getenv('BIGQUERY_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}.humanresources_employee"
RAW_DATA_CLEANED_TABLE = f"{os.getenv('RAW_DATA_CLEANED_PROJECT')}.{os.getenv('RAW_DATA_CLEANED_DATASET')}.humanresources_employee"
client = bigquery.Client(credentials=credentials, project=os.getenv("BIGQUERY_PROJECT"), location="us-central1")
# Configuração do BigQuery
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
#client = bigquery.Client(credentials=credentials, project=os.getenv("BIGQUERY_PROJECT"))
client = bigquery.Client(credentials=credentials, project=os.getenv("BIGQUERY_PROJECT"), location="us-central1")


# Verifica se a variável está configurada
print(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

# === FUNÇÕES DO PIPELINE ===

# === EXTRACT ===
def extract_raw_data():
    query = f"SELECT * FROM `{RAW_DATA_TABLE}`"
    raw_data = client.query(query).result().to_dataframe()
    raw_data.to_pickle("/tmp/humanresources_employee_raw.pkl")
    print("Dados extraídos do BigQuery.")

# === TRANSFORM ===
def transform_data():
    print("Placeholder para a transformação (Papermill na DAG).")

# === LOAD ===
def load_cleaned_data():
    print("Placeholder para a carga (Papermill na DAG).")

# === default_args ===
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# === DAG ===
with DAG(
    dag_id="dag_raw_data_pipeline2",
    default_args=default_args,
    description='Executa EDA do humanresources_employee usando Papermill',
    schedule_interval=None,  # Pode ser ajustado para rodar periodicamente
    start_date=days_ago(1),
    catchup=False,

) as dag:

    # Tarefa de Extração
    extract = PythonOperator(
        task_id="extract_raw_data",
        python_callable=extract_raw_data,
    )

    # Tarefa de Tranformação e Carga com Papermill
    load = PapermillOperator(
    task_id="load_humanresources_employee",
    input_nb="/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/EDA_humanresources_employee.ipynb",
    output_nb="/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/EDA_humanresources_employee_output.ipynb",
    parameters={
        'credentials_path': CREDENTIALS_PATH,
        'table_name': 'raw_data_cleaned.humanresources_employee',
    },
)

    # Ordem das tarefas
    extract >> load  # O notebook de "load" unifica as fases de transformação e carga

