from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd
import os

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações a partir do .env
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH")
RAW_DATA_TABLE = f"{os.getenv('BIGQUERY_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}.humanresources_employee"
RAW_DATA_CLEANED_TABLE = f"{os.getenv('RAW_DATA_CLEANED_PROJECT')}.{os.getenv('RAW_DATA_CLEANED_DATASET')}.humanresources_employee"

# Configuração do BigQuery
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=os.getenv("BIGQUERY_PROJECT"))

# === default_args ===
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),  # Ajuste a data conforme necessário
    'retries': 1,
}

# Definindo a DAG
with DAG(
    dag_id="humanresources_employee_pipeline",
    default_args=default_args,
    schedule_interval=None,  # DAG executada manualmente
    catchup=False,
) as dag:
    # Definindo as tarefas
    extract = PythonOperator(
        task_id="extract_raw_data",
        python_callable=extract_raw_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_cleaned_data",
        python_callable=load_cleaned_data,
    )

    # Definindo a sequência de execução das tarefas
    extract >> transform >> load

# === FUNÇÕES DO PIPELINE ===

# === EXTRACT ===

def extract_raw_data():
    # Extrair os dados brutos do BigQuery
    query = f"SELECT * FROM `{RAW_DATA_TABLE}`"
    raw_data = client.query(query).result().to_dataframe()
    
    # Salvar os dados extraídos como pickle para a próxima etapa
    raw_data.to_pickle("/tmp/humanresources_employee_raw.pkl")  
    print("Dados extraídos do BigQuery.")

# === TRANSFORM ===

def transform_data():
    # Executar o notebook humanresources_employee.ipynb usando o PapermillOperator
    transform_notebook = PapermillOperator(
        task_id="transform_humanresources_employee_notebook",
        input_nb="C:/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/humanresources_employee.ipynb",  # Caminho do notebook de transformação
        output_nb="/tmp/humanresources_employee_transformed_output.ipynb",
        parameters={
            'credentials_path': CREDENTIALS_PATH,
            'table_name': 'humanresources_employee',
        }
    )
    transform_notebook.execute(None)
    print("Dados tratados no notebook.")

# === LOAD ===

def load_cleaned_data():
    # Executar o notebook para carregar os dados no BigQuery
    load_notebook = PapermillOperator(
        task_id="load_humanresources_employee_notebook",
        input_nb="C:/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/humanresources_employee_load.ipynb",  # Caminho do notebook de carga
        output_nb="/tmp/humanresources_employee_load_output.ipynb",
        parameters={
            'credentials_path': CREDENTIALS_PATH,
            'destination_table': RAW_DATA_CLEANED_TABLE,
        }
    )

    load_notebook.execute(None)
    print("Dados tratados carregados no BigQuery.")
