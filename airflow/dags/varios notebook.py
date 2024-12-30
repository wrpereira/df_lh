from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações a partir do .env
CREDENTIALS_PATH = "/mnt/c/Temp/desafiolh-445818-3cb0f62cb9ef.json"
RAW_DATASET = os.getenv('BIGQUERY_DATASET')
CLEANED_DATASET = os.getenv('RAW_DATA_CLEANED_DATASET')
PROJECT_ID = os.getenv('BIGQUERY_PROJECT')

# Configuração do BigQuery
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Função para extrair dados
def extract_raw_data(table_name, **kwargs):
    query = f"SELECT * FROM `{PROJECT_ID}.{RAW_DATASET}.{table_name}`"
    raw_data = client.query(query).result().to_dataframe()
    raw_data.to_pickle(f"/tmp/{table_name}_raw.pkl")
    print(f"Dados extraídos para a tabela: {table_name}")

# Parâmetros padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Definição da DAG
with DAG(
    dag_id="dag_dynamic_raw_data_pipeline",
    default_args=default_args,
    description='Executa EDA em múltiplas tabelas usando Papermill',
    schedule_interval=None,  # Pode ser ajustado para rodar periodicamente
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Lista de tabelas e seus notebooks correspondentes
    tables = [
        {"table_name": "humanresources_employee", "notebook": "EDA_humanresources_employee.ipynb"},
        {"table_name": "person_person", "notebook": "EDA_person_person.ipynb"},
        {"table_name": "sales_store", "notebook": "EDA_sales_store.ipynb"},
    ]

    for table in tables:
        table_name = table["table_name"]
        notebook_path = f"/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/{table['notebook']}"
        output_nb_path = f"/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/{table_name}_output.ipynb"
        raw_table = f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"
        cleaned_table = f"{PROJECT_ID}.{CLEANED_DATASET}.{table_name}"

        # Tarefa de Extração
        extract = PythonOperator(
            task_id=f"extract_{table_name}",
            python_callable=extract_raw_data,
            op_args=[table_name],  # Passa o nome da tabela para a função
        )

        # Tarefa de Transformação e Carga com Papermill
        load = PapermillOperator(
            task_id=f"load_{table_name}",
            input_nb=notebook_path,
            output_nb=output_nb_path,
            parameters={
                'credentials_path': CREDENTIALS_PATH,
                'table_name': raw_table,  # Tabela de origem
                'destination_table': cleaned_table,  # Tabela de destino
            },
        )

        # Ordem das tarefas
        extract >> load
