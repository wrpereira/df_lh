from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import os

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações a partir do .env
CREDENTIALS_PATH = "/mnt/c/Temp/desafiolh-445818-3cb0f62cb9ef.json"
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
RAW_DATA_CLEANED_PROJECT = os.getenv("RAW_DATA_CLEANED_PROJECT")
RAW_DATA_CLEANED_DATASET = os.getenv("RAW_DATA_CLEANED_DATASET")

# Configuração do BigQuery
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT, location="us-central1")

# Lista de tabelas a serem processadas (schema-tabela)
TABLES_TO_PROCESS = [
    "humanresources-employee",
]

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
    dag_id="dag_combined_pipeline_with_meltano2",
    default_args=default_args,
    description='Pipeline integrado com Meltano: extrai tabelas do PostgreSQL, carrega no BigQuery e executa transformações com Papermill e DBT',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Tarefa para executar o Meltano
    meltano_run = BashOperator(
        task_id="meltano_run",
        bash_command="""
        source /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/meltano_env/bin/activate && \
        cd /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/meltano/.meltano && \
        meltano run tap-postgres target-bigquery
        """,
        cwd="/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/meltano/.meltano",  # Diretório de execução dos comandos
    )

    #Validação após o meltano_run
    def validate_bigquery_table(schema_table):
        schema, table = schema_table.split("-")
        table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{schema}_{table}"
        try:
            client.get_table(table_id)  # Verifica se a tabela existe
            print(f"Tabela {table_id} encontrada no BigQuery.")
        except Exception as e:
            raise ValueError(f"Tabela {table_id} não encontrada no BigQuery: {e}")

    for schema_table in TABLES_TO_PROCESS:
        validate_task = PythonOperator(
            task_id=f"validate_{schema_table}",
            python_callable=validate_bigquery_table,
            op_args=[schema_table],
    )
   
    # Lista de tarefas para notebooks e DBT
    for schema_table in TABLES_TO_PROCESS:
        schema, table = schema_table.split("-")

        # Nome do notebook específico para a tabela
        input_nb = f"/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/{schema}_{table}.ipynb"
        output_nb = f"/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/{schema}_{table}_output.ipynb"

        # Tarefa para rodar o notebook com Papermill
        notebook_task = PapermillOperator(
            task_id=f"notebook_{schema}_{table}",
            input_nb=input_nb,
            output_nb=output_nb,
            parameters={
                'credentials_path': CREDENTIALS_PATH,
                'input_table': f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{schema}_{table}",
                'output_table': f"{RAW_DATA_CLEANED_PROJECT}.{RAW_DATA_CLEANED_DATASET}.{schema}_{table}",
            },
        )

        # Tarefa para rodar DBT
        dbt_task = BashOperator(
            task_id=f"dbt_transform_{schema}_{table}",
            bash_command=f"""
            source /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/dbt_env/bin/activate && \
            cd /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/dbt && \
            dbt run --select {schema}_{table} --profiles-dir /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/dbt_profiles
            """,
        )

        # Configurar dependências
        meltano_run >> notebook_task >> dbt_task
