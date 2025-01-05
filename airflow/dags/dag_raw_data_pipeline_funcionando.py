from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import time
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
from pendulum import timezone #adicionei depois de validar, só tirar
import os

# Carregar variáveis do arquivo .env
load_dotenv("/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/.env")  # Caminho para o arquivo .env

# Configurações a partir do .env
CREDENTIALS_PATH = "/mnt/c/Temp/desafiolh-445818-3cb0f62cb9ef.json"
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
RAW_DATA_CLEANED_PROJECT = os.getenv("RAW_DATA_CLEANED_PROJECT")
RAW_DATA_CLEANED_DATASET = os.getenv("RAW_DATA_CLEANED_DATASET")

# Configuração do BigQuery
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT, location="us-central1")

# Lista de tabelas a serem processadas (sschema-tabela)
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
# Configura o fuso horário
fuso_horario = timezone("America/Sao_Paulo")

def wait_for_3_minutes():
    """
    Função que aguarda 3 minutos antes de continuar com a execução.
    """
    print("Aguardando 3 minutos para garantir que a DAG do Meltano tenha sido concluída...")
    time.sleep(1800)  # Aguarda 3 minutos

def wait_10_minutes():
    """
    Função que aguarda 2 minutos antes de continuar com a execução.
    """
    print("Aguardando 2 minutos para garantir que as tabelas estejam no raw_data_cleaned...")
    time.sleep(600)  # Aguarda 2 minutos


# === DAG ===

with DAG(
    dag_id="dag_combined_pipeline_with_meltano3",
    default_args=default_args,
    description='Pipeline integrado com Meltano: extrai tabelas do PostgreSQL, carrega no BigQuery e executa transformações com Papermill e DBT',
    # schedule_interval='@daily',  # Executa diariamente
    # start_date=timezone("America/Sao_Paulo").datetime(2025, 1, 4, 0, 0),  # Data de início EDITAR PRA 1X AO DIA AS TAL HORA IGUAL A DO MELTANO
    # end_date=timezone("America/Sao_Paulo").datetime(2025, 1, 6, 23, 59),  # Data de término
    schedule_interval=None, #DEPOIS QUE EU FINALIZAR -- APAAAAAAAAAAAGAR ESSA LINHA
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:


    print(f"Tabelas a processar: {TABLES_TO_PROCESS}")

    # Tarefa para disparar a DAG do Meltano
    trigger_meltano = TriggerDagRunOperator(
        task_id="trigger_meltano_dag",
        trigger_dag_id="dag_meltano_pipeline",  # ID da DAG do Meltano
        conf={},  # Você pode passar configurações, se necessário
    )

    # Tarefa para aguardar 3 minutos
    wait_for_3_minutes_task = PythonOperator(
        task_id="wait_for_3_minutes",
        python_callable=wait_for_3_minutes,
    )

    # Loop para criar tarefas específicas para cada tabela
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

        # Tarefa para aguardar 10 minutos
        wait_10_task = PythonOperator(
            task_id=f"wait_10_minutes_{schema}_{table}",
            python_callable=wait_10_minutes,       
        )

        # Tarefa para rodar DBT
        dbt_task = BashOperator(
            task_id=f"dbt_transform_{schema}_{table}",
            bash_command=f"""
            source /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/dbt_env/bin/activate && \
            cd /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/dbt && \
            dbt run --select staging.stg_{schema}_{table} --profiles-dir /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/dbt_profiles
            """,
        )

        # Configurar dependências
        trigger_meltano >> wait_for_3_minutes_task >> notebook_task >> wait_10_task >> dbt_task
        
