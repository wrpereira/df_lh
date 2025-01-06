from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

# Carregar variáveis do arquivo .env
load_dotenv("/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/.env")

# Configurações do BigQuery
CREDENTIALS_PATH = "/mnt/c/Temp/desafiolh-445818-3cb0f62cb9ef.json"
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=os.getenv("BIGQUERY_PROJECT"), location="us-central1")

TABLES_TO_PROCESS = ["humanresources-employee",
                     "person-address",
                     "person-businessentity",
                     "person-person",
                     "person-stateprovince",
                     "production-location",
                     "production-product",
                     "production-productcategory",
                     "production-productinventory",
                     "production-productsubcategory",
                     "sales-customer",
                     "sales-salesorderdetail",
                     "sales-salesorderheader",
                     "sales-salesterritory",
                     "sales-store"]

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG
with DAG(
    dag_id="dag_combined_pipeline_with_meltano",
    default_args=default_args,
    description='Pipeline integrado com Meltano',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    
    # Disparar a DAG do Meltano
    trigger_meltano = TriggerDagRunOperator(
        task_id="trigger_meltano_dag",
        trigger_dag_id="dag_meltano_postgres_csv_bigquery",
        conf={},
    )

    # Esperar 1 minutos com TimeDeltaSensor
    wait_for_1_minutes_task = TimeDeltaSensor(
        task_id="wait_for_1_minutes",
        delta=timedelta(minutes=1),
    )

    for schema_table in TABLES_TO_PROCESS:
        schema, table = schema_table.split("-")

        input_nb = f"/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/{schema}_{table}.ipynb"
        output_nb = f"/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/{schema}_{table}_output.ipynb"

        # Executar notebook com Papermill
        notebook_task = PapermillOperator(
            task_id=f"notebook_{schema}_{table}",
            input_nb=input_nb,
            output_nb=output_nb,
            parameters={
                'credentials_path': CREDENTIALS_PATH,
                'input_table': f"{os.getenv('BIGQUERY_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}.{schema}_{table}",
                'output_table': f"{os.getenv('RAW_DATA_CLEANED_PROJECT')}.{os.getenv('RAW_DATA_CLEANED_DATASET')}.{schema}_{table}",
            },
        )

        # Esperar 1 minutos com TimeDeltaSensor
        wait_1_task = TimeDeltaSensor(
            task_id=f"wait_1_minutes_{schema}_{table}",
            delta=timedelta(minutes=5),
        )

        # Executar transformações com DBT
        dbt_task = BashOperator(
            task_id=f"dbt_transform_{schema}_{table}",
            bash_command=f"""
            source /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/dbt_env/bin/activate && \
            cd /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/dbt && \
            dbt clean --profiles-dir /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/dbt_profiles && \
            dbt run --select staging.stg_{schema}_{table} --profiles-dir /mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/dbt_profiles
            """,
        )


        # Configurar dependências
        trigger_meltano >> wait_for_1_minutes_task >> notebook_task >> wait_1_task >> dbt_task
   