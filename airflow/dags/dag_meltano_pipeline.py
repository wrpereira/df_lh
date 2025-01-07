from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pendulum import timezone
import os
import glob

# Carregar variáveis do arquivo .env
load_dotenv("/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/.env")

# Configurações a partir do .env
MELTANO_ENV_PATH = "/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/meltano_env/bin/activate"
MELTANO_DIR = "/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/meltano/.meltano"
CSV_DIR = "/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/meltano/.meltano/csv_output" 
BIGQUERY_PROJECT = "desafioadventureworks-446600"
BIGQUERY_DATASET = "raw_data"

# Configura o fuso horário
fuso_horario = timezone("America/Sao_Paulo")

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# === DAG ===
# with DAG(
#     dag_id="dag_meltano_postgres_csv_bigquery",
#     default_args=default_args,
#     description="Pipeline Meltano: Postgres -> CSV -> BigQuery",
#     # schedule_interval='0 */3 * * *',  # Executa a cada 3 horas    
#     start_date=fuso_horario.datetime(2025, 1, 4, 0, 0),
#     end_date=fuso_horario.datetime(2025, 1, 6, 23, 59),
#     catchup=False,
#     max_active_runs=2,
# ) as dag:

# DAG
with DAG(
    dag_id="dag_meltano_postgres_csv_bigquery",
    default_args=default_args,
    description='Pipeline Meltano: Postgres -> CSV -> BigQuery',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=2,
) as dag:

    # Log de início
    def log_execution(**kwargs):
        print(f"Executando a DAG às {datetime.now()}")

    log_task = PythonOperator(
        task_id="log_execution",
        python_callable=log_execution,
    )

    # Tarefa: Meltano run para extrair dados em CSV
    meltano_to_csv = BashOperator(
        task_id="meltano_to_csv",
        bash_command=f"""
        source {MELTANO_ENV_PATH} && \
        cd {MELTANO_DIR} && \
        meltano run tap-postgres target-csv
        """,
        cwd=MELTANO_DIR,
    )

    # Tarefa: Carregar os CSVs no BigQuery
    def load_csv_to_bigquery(**kwargs):
        csv_files = glob.glob(f"{CSV_DIR}/*.csv")
        for csv_file in csv_files:
            table_name = os.path.basename(csv_file).replace('.csv', '')  # Remove a extensão .csv
            command = f"""
            bq load \
            --source_format=CSV \
            --autodetect \
            {BIGQUERY_PROJECT}:{BIGQUERY_DATASET}.{table_name} \
            {csv_file}
            """
            print(f"Executando: {command}")
            result = os.system(command)
            if result != 0:
                raise Exception(f"Erro ao carregar o arquivo {csv_file} no BigQuery")


    load_to_bigquery = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_csv_to_bigquery,
    )

    # Ordem de execução
    log_task >> meltano_to_csv >> load_to_bigquery
