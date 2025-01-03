from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Carregar variáveis do arquivo .env
from dotenv import load_dotenv
load_dotenv()

# Configurações a partir do .env
MELTANO_ENV_PATH = "/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/meltano_env/bin/activate"
MELTANO_DIR = "/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/meltano/.meltano"

# === default_args ===
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# === DAG para Meltano ===
with DAG(
    dag_id="dag_meltano_pipeline",
    default_args=default_args,
    description="Pipeline Meltano isolado para extrair dados com Meltano",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Tarefa para executar o Meltano
    meltano_run = BashOperator(
        task_id="meltano_run",
        bash_command=f"""
        source {MELTANO_ENV_PATH} && \
        cd {MELTANO_DIR} && \
        meltano run tap-postgres target-bigquery
        """,
        cwd=MELTANO_DIR,  # Diretório de execução do Meltano
    )
