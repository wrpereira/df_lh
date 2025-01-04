from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
from pendulum import timezone #adicionei depois de validar, só tirar
import os

# Carregar variáveis do arquivo .env

load_dotenv("/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/.env")  # Caminho para o arquivo .env

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
    #schedule_interval='@daily',  # Executa diariamente
    schedule_interval='0 */3 * * *',
    start_date=timezone("America/Sao_Paulo").datetime(2025, 1, 4, 0, 0),  # Data de início
    end_date=timezone("America/Sao_Paulo").datetime(2025, 1, 6, 23, 59),  # Data de término
    #schedule_interval=None,
    #start_date=datetime(2023, 1, 1),
    catchup=False,
    timezone=timezone("America/Sao_Paulo"),  # Define o fuso horário
) as dag:

    def log_execution(**kwargs):
        print(f"Executando a DAG às {datetime.now()}")

    log_task = PythonOperator(
        task_id="log_execution",
        python_callable=log_execution,
    )    


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

    log_task >> meltano_run