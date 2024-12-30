from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import subprocess

def check_env():
    print("Python Path:", sys.executable)

    # Listar pacotes instalados
    print("Installed Packages:")
    subprocess.run([sys.executable, "-m", "pip", "list"])

# Definição do DAG
with DAG(
    dag_id="check_env",
    default_args={"retries": 1},
    description="DAG para verificar o ambiente Python",
    schedule_interval=None,  # Rodar manualmente
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Definição da tarefa
    task_check_env = PythonOperator(
        task_id="check_env_task",
        python_callable=check_env,
    )
