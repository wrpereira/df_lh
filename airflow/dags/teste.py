from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'meltano_etl',
    default_args=default_args,
    description='Executa pipeline Meltano com exclusão de information_schema',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 27),
    catchup=False,
) as dag:

    exclude_information_schema = BashOperator(
        task_id='exclude_information_schema',
        bash_command='meltano select tap-postgres "information_schema*" --exclude',
        cwd='/path/to/meltano/project'
    )

    run_meltano_etl = BashOperator(
        task_id='run_meltano_etl',
        bash_command='meltano run tap-postgres target-bigquery',
        cwd='/path/to/meltano/project'
    )

    exclude_information_schema >> run_meltano_etl
