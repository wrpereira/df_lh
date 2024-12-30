from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.utils.dates import days_ago

# Definir argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Criar a DAG
with DAG(
    dag_id='eda_humanresources_employee_dag',
    default_args=default_args,
    description='Executa EDA do humanresources_employee usando Papermill',
    schedule_interval=None,  # Pode ser ajustado para rodar periodicamente
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Tarefa para executar o notebook
    execute_notebook = PapermillOperator(
        task_id='execute_eda_notebook',
        input_nb='/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/EDA_humanresources_employee.ipynb',
        output_nb='/mnt/c/Users/wrpen/OneDrive/Desktop/df_lh/airflow/jupyter/EDA_humanresources_employee_output.ipynb',
        parameters={"run_date": "{{ ds }}"},  # Pode passar parâmetros para o notebook
    )
