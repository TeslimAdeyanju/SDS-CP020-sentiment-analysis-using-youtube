from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  # Past date for immediate availability
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG('hello_world',
         default_args=default_args,
         schedule_interval=timedelta(days=1),
         catchup=False) as dag:

    t1 = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello World from Airflow!"'
    )
