from datetime import datetime, timedelta
import time
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def sleep_for_seconds():
    """Sleep for 5 seconds."""
    time.sleep(5)

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'parallel_dag_demo',
    default_args=default_args,
    description='A DAG with 10 parallel tasks',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Generate 10 parallel tasks that sleep for 5 seconds each
parallel_tasks = [
    PythonOperator(
        task_id=f'task_{i}',
        python_callable=sleep_for_seconds,
        dag=dag,
    )
    for i in range(1, 11)
]

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start >> parallel_tasks >> end

