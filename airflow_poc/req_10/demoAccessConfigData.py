# Trigger the DAG using Config JSON from UI
# To trigger the DAG supply the following JSON config:
# {"timestamp": "2023-09-14 12:00:00", "another_column": "100"}

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

# Define default_args for DAG
default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'demo_access_transmitted_data',
    default_args=default_args,
    description='Process data from manually provided JSON and branch',
    schedule_interval=None,  # Setting to None as it will be triggered manually
    start_date=days_ago(0),
    catchup=False,
    tags=['example'],
)

def check_data_age(**kwargs):
    received_json = kwargs['dag_run'].conf
    timestamp_str = received_json["timestamp"]
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    specific_timestamp = datetime.strptime('2023-09-15 15:30:00', '%Y-%m-%d %H:%M:%S')

    return 'store_data_task' if timestamp < specific_timestamp else 'log_not_older_task'

def store_transformed_data(**kwargs):
    received_json = kwargs['dag_run'].conf
    another_column_value = int(received_json["another_column"])
    # Transformation logic
    transformed_value = another_column_value + 10
    print(f"Logging transformed value: {transformed_value}")

def log_data_message():
    print("Supplied timestamp value is newer!")

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=check_data_age,
    provide_context=True,
    dag=dag
)

store_data_task = PythonOperator(
    task_id='store_data_task',
    python_callable=store_transformed_data,
    provide_context=True,
    dag=dag
)

log_not_older_task = PythonOperator(
    task_id='log_not_older_task',
    python_callable=log_data_message,
    dag=dag
)

branch_task >> [store_data_task, log_not_older_task]

