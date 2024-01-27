# Trigger conditional task execution based on data from the Table

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

# Define default_args for DAG
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_processing_based_on_table_value',
    default_args=default_args,
    description='Read and process data from Postgres DB',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    catchup=False,
    tags=['demo'],
)

def check_data_age(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = "SELECT created_on FROM accounts WHERE id=1 LIMIT 1;"  
    timestamp = pg_hook.get_first(sql)[0]
    specific_timestamp = datetime.strptime('2023-09-15 15:30:00', '%Y-%m-%d %H:%M:%S')

    return 'store_data_task' if timestamp < specific_timestamp else 'log_not_older_task'

def store_transformed_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    # Transformation logic and SQL update statement
    update_sql = """
        UPDATE accounts
        SET unit = unit + 5
    """
    pg_hook.run(update_sql)

def log_data_message():
    print("Value in table is newer!")

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

