from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import time
import requests
from uuid import uuid1


def load_states():
    print('Hello, this is the first task of the DAG')
    time.sleep(5)


def extract_api_1():
    print("Extracting data from API 1")
    resp = requests.get("https://catfact.ninja/fact")
    data = resp.json()
    return {'fact': data['fact']}


def extract_api_2():
    print("Extracting data from API 2")
    time.sleep(5)


def extract_api_3():
    print("Extracting data from API 3")
    time.sleep(5)


def transform_api_1(**context):
    print("Transforming API 1 data")
    extracted = context['ti'].xcom_pull(task_ids='extract_1')
    transformed = extracted["fact"].upper()
    if context['ti'].try_number < 2:
        raise ValueError("Try Number less than 2")
    return {"fact": transformed}


def transform_api_2():
    print("Transforming API 2 data")
    time.sleep(5)


def transform_api_3():
    print("Transforming API 3 data")
    time.sleep(5)


def load_api_1(**context):
    print("Loading API 1 data")
    transformed_data = context["ti"].xcom_pull(task_ids="transform_1")
    fact_id = str(uuid1())
    hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = hook.get_conn()
    cursor = connection.cursor()
    insert_sql = """
        INSERT INTO fact (id, fact)
        VALUES (%s, %s);
    """
    cursor.execute(insert_sql, (fact_id, transformed_data["fact"]))
    connection.commit()
    cursor.close()
    connection.close()


def load_api_2():
    print("Loading API 2 data")
    time.sleep(5)


def load_api_3():
    print("Loading API 3 data")
    time.sleep(5)


def save_states():
    print('DAG run is done.')


default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=5),
}


with DAG(
    dag_id="celery_executor_demo",
    default_args=default_args,
	start_date=datetime(2021, 1, 1),
	schedule_interval="@hourly",
	catchup=False) as dag:
    
    start = PythonOperator(
        task_id="start",
        python_callable=load_states
    )

    extract_1 = PythonOperator(
        task_id="extract_1",
        python_callable=extract_api_1
    )
    
    extract_2 = PythonOperator(
        task_id="extract_2",
        python_callable=extract_api_2
    )
    
    extract_3 = PythonOperator(
        task_id="extract_3",
        python_callable=extract_api_3
    )
    
    transform_1 = PythonOperator(
        task_id="transform_1",
        provide_context=True,
        python_callable=transform_api_1
    )

    transform_2 = PythonOperator(
        task_id="transform_2",
        python_callable=transform_api_2
    )

    transform_3 = PythonOperator(
        task_id="transform_3",
        python_callable=transform_api_3
    )

    load_1 = PythonOperator(
        task_id="load_1",
        python_callable=load_api_1
    )

    load_2 = PythonOperator(
        task_id="load_2",
        python_callable=load_api_2
    )

    load_3 = PythonOperator(
        task_id="load_3",
        python_callable=load_api_3
    )
    
    end = PythonOperator(
        task_id="end",
        python_callable=save_states
    )


start >> [extract_1, extract_2, extract_3]
extract_1 >> transform_1
extract_2 >> transform_2
extract_3 >> transform_3
transform_1 >> load_1
transform_2 >> load_2
transform_3 >> load_3
[load_1, load_2, load_3] >> end

