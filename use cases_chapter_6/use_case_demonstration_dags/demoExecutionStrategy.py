import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from utils import annotate, set_annotations
from execution_strategy_manager import ExecutionManager


# Custom Annotated Task #

@annotate(category="LoadToReportDB", desc="Load transformed data to data warehouse", execution_strategy="latest_only", target_db="dw_sales", sla="24 hours")
def load_data(**context):
    task_instance = context['ti']
    if task_instance.try_number < 3:
        raise ValueError("Try Number less than 3")
    else:
        # Checking for action determined by the execution manager after 2 failed retries
        action = ExecutionManager.handle_task_restart(context)
        print(f"Task failed twice. Trying new strategy. Action for retry: {action}")

        # Define behavior based on the action
        if action == 'wait_for_prior_tasks':
            print("Waiting for prior tasks to complete before retrying.")
            time.sleep(10)
        else:
            # Possible options: Execute follow up task, Alert for failure, Skip execution etc.
            print("Proceeding without waiting for task to complete")


default_args = {
    "retries": 4,
    "retry_delay": timedelta(seconds=4),
}

with DAG(
    dag_id="load_data_pipeline",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["demo"],
) as dag:

    start_task = PythonOperator(task_id="start", python_callable=lambda: print("Pipeline started"))

    with TaskGroup('load_group') as load_group:
        load_task = PythonOperator(task_id="load_data", python_callable=load_data)
        set_annotations(load_group, [load_task])
        load_task

    end_task = PythonOperator(task_id="end", python_callable=lambda: print("Pipeline completed"))

    start_task >> load_group >> end_task

