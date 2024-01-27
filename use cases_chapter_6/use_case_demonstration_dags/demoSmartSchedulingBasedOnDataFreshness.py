from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from data_insights_plugin import DataInsightScheduler

def decide_report_generation():
    generate_report = DataInsightScheduler.analyze_data_freshness('dw_report_sources', 24)
    return 'generate_report_task' if generate_report else 'no_report_task'


with DAG('demo_smart_scheduling_data_freshness', start_date=datetime(2023, 1, 1)) as dag:
    decide_task = BranchPythonOperator(
        task_id='decide_report_generation',
        python_callable=decide_report_generation
    )

    no_report_task = PythonOperator(
        task_id='no_report_task',
        python_callable=lambda: print("Report generation was not initiated due to the absence of any data updates within the specified time frame.")
    )

    generate_report_task = PythonOperator(
        task_id='generate_report_task',
        python_callable=lambda: print("Report generation criteria satisfied. Initiating the report creation process utilizing the recently obtained data....")
    )

    decide_task >> [no_report_task, generate_report_task]

