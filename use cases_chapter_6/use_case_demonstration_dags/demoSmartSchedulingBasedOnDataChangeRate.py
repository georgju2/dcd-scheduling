from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from data_insights_plugin import DataInsightScheduler


# Database and parameters
db_name = 'dw_report_sources'
timespan = 24  # Last 24 hours
threshold = 10  # Threshold for change rate (%)

def analyze_and_check_change_rate():
    change_rate = DataInsightScheduler.analyze_data_change_rate(db_name, timespan)
    is_below_threshold = DataInsightScheduler.is_change_rate_below_threshold(db_name, timespan, threshold)
    print (f"In the last {timespan} hours, the database exhibited a change rate of: {change_rate}%. Provided threshold required for report generation: {threshold}%")
    return 'no_report_task' if is_below_threshold else 'generate_report_task'

with DAG('demo_smart_scheduling_data_change_rate', start_date=datetime(2023, 1, 1)) as dag:
    decide_task = BranchPythonOperator(
        task_id='analyze_and_check_change_rate',
        python_callable=analyze_and_check_change_rate
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


