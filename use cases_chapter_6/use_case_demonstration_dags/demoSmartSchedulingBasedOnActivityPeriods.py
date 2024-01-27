from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from data_insights_plugin import DataInsightScheduler

# Database and parameters
db_name = 'dw_report_sources'
interval_hours = 6  # Interval duration for checking activity
lookback_duration = timedelta(days=365)  # Lookback duration for activity analysis

def check_activity_and_decide():
    low_activity_periods = DataInsightScheduler.find_low_activity_periods(db_name, interval_hours, lookback_duration)
    current_time = datetime.now()
    current_month_day_hour = current_time.replace(year=1900, minute=0, second=0, microsecond=0)

    # Check if the current month, day, and hour is in a high activity period
    for period in low_activity_periods:
        period_start = datetime.strptime(period[0], "%Y-%m-%d %H:%M:%S").replace(year=1900, minute=0, second=0, microsecond=0)
        if period_start <= current_month_day_hour < (period_start + timedelta(hours=interval_hours)):
            print (f"The current time {current_time}, lies in a higher activity period: {period}")
            return 'no_report_task'  # Current month, day, and hour is in a high activity period

    print (f"The current time {current_time}, lies in a lower activity period: {period}")
    return 'generate_report_task'  # Current month, day, and hour is not in a high activity period

with DAG('demo_smart_scheduling_activity_periods', start_date=datetime(2023, 1, 1)) as dag:
    decide_activity_task = BranchPythonOperator(
        task_id='check_activity_and_decide',
        python_callable=check_activity_and_decide
    )

    no_report_task = PythonOperator(
        task_id='no_report_task',
        python_callable=lambda: print("Report generation was skipped as the current time (month, day, and hour) is within a high activity period.")
    )

    generate_report_task = PythonOperator(
        task_id='generate_report_task',
        python_callable=lambda: print("Low activity period detected for the current month, day, and hour. Proceeding with report generation.")
    )

    decide_activity_task >> [no_report_task, generate_report_task]

