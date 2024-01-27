import time

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from utils import annotate, set_annotations
from airflow.models import TaskFail
from alert_task_failure import AlertUtils 


# Custom Annotated Tasks #

@annotate(category="ExtractFromCSV", desc="Extract Client Data from CSV file", dependency="none", data_sensitivity_level="high", upstream= "monthly_dqm_file", special_fields="Confidential | Contains PII")
def extractLegalInfo():
    print("Running my task 1")
    # Demo - send out email to team on failure of a task that has annotation with data_sensitivity_level="high"
    raise Exception("Simulating a task failure!")

@annotate(category="SalesTransform", desc="Quarterly Sales Transform", dependency="extract_store_sales, extract_online_sales", target_db= "t_04", exceptions="Will not be run on public holidays")
def transformSales():
    print("Running my task 2")


@annotate(category="LoadToCloud", desc="Loads generated report to AWS", dependency="validateInfo, transformSales", downstream= "AWS S3 Bucket", log_performance="true", failure_contact="support_person@aws.com")
def loadReport():
    time.sleep(2)
    print("Running my task 3")


with DAG(
    dag_id="demo_annotations",
    start_date=datetime(2023, 9, 15),
    catchup=False,
    tags=["example"],
) as dag:

    start = DummyOperator(task_id="start")

    with TaskGroup("section_1") as section_1:
        task_1 = PythonOperator(task_id="task_1", 
                                python_callable=extractLegalInfo,
                                provide_context=True,
                                on_failure_callback=AlertUtils.handle_task_failure)
        
        task_2 = PythonOperator(task_id="task_2", python_callable=transformSales)
        
        task_3 = PythonOperator(task_id="task_3", python_callable=loadReport)

        # Propagate the metadata in the UI and store to dedicated files #
        set_annotations(section_1, [task_1, task_2, task_3])

        task_1 >> task_2 >> task_3

    end = DummyOperator(task_id='end')

    start >> section_1 >> end

