from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from utils import annotate, set_annotations
from dqm_checker import DataQualityChecker

# Custom Annotated Tasks #

@annotate(category="ExtractFromDB", desc="Extract sales data from SQL database", long_running_task="true", source_db="main_sql_db", data_sensitivity_level="low", upstream="None", special_fields="sales_volume", downstream="transformRawSales")
def extractSalesData():
    pass

@annotate(category="SQLTransform", desc="Transform raw sales data", dependency="extractSalesData", target_db="analytics_db", exceptions="Handle missing fields")
def transformRawSales():
    pass

@annotate(category="LoadToReportDB", desc="Load transformed data to data warehouse", long_running_task="true", dependency="transformRawSales", target_db="dw_sales", sla="24 hours")
def loadToDataWarehouse():
    pass


with DAG(
    dag_id="retail_data_pipeline",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["retail", "ETL"],
) as dag:

    start_task = PythonOperator(task_id="start", python_callable=lambda: print("Pipeline started"))

    with TaskGroup("etl_process") as etl_group:
        extract_task = PythonOperator(task_id="extract_sales_data", 
                python_callable=extractSalesData,
                provide_context=True,
                on_success_callback=DataQualityChecker.dqm_check)
        transform_task = PythonOperator(task_id="transform_sales_data", python_callable=transformRawSales)
        load_task = PythonOperator(task_id="load_to_dw", python_callable=loadToDataWarehouse)

        set_annotations(etl_group, [extract_task, transform_task, load_task])
        extract_task >> transform_task >> load_task

    end_task = PythonOperator(task_id="end", python_callable=lambda: print("Pipeline completed"))

    start_task >> etl_group >> end_task

