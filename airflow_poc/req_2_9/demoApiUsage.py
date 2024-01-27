"""Example DAG to demonstrate API use in Tasks"""
from __future__ import annotations

import datetime
import time

import pendulum

from airflow.decorators import dag, task


def sla_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(
        "The callback arguments are: ",
        {
            "dag": dag,
            "task_list": task_list,
            "blocking_task_list": blocking_task_list,
            "slas": slas,
            "blocking_tis": blocking_tis,
        },
    )


@dag(
    schedule="*/20 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    sla_miss_callback=sla_callback,
    default_args={"email": "email@example.com"},
)
def demo_api_usage():
    @task(task_id="long_running_task1",sla=datetime.timedelta(seconds=10))
    def sleep_60():
        """Sleep for 60 seconds"""
        time.sleep(60)

    @task(task_id="long_running_task_finished")
    def echo():
        print("long running task is over")


    sleep_60() >> echo()


demo_dag = demo_api_usage()

