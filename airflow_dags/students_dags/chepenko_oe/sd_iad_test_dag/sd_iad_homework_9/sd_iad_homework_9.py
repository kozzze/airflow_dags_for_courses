
import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_DIR = os.path.dirname(os.path.abspath(__file__))

if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)

from subtask_scripts.rabbit_to_s3 import fetch_rabbit_to_s3
from subtask_scripts.s3_to_clickhouse import load_s3_to_clickhouse


default_args = {
    "owner": "chepenko_oe",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2),
}


with DAG(
    dag_id="chepenko_oe_sd_iad_homework_9",
    default_args=default_args,
    description="Supply events: RabbitMQ -> Yandex Object Storage -> ClickHouse",
    start_date=datetime.datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["chepenko_oe", "homework_9", "supply"],
) as dag:

    rabbit_to_s3 = PythonOperator(
        task_id="chepenko_oe_rabbit_to_s3",
        python_callable=fetch_rabbit_to_s3,
        op_kwargs={"login": "chepenko_oe"},
    )

    s3_to_clickhouse = PythonOperator(
        task_id="chepenko_oe_s3_to_clickhouse",
        python_callable=load_s3_to_clickhouse,
        op_kwargs={
            "s3_key": "{{ ti.xcom_pull(task_ids='chepenko_oe_rabbit_to_s3') }}"
        },
    )

    rabbit_to_s3 >> s3_to_clickhouse
