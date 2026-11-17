import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'subtask_scripts'))
import eag_supply_stage_etl

default_args = {
    'owner': 'evdokimenko_ag',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=2),
}

with DAG(
    dag_id='sd_eag_homework_10',
    default_args=default_args,
    schedule=None,
    start_date=datetime.datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=['evdokimenko_ag', 'homework_10'],
) as dag:
    rabbit_to_s3 = PythonOperator(
        task_id='rabbit_to_s3',
        python_callable=eag_supply_stage_etl.rabbit_to_s3,
    )

    s3_to_stage = PythonOperator(
        task_id='s3_to_stage',
        python_callable=eag_supply_stage_etl.s3_to_stage,
    )

    swap_stage_prod = PythonOperator(
        task_id='swap_stage_prod',
        python_callable=eag_supply_stage_etl.swap_stage_prod,
    )

    clear_stage = PythonOperator(
        task_id='clear_stage',
        python_callable=eag_supply_stage_etl.clear_stage,
    )

    load_fact = PythonOperator(
        task_id='load_fact',
        python_callable=eag_supply_stage_etl.load_fact,
    )

    check_duplicates = PythonOperator(
        task_id='check_duplicates',
        python_callable=eag_supply_stage_etl.check_duplicates,
    )

    rabbit_to_s3 >> s3_to_stage >> swap_stage_prod >> clear_stage >> load_fact >> check_duplicates
