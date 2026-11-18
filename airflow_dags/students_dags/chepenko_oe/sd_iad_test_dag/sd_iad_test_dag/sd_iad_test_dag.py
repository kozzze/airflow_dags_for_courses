from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Позволяет импортировать вспомогательные модули из папки рядом с DAG.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from subtask_scripts.subtask_from_folder import task_from_folder


def test_python(arg="Chepenko"):
    print(f"SUCCESS {arg}!!!")


default_args = {
    "owner": "chepenko_oe",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="sd_iad_test_dag",
    default_args=default_args,
    description="Тестовый DAG для проверки загрузки и запуска задач",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["chepenko_oe", "test", "homework_9"],
) as dag:
    task_1 = PythonOperator(
        task_id="test_without_argument",
        python_callable=test_python,
    )

    task_2 = PythonOperator(
        task_id="test_with_argument",
        python_callable=test_python,
        op_kwargs={"arg": "Airflow"},
    )

    task_3 = PythonOperator(
        task_id="test_from_subtask",
        python_callable=task_from_folder,
    )

    task_1 >> task_2 >> task_3
