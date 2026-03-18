from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.insert(0, "/opt/airflow/scripts")

from bronze import bronze
from silver import silver
from gold import gold

with DAG(
    dag_id="pipeline_olist",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["olist", "medallion"],
) as dag:

    task_bronze = PythonOperator(
        task_id="bronze",
        python_callable=bronze,
    )

    task_silver = PythonOperator(
        task_id="silver",
        python_callable=silver,
    )

    task_gold = PythonOperator(
        task_id="gold",
        python_callable=gold,
    )

    task_bronze >> task_silver >> task_gold
