import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator

dag = DAG(
    dag_id="iliass-capstone-summer-school-dag",
    schedule_interval=None,
    start_date=datetime.datetime(2023, 9, 15),
    catchup=False,
)

BatchOperator(
    dag=dag,
    task_id="weather_catch_sf",
    job_definition="Iliass_capstone",
    job_queue="academy-capstone-summer-2023-job-queue",
    job_name="iliass-capstone-airflow",
)