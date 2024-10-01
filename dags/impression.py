from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
from impression_scraper import main
from datetime import datetime
from airflow.utils.dates import days_ago
import logging

def start_task_func():
    logging.info("this DAG started!")


dag = DAG(
    dag_id="impression_scraper_dag",
    default_args={"start_date": days_ago(1)},
    schedule_interval="57 12 1 10 *",
    catchup = False
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=start_task_func,
    dag=dag
)

scraper_task = PythonOperator(
    task_id='scraper_task',
    python_callable=main,
    dag=dag
)

start_task >> scraper_task