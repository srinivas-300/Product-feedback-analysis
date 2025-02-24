from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.topic_modelling import topic
from scripts.data_merge import merge

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG("reviews_analysis_airflow",
         default_args = default_args,
         description = "This is for automation of the extracting usefull information from the reviews data",
         catchup=False,
         schedule_interval=@daily)

topics_extraction = PythonOperator( task = "topics_extraction" , dag=dag , python_callable= topic)

merge_dataset = PythonOperator(task = "merge_dataset" , dag=dag , python_callable = merge)

topics_extraction >> merge_dataset