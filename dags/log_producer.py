from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
from faker import Faker
import logging

fake = Faker()
logger = logging.hetLogger(__name__)

def produce_logs(**context):
    """ Produce log entries into Kafka """

default_args = {
    'owner': "Umang Gupta",
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'dag_id': 'log_generation_pipeline',
    'default_args'=default_args,
    description='Generate and produce synthetic logs',
    schedule_interval='*/5 * * * *',
    start_date=datetime(year:2025, month:1, day:26),
    catup=False,
    tags=['logs', 'kafka', 'production']

)

produce_logs_task = PythonOperator(
    task_id='generate_and_produce_logs',
    python_callable=produce_logs,
    dag=dag
)