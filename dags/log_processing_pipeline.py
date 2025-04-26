from datetime import datetime, timedelta
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
import logging
import boto3

logger = logging.getLogger(__name__)

def get_secret(secret_name, region_name='ap-south-1'):
    """ Retrieve secrets from AWS secret Manager """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        logger.error(f"Secret retrieval: {e}")
        raise

def parsed_log_entry(log_entry):
    log_pattern = r'(?P<ip>[\d\.]+) - - \[(?P<timestamp>.*?)\] "(?P<method>\w+) (?P<endpoint>[\w/]+) (?P<protocol>[\w/\.]+)'
    match = re.match(log_pattern, log_entry)
    if not match:
        logger.warning(f'Invalid log format: {log_entry}')
        return None
    
    data = match.groupdict()

    try:
        parsed_timestamp = datetime.strptime(data['timestamp'], format= '%b %d %Y, %H:%M:%S')
        data['@timestamp'] = parsed_timestamp.isoformat()
    except ValueError:
        logger.error(f'Timestamp parsing error: {data['timestamp']}')
        return None

    return data

def consume_and_index_logs(**context):
    secrets = get_secret('MWAA_Secrets')

    consumer_config = {
        'bootstrap.servers': secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'group.id': 'mwaa_log_indexer',
        'auto.offset.reset': 'latest'
    }

    es_config = {
        'hosts': [secrets['ELASTICSEARCH_URL']],
        'api_key': secrets['ELASTICSEARCH_API_KEY']
    }

    consumer = Consumer(consumer_config)
    es = Elasticsearch(**es_config)
    topic = 'billlion_website_logs'
    consumer.subscribe([topic])

    try:
        index_name = 'billlion_website_logs'
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            logger.info(f'Created index: {index_name}')
    except Exception as e:
        logger.error(f'Failed to create index: {index_name} {e}')

    try:
        logs = []
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break

            if msg.error():
                if msg.error().code() == KafkaException.PARTITION_EOF:
                    break
                raise KafkaException(msg.error())
        
            log_entry = msg.value().decode('utf-8')
            parsed_log = parsed_log_entry(log_entry)

            if parsed_log:
                logs.append(parsed_log)
        
            # index when 15000 logs are collected
            if len(logs) >= 50:
                actions = [
                    {
                        '_op_type': 'create',
                        '_index': index_name,
                        '_source': log
                    }
                    for log in logs
                ]
                success, failed = bulk(es, actions, refresh=True)
                logs = []
                logger.info(f'Indexed {success} logs, {len(failed)} failed.')
    except Exception as e:
        logger.error(f'Failed to index log: {e}')
    
    try:
        # index any remaining logs
        if logs:
            actions = [
                {
                    '_op_type': 'create',
                    '_index': index_name,
                    '_source': log
                }
                for log in logs
            ]
            bulk(es, actions, refresh=True)
    except Exception as e:
        logger.error(f'Log processing error: {e}')
    finally:
        consumer.close()
        es.close()

default_args = {
    'owner': "Umang Gupta",
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

# dag = DAG(
#     dag_id='log_consumer_pipeline',
#     default_args=default_args,
#     description='Generate and produce synthetic logs',
#     schedule_interval='*/5 * * * *',
#     start_date=datetime(2025, 1, 26),
#     catchup=False,
#     tags=['logs', 'kafka', 'production']
# )

# consume_logs_task = PythonOperator(
#     task_id='generate_and_produce_logs',
#     python_callable=consume_and_index_logs,
#     dag=dag
# )

consume_and_index_logs()
