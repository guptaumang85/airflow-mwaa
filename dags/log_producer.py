from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
from faker import Faker
import boto3
import logging, json, random
from datetime import datetime, timedelta

fake = Faker()
logger = logging.getLogger(__name__)

def create_kafka_producer(config):
    return Producer(config)

def generate_log():
    """ Generate synthetic log """
    methods = ['GET', 'POST', 'PUT', 'DELETE']
    endpoints = ['/api/users', '/home', '/about', '/contact', '/services']
    statuses = [200, 301, 302, 400, 404, 500]

    user_agents = [
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X)',
        'Mozilla/5.0 (X11; Linux x86_64)',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Mozilla/5.0 (Macintosh; Intel Max OS X 10_15_7)'
    ]

    referrers = ['https://example.com', 'https://google.com', '-', 'https://bing.com', 'https://yahoo.com']

    ip = fake.ipv4()
    timestamp = datetime.now().strftime('%b %d %Y, %H:%M:%S')
    method = random.choice(endpoints)
    status = random.choice(statuses)
    size = random.randint(1000, 15000)
    referrer = random.choice(referrers)
    user_agent = random.choice(user_agents)

    log_entry = (
        f'{ip} - - [{timestamp}] "{method} {endpoints} HTTP/1.1" {status} {size} "{referrer}" {user_agent}'
    )

    return log_entry

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failer: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

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

def produce_logs(**context):
    """ Produce log entries into Kafka """
    secrets = get_secret('MWAA_Secrets')
    kafka_config = {
        'bootstrap.servers': secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'session.timeout.ms': 50000
    }

    producer = create_kafka_producer(kafka_config)
    topic = 'billlion_website_logs'

    for _ in range(15000):
        log = generate_log()
        try:
            producer.produce(topic, log.encode('utf-8'), on_delivery=delivery_report)
            producer.flush()
        except Exception as e:
            logger.error(f'Error producing log: {e}')
            raise
    
    logger.info(f'Produced 15,000 logs to topic {topic}')

default_args = {
    'owner': "Umang Gupta",
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

# dag = DAG(
#     'dag_id': 'log_generation_pipeline',
#     'default_args'=default_args,
#     description='Generate and produce synthetic logs',
#     schedule_interval='*/5 * * * *',
#     start_date=datetime(year:2025, month:1, day:26),
#     catup=False,
#     tags=['logs', 'kafka', 'production']

# )

# produce_logs_task = PythonOperator(
#     task_id='generate_and_produce_logs',
#     python_callable=produce_logs,
#     dag=dag
# )

produce_logs()