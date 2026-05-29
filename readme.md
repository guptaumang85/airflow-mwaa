# Real-Time Log Processing on Amazon MWAA

A production-style streaming pipeline that ingests, transports, and indexes **web server logs in
real time** using Apache Kafka and Elasticsearch, orchestrated by Apache Airflow on
**Amazon MWAA** (Managed Workflows for Apache Airflow).

The goal of the project is to demonstrate, step by step, how to build a system capable of handling
billions of log records — from provisioning managed Kafka and Elasticsearch clusters to real-time
indexing and scheduled orchestration.

## Architecture

```
 Log Producer (Faker)
        │  synthetic Apache-style access logs
        ▼
 Confluent Kafka  (topic: billion_website_logs)
        │
        ▼
 Log Consumer  ──parse with regex──▶  Elasticsearch  (bulk index)
        ▲
        └── orchestrated by Airflow DAGs running on Amazon MWAA
```

Credentials for Kafka and Elasticsearch are pulled at runtime from **AWS Secrets Manager**
(`MWAA_Secrets`), so no secrets are stored in the DAG code.

## DAGs

| File | Purpose |
|------|---------|
| `dags/log_producer.py` | Generates synthetic access logs with [Faker](https://faker.readthedocs.io/) and produces them to a Kafka topic over `SASL_SSL`. |
| `dags/log_processing_pipeline.py` | Consumes log messages from Kafka, parses each line with a regex into structured fields, and bulk-indexes them into Elasticsearch. |

## Tech stack

- **Apache Airflow** on Amazon MWAA — orchestration
- **Confluent Kafka** — log transport (SASL/SSL authenticated)
- **Elasticsearch** — search and storage of parsed logs
- **AWS Secrets Manager** — secure credential storage
- **boto3**, **confluent-kafka**, **elasticsearch**, **faker** (see `requirements.txt`)

## Getting started

1. **Provision the managed services** — a Kafka cluster (e.g. Confluent Cloud) and an Elasticsearch
   cluster (e.g. Elastic Cloud).
2. **Store the credentials** in an AWS Secrets Manager secret named `MWAA_Secrets` with the keys:
   `KAFKA_BOOTSTRAP_SERVER`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`,
   `ELASTICSEARCH_URL`, `ELASTICSEARCH_API_KEY`.
3. **Deploy to MWAA** — upload the `dags/` folder and the `requirements.txt` to the S3 bucket backing
   your MWAA environment.
4. **Run** — trigger `log_producer` to stream logs into Kafka, then run the log processing pipeline
   to index them into Elasticsearch.

## Repository layout

```
dags/
  log_producer.py            # produce synthetic logs to Kafka
  log_processing_pipeline.py # consume from Kafka and index into Elasticsearch
requirements.txt             # Python dependencies for the MWAA environment
```
