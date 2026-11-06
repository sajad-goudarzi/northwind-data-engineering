# Northwind Data Engineering Project

An end-to-end data engineering project built around the Northwind dataset, demonstrating data ingestion, processing, storage, orchestration, and visualization using a Docker-based environment.

## Overview

This project uses Northwind data to explore a data engineering workflow with SQL Server, Apache Kafka, Debezium, PostgreSQL, ClickHouse, Apache Spark, Apache Airflow, and Grafana.

The project includes data processing scripts, a ClickHouse data pipeline, an Airflow DAG, and Grafana dashboards for data visualization and reporting.

## Architecture

The project uses the following components:

- **SQL Server** — Source database
- **Debezium** — Change Data Capture (CDC)
- **Apache Kafka** — Event streaming
- **Apache Spark** — Data processing
- **PostgreSQL** — Relational data storage
- **ClickHouse** — Analytical data storage
- **Apache Airflow** — Workflow orchestration
- **Grafana** — Data visualization and dashboards

The CDC workflow captures changes from SQL Server and publishes them to Kafka through Debezium. Python and Spark scripts support data processing and loading into the target databases. Grafana connects to ClickHouse for analytical dashboards, while Airflow provides workflow orchestration and monitoring.

## Services and Ports

| Service | Host Port |
|---|---:|
| Grafana | 23000 |
| Apache Airflow | 8080 |
| ClickHouse HTTP | 28123 |
| ClickHouse Native | 29000 |
| Debezium | 28083 |
| Kafka | 29092 |
| PostgreSQL | 25432 |
| SQL Server | 22433 |

## Technologies

- Docker and Docker Compose
- Microsoft SQL Server
- Apache Kafka and Debezium
- Apache Spark
- PostgreSQL
- ClickHouse
- Apache Airflow
- Grafana
- Python

## Project Structure

```text
northwind_export/
├── airflow/
│   ├── dags/
│   │   └── northwind_clickhouse_check.py
│   └── airflow_home/
├── docker-compose.example.yml
├── .gitignore
├── kafka_to_clickhouse.py
├── spark_order_details_to_postgres.py
├── spark_to_postgres.py
└── test_kafka.py
```

Runtime data, logs, database backups, local configuration files, and credentials are excluded from version control.

## Configuration

The example Compose file uses environment variables for sensitive configuration.

Required variables:

| Variable | Description |
|---|---|
| `MSSQL_SA_PASSWORD` | SQL Server administrator password |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `CLICKHOUSE_PASSWORD` | ClickHouse password |
| `GF_SECURITY_ADMIN_PASSWORD` | Grafana administrator password |
| `AIRFLOW_DB_URL` | SQLAlchemy connection URL for the Airflow metadata database |

Set these variables in the shell before starting the services. Do not commit real passwords or local configuration containing credentials.

The `AIRFLOW_DB_URL` value must point to the Airflow metadata database and use the connection details configured for that database.

## Running the Project

Requirements:

- Docker Desktop with WSL 2 integration, or a Linux Docker environment
- Docker Compose
- Required environment variables configured

Start the services:

```bash
docker-compose -f docker-compose.example.yml up -d
```

Check the service status:

```bash
docker-compose -f docker-compose.example.yml ps
```

## Accessing the Services

After the services are running:

- Grafana: http://localhost:23000
- Airflow: http://localhost:8080
- ClickHouse HTTP interface: http://localhost:28123
- Debezium REST API: http://localhost:28083

## Notes

- The example Compose file is intended for reference and public sharing.
- Local credentials and runtime data should remain outside version control.
- The Airflow metadata database is separate from the Northwind analytical databases.
- Grafana dashboards use ClickHouse as an analytical data source.
