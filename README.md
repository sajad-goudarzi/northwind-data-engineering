# Northwind Data Engineering Project

An end-to-end data engineering project built around the Northwind dataset, demonstrating data ingestion, processing, storage, workflow orchestration, and visualization using a Docker-based environment.

## Overview

This project demonstrates a data engineering workflow using Microsoft SQL Server, Debezium, Apache Kafka, Apache Spark, PostgreSQL, ClickHouse, Apache Airflow, and Grafana.

The project includes Change Data Capture (CDC), Kafka event consumption, Python and Spark-based data processing, relational and analytical data storage, Airflow DAG execution, and interactive Grafana dashboards.

## Architecture & Technologies

- **SQL Server:** Source database for operational Northwind data.
- **Debezium & CDC:** Captures data changes from SQL Server and publishes change events to Kafka.
- **Apache Kafka:** Handles streaming and transferring CDC events.
- **Apache Spark:** Processes and transforms relational data before loading it into PostgreSQL.
- **PostgreSQL:** Stores Spark-processed relational data and Airflow metadata in separate databases.
- **Python Kafka Consumer:** Consumes Kafka events and loads data into ClickHouse.
- **ClickHouse:** Analytical database used to store data for reporting and visualization.
- **Apache Airflow:** Executes and monitors DAGs, including ClickHouse connectivity checks. Uses PostgreSQL to store workflow metadata.
- **Grafana:** Connects to ClickHouse and provides interactive dashboards for data analysis and visualization.
- **Docker & Docker Compose:** Containerizes and manages the project services and their dependencies.

## Data Flow

### 1. Relational Data Processing & Workflow Orchestration

SQL Server → Debezium (CDC) → Apache Kafka → Apache Spark → PostgreSQL

Apache Airflow executes and monitors DAGs, using PostgreSQL to store workflow metadata.

**PostgreSQL Databases:**
- `northwind`: Stores relational data processed by Apache Spark.
- `airflow`: Stores Airflow metadata, including DAG and task execution information.

### 2. Analytical Data Pipeline

SQL Server → Debezium (CDC) → Apache Kafka → Python Kafka Consumer → ClickHouse → Grafana

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

## Project Structure

```text
northwind_export/
├── airflow/
│   └── dags/
│       └── northwind_clickhouse_check.py
├── grafana/
│   └── dashboards/
│       ├── sales-overview.json
│       ├── product-analysis.json
│       └── geographical-sales-analysis.json
├── sql/
│   ├── sales-overview/
│   ├── product-analysis/
│   └── geography/
├── docker-compose.example.yml
├── .gitignore
├── kafka_to_clickhouse.py
├── spark_order_details_to_postgres.py
├── spark_to_postgres.py
└── test_kafka.py
```

## Grafana Dashboards

The project includes three dashboards:

### 1. Sales Overview

Provides an overview of sales performance through key metrics and visualizations:

- Total Revenue
- Average Order Value
- Units Sold
- Monthly Order Volume
- Monthly Sales Trend

### 2. Product Analysis

Focuses on product-level performance and includes:

- Top 10 Products by Units Sold
- Top 10 Products by Revenue
- Product sales performance

### 3. Geographical Sales Analysis

Provides insights into geographical sales distribution, including:

- Countries Served
- Total Orders by Country
- Revenue by Country
- Customer Distribution by Country

## SQL Queries

The SQL directory contains analytical queries used by Grafana dashboards:

- `sales-overview/`: Queries for sales KPIs and monthly trends.
- `product-analysis/`: Queries for product performance and top-selling products.
- `geography/`: Queries for geographical sales analysis.

## Configuration

The project uses environment variables for sensitive credentials and connection settings.

| Variable | Description |
|---|---|
| `MSSQL_SA_PASSWORD` | SQL Server administrator password |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `CLICKHOUSE_PASSWORD` | ClickHouse password |
| `GF_SECURITY_ADMIN_PASSWORD` | Grafana administrator password |
| `AIRFLOW_DB_URL` | SQLAlchemy connection URL for the Airflow metadata database |

The `AIRFLOW_DB_URL` must point to the `airflow` database on the PostgreSQL service.

Sensitive credentials, local configuration files, and runtime data are excluded from version control.

## Running the Project

Make sure Docker and Docker Compose are installed and the required environment variables are configured.

Start the services:

```bash
docker-compose -f docker-compose.example.yml up -d
```

Check service status:

```bash
docker-compose -f docker-compose.example.yml ps
```

## Accessing the Services

| Service | URL |
|---|---|
| Grafana | http://localhost:23000 |
| Apache Airflow | http://localhost:8080 |
| ClickHouse HTTP | http://localhost:28123 |
| Debezium | http://localhost:28083 |

## Notes

- `docker-compose.example.yml` is provided as an example Compose configuration.
- Sensitive credentials, runtime data, logs, backups, and local configuration files are excluded from version control.
- PostgreSQL is used for both Spark-processed relational data and Airflow metadata, stored in separate databases.
- The current Airflow DAG checks ClickHouse connectivity and can be triggered through the Airflow interface. It does not orchestrate the entire end-to-end data pipeline.
- Grafana dashboards use ClickHouse as their analytical data source.
- Docker Compose manages the project services and their dependencies.
