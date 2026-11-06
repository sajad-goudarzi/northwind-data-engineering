# Northwind Data Engineering Project

An end-to-end data engineering project built around the Northwind dataset, demonstrating data ingestion, processing, storage, orchestration, and visualization using a Docker-based environment.

## Overview

This project demonstrates a data engineering workflow using Microsoft SQL Server, Debezium, Apache Kafka, PostgreSQL, Apache Spark, ClickHouse, Apache Airflow, and Grafana.

It includes a CDC-based data ingestion pipeline, Python and Spark processing scripts, analytical SQL queries, Grafana dashboards, and an Airflow DAG for monitoring ClickHouse connectivity.

## Architecture

The project uses the following components:

- **SQL Server** — Source database
- **Debezium** — Change Data Capture (CDC)
- **Apache Kafka** — Event streaming and message handling
- **Apache Spark** — Data processing and transformation
- **PostgreSQL** — Relational data storage
- **ClickHouse** — Analytical data storage and querying
- **Apache Airflow** — Workflow scheduling and monitoring
- **Grafana** — Data visualization and dashboards

The CDC workflow captures changes from SQL Server and publishes them to Kafka through Debezium. Python and Spark scripts support data processing and loading into PostgreSQL and ClickHouse. Grafana connects to ClickHouse to visualize analytical results, while Airflow includes a DAG for checking ClickHouse connectivity.

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
- SQL

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

Runtime data, logs, database backups, local configuration files, and credentials are excluded from version control.

## Grafana Dashboards

The project includes three Grafana dashboards:

### 1. Sales Overview
Provides an overview of sales performance through key metrics and visualizations, including:

- Total Revenue
- Average Order Value
- Total Orders
- Units Sold
- Monthly Order Volume
- Monthly Sales Trend
- Top Products by Revenue

### 2. Product Analysis
Focuses on product-level performance, including:

- Product Details
- Total Products
- Total Revenue
- Total Units Sold
- Top 10 Products by Revenue
- Top 10 Products by Units Sold

### 3. Geographical Sales Analysis
Provides geographical insights into Northwind sales data, including:

- Countries Served
- Total Orders
- Customers by Country
- Revenue by Country
- Sales by Country

Dashboard definitions are available in `grafana/dashboards/`. To use them, configure a ClickHouse data source in Grafana and import the corresponding JSON files.

## SQL Queries

The SQL scripts used by the dashboards are organized into three directories:

- `sql/sales-overview/` — Queries for sales KPIs, order volume, revenue, and sales trends.
- `sql/product-analysis/` — Queries for product details, product metrics, and top-performing products.
- `sql/geography/` — Queries for geographical sales, revenue, orders, customers, and countries served.

These scripts provide the SQL logic behind the dashboard panels and can also be reviewed and executed independently.

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
- Grafana dashboards use ClickHouse as their analytical data source.
- Dashboard JSON files can be imported into Grafana after configuring the required data source.
