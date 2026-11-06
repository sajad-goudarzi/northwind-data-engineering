
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.parse import quote
import base64
import os


def check_clickhouse():
    query = quote("SELECT 1")
    url = f"http://northwind_clickhouse:8123/?query={query}"

    password = os.environ["CLICKHOUSE_PASSWORD"]
    credentials = base64.b64encode(
        f"default:{password}".encode()
    ).decode("ascii")

    request = Request(url)
    request.add_header("Authorization", f"Basic {credentials}")

    with urlopen(request, timeout=10) as response:
        result = response.read().decode().strip()

    print(f"ClickHouse query result: {result}")

    if result != "1":
        raise ValueError(f"Unexpected result: {result}")


from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


with DAG(
    dag_id="northwind_clickhouse_check",
    description="Test Airflow connection to Northwind ClickHouse",
    start_date=datetime(2026, 10, 31),
    schedule=None,
    catchup=False,
    tags=["northwind", "clickhouse"],
) as dag:

    check_clickhouse_task = PythonOperator(
        task_id="check_clickhouse_connection",
        python_callable=check_clickhouse,
    )
