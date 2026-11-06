import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("NorthwindKafkaToPostgres")
    .master("local[*]")
    .getOrCreate()
)

# =========================
# Schemas
# =========================

order_schema = StructType([
    StructField("OrderID", IntegerType(), False),
    StructField("CustomerID", StringType(), True),
    StructField("EmployeeID", IntegerType(), True),
    StructField("OrderDate", LongType(), True),
    StructField("RequiredDate", LongType(), True),
    StructField("ShippedDate", LongType(), True),
    StructField("ShipVia", IntegerType(), True),
    StructField("Freight", StringType(), True),
    StructField("ShipName", StringType(), True),
    StructField("ShipAddress", StringType(), True),
    StructField("ShipCity", StringType(), True),
    StructField("ShipRegion", StringType(), True),
    StructField("ShipPostalCode", StringType(), True),
    StructField("ShipCountry", StringType(), True)
])

payload_schema = StructType([
    StructField("before", order_schema, True),
    StructField("after", order_schema, True),
    StructField("op", StringType(), True)
])

debezium_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", payload_schema, True)
])

# =========================
# Read Kafka
# =========================

kafka_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "northwind.Northwind.dbo.Orders")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

# =========================
# Parse Debezium JSON
# =========================

json_df = kafka_df.select(
    from_json(
        col("value").cast("string"),
        debezium_schema
    ).alias("data")
)

orders_df = json_df.select(
    col("data.payload.after.*"),
    col("data.payload.op").alias("operation")
)

# =========================
# Keep snapshot records
# =========================

orders_df = (
    orders_df
    .filter(col("operation") == "r")
    .drop("operation")
)

# =========================
# Convert timestamps
# =========================

orders_df = (
    orders_df
    .withColumn(
        "OrderDate",
        to_timestamp(col("OrderDate") / 1000)
    )
    .withColumn(
        "RequiredDate",
        to_timestamp(col("RequiredDate") / 1000)
    )
    .withColumn(
        "ShippedDate",
        to_timestamp(col("ShippedDate") / 1000)
    )
)

# =========================
# Rename columns
# =========================

orders_df = orders_df.select(
    col("OrderID").alias("order_id"),
    col("CustomerID").alias("customer_id"),
    col("EmployeeID").alias("employee_id"),
    col("OrderDate").alias("order_date"),
    col("RequiredDate").alias("required_date"),
    col("ShippedDate").alias("shipped_date"),
    col("ShipVia").alias("ship_via"),
    col("Freight").alias("freight"),
    col("ShipName").alias("ship_name"),
    col("ShipAddress").alias("ship_address"),
    col("ShipCity").alias("ship_city"),
    col("ShipRegion").alias("ship_region"),
    col("ShipPostalCode").alias("ship_postal_code"),
    col("ShipCountry").alias("ship_country")
)

print("===== Data to PostgreSQL =====")
orders_df.show(10, truncate=False)

print("Total rows:", orders_df.count())

# =========================
# PostgreSQL
# =========================

jdbc_url = f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"

(
    orders_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "orders")
    .option("user", os.environ["POSTGRES_USER"])
    .option("password", os.environ["POSTGRES_PASSWORD"])
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

print("===== Successfully written to PostgreSQL =====")

spark.stop()
