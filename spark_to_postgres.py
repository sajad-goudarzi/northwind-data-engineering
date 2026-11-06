import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from decimal import Decimal
import base64


# ==========================================
# 1. Start Spark
# ==========================================

spark = SparkSession.builder \
    .appName("Northwind-Kafka-to-PostgreSQL") \
    .getOrCreate()


# ==========================================
# 2. Debezium Schema
# ==========================================

order_schema = StructType([
    StructField("OrderID", IntegerType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("EmployeeID", IntegerType(), True),
    StructField("OrderDate", StringType(), True),
    StructField("RequiredDate", StringType(), True),
    StructField("ShippedDate", StringType(), True),
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


# ==========================================
# 3. Decode Freight
# ==========================================

@udf(returnType=DecimalType(19, 4))
def decode_freight(value):

    if value is None:
        return None

    try:
        raw = base64.b64decode(value)

        unscaled = int.from_bytes(
            raw,
            byteorder="big",
            signed=True
        )

        return Decimal(unscaled) / Decimal(10000)

    except Exception:
        return None


# ==========================================
# 4. Read Kafka
# ==========================================

kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "northwind.Northwind.dbo.Orders") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()


# ==========================================
# 5. Parse Debezium JSON
# ==========================================

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


# ==========================================
# 6. Select Snapshot Records
# ==========================================

orders_df = orders_df.filter(
    col("operation") == "r"
)


# ==========================================
# 7. Transform Data
# ==========================================

orders_df = orders_df \
    .withColumn(
        "OrderDate",
        to_timestamp(col("OrderDate").cast("long") / 1000)
    ) \
    .withColumn(
        "RequiredDate",
        to_timestamp(col("RequiredDate").cast("long") / 1000)
    ) \
    .withColumn(
        "ShippedDate",
        to_timestamp(col("ShippedDate").cast("long") / 1000)
    ) \
    .withColumn(
        "Freight",
        decode_freight(col("Freight"))
    )


# ==========================================
# 8. Rename Columns
# ==========================================

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


# ==========================================
# 9. Check Data Before Insert
# ==========================================

print("========================================")
print("DATA TO POSTGRESQL")
print("========================================")

orders_df.show(10, truncate=False)


print("========================================")
print("ROW COUNT")
print("========================================")

print(orders_df.count())


# ==========================================
# 10. Write to PostgreSQL
# ==========================================

orders_df.write \
    .format("jdbc") \
    .option(
        "url",
        f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    ) \
    .option(
        "dbtable",
        "orders"
    ) \
    .option(
        "user",
        os.environ["POSTGRES_USER"]
    ) \
    .option(
        "password",
        os.environ["POSTGRES_PASSWORD"]
    ) \
    .option(
        "driver",
        "org.postgresql.Driver"
    ) \
    .mode("append") \
    .save()


print("========================================")
print("SUCCESS: DATA WRITTEN TO POSTGRESQL")
print("========================================")


# ==========================================
# 11. Stop Spark
# ==========================================

spark.stop()
