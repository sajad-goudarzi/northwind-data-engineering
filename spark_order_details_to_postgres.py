import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ShortType,
    FloatType,
    DecimalType
)
from decimal import Decimal
import base64


spark = SparkSession.builder \
    .appName("Northwind-OrderDetails-Kafka-to-PostgreSQL") \
    .getOrCreate()


# ========================================
# ORDER DETAILS SCHEMA
# ========================================

order_detail_schema = StructType([
    StructField("OrderID", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("UnitPrice", StringType(), True),
    StructField("Quantity", ShortType(), True),
    StructField("Discount", FloatType(), True)
])


payload_schema = StructType([
    StructField("before", order_detail_schema, True),
    StructField("after", order_detail_schema, True),
    StructField("op", StringType(), True)
])


debezium_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", payload_schema, True)
])


# ========================================
# DECODE DEBEZIUM DECIMAL
# ========================================

@udf(returnType=DecimalType(19, 4))
def decode_decimal(value):
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


# ========================================
# READ FROM KAFKA
# ========================================

kafka_df = spark.read \
    .format("kafka") \
    .option(
        "kafka.bootstrap.servers",
        "localhost:29092"
    ) \
    .option(
        "subscribe",
        "northwind.Northwind.dbo.Order_Details"
    ) \
    .option(
        "startingOffsets",
        "earliest"
    ) \
    .option(
        "endingOffsets",
        "latest"
    ) \
    .load()


# ========================================
# PARSE DEBEZIUM JSON
# ========================================

json_df = kafka_df.select(
    from_json(
        col("value").cast("string"),
        debezium_schema
    ).alias("data")
)


order_details_df = json_df.select(
    col("data.payload.after.*"),
    col("data.payload.op").alias("operation")
)


# ========================================
# KEEP INSERT / SNAPSHOT / UPDATE
# ========================================

order_details_df = order_details_df.filter(
    col("operation").isin("r", "c", "u")
)


# ========================================
# DECODE UNIT PRICE
# ========================================

order_details_df = order_details_df.withColumn(
    "UnitPrice",
    decode_decimal(col("UnitPrice"))
)


# ========================================
# RENAME COLUMNS
# ========================================

order_details_df = order_details_df.select(
    col("OrderID").alias("order_id"),
    col("ProductID").alias("product_id"),
    col("UnitPrice").alias("unit_price"),
    col("Quantity").alias("quantity"),
    col("Discount").alias("discount")
)


# ========================================
# SHOW DATA
# ========================================

print("========================================")
print("ORDER DETAILS DATA")
print("========================================")

order_details_df.show(
    20,
    truncate=False
)


print("========================================")
print("ROW COUNT")
print("========================================")

print(order_details_df.count())


# ========================================
# WRITE TO POSTGRESQL
# ========================================

order_details_df.write \
    .format("jdbc") \
    .option(
        "url",
        f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    ) \
    .option(
        "dbtable",
        "order_details"
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
print("SUCCESS: ORDER DETAILS WRITTEN TO POSTGRESQL")
print("========================================")


spark.stop()
