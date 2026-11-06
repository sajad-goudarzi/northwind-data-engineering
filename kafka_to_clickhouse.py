import os
from kafka import KafkaConsumer
import clickhouse_connect
import json
import base64
from decimal import Decimal
from datetime import datetime


# ========================================
# CONFIG
# ========================================

KAFKA_BOOTSTRAP = "localhost:29092"

ORDERS_TOPIC = "northwind.Northwind.dbo.Orders"
ORDER_DETAILS_TOPIC = "northwind.Northwind.dbo.Order_Details"


# ========================================
# CLICKHOUSE
# ========================================

client = clickhouse_connect.get_client(
    host=os.environ["CLICKHOUSE_HOST"],
    port=int(os.environ["CLICKHOUSE_PORT"]),
    username=os.environ["CLICKHOUSE_USER"],
    password=os.environ["CLICKHOUSE_PASSWORD"],
    database=os.environ["CLICKHOUSE_DB"]
)


# ========================================
# HELPERS
# ========================================

def decode_decimal(value, scale=4):

    if value is None:
        return None

    raw = base64.b64decode(value)

    unscaled = int.from_bytes(
        raw,
        byteorder="big",
        signed=True
    )

    return Decimal(unscaled) / Decimal(10 ** scale)


def convert_timestamp(value):

    if value is None:
        return None

    return datetime.fromtimestamp(
        int(value) / 1000
    )


# ========================================
# KAFKA CONSUMER
# ========================================

consumer = KafkaConsumer(
    ORDERS_TOPIC,
    ORDER_DETAILS_TOPIC,

    bootstrap_servers=KAFKA_BOOTSTRAP,

    auto_offset_reset="earliest",

    enable_auto_commit=False,

    group_id="northwind-clickhouse-consumer",

    value_deserializer=lambda x:
        json.loads(x.decode("utf-8"))
)


print("========================================")
print("Kafka → ClickHouse Consumer Started")
print("========================================")


# ========================================
# CONSUME MESSAGES
# ========================================

for message in consumer:

    data = message.value

    payload = data.get("payload", {})

    operation = payload.get("op")

    after = payload.get("after")


    # Ignore delete events
    if after is None:
        continue


    # ====================================
    # ORDERS
    # ====================================

    if message.topic == ORDERS_TOPIC:

        row = [[

            after.get("OrderID"),

            after.get("CustomerID"),

            after.get("EmployeeID"),

            convert_timestamp(
                after.get("OrderDate")
            ),

            convert_timestamp(
                after.get("RequiredDate")
            ),

            convert_timestamp(
                after.get("ShippedDate")
            ),

            after.get("ShipVia"),

            decode_decimal(
                after.get("Freight")
            ),

            after.get("ShipName"),

            after.get("ShipAddress"),

            after.get("ShipCity"),

            after.get("ShipRegion"),

            after.get("ShipPostalCode"),

            after.get("ShipCountry")
        ]]


        client.insert(

            "orders",

            row,

            column_names=[

                "order_id",
                "customer_id",
                "employee_id",
                "order_date",
                "required_date",
                "shipped_date",
                "ship_via",
                "freight",
                "ship_name",
                "ship_address",
                "ship_city",
                "ship_region",
                "ship_postal_code",
                "ship_country"
            ]
        )


        print(
            f"[ORDERS] "
            f"op={operation} "
            f"OrderID={after.get('OrderID')}"
        )


    # ====================================
    # ORDER DETAILS
    # ====================================

    elif message.topic == ORDER_DETAILS_TOPIC:

        row = [[

            after.get("OrderID"),

            after.get("ProductID"),

            decode_decimal(
                after.get("UnitPrice")
            ),

            after.get("Quantity"),

            after.get("Discount")
        ]]


        client.insert(

            "order_details",

            row,

            column_names=[

                "order_id",
                "product_id",
                "unit_price",
                "quantity",
                "discount"
            ]
        )


        print(
            f"[ORDER_DETAILS] "
            f"op={operation} "
            f"OrderID={after.get('OrderID')} "
            f"ProductID={after.get('ProductID')}"
        )
