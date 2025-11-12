import json
import time
import random
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Retry loop to wait for Kafka broker
for i in range(10):
    try:
        consumer = KafkaConsumer(
            "orders",
            bootstrap_servers=["kafka:9092"],
            group_id="delivery-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=2000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        break
    except NoBrokersAvailable:
        print(f"⏳ Kafka not ready yet... retrying ({i+1}/10)")
        time.sleep(5)
else:
    raise Exception("Kafka not reachable")

print("🚛 Delivery service listening for new orders...")

# Consume orders and publish delivery status updates
for message in consumer:
    order = message.value
    oid, item, customer = order["order_id"], order["item"], order["customer"]
    print(f"\n📦 Received order #{oid} ({item}) for {customer}", flush=True)

    # Send sequential status updates
    statuses = ["Preparing", "Picked Up", "Delivered"]
    for status in statuses:
        # Log locally
        emoji = (
            "🍳" if status == "Preparing" else "🛵" if status == "Picked Up" else "✅"
        )
        print(f"{emoji} Order #{oid} {status.lower()} for {customer}", flush=True)

        # Send to Kafka topic 'deliveries'
        producer.send(
            "deliveries",
            {
                "order_id": oid,
                "customer": customer,
                "item": item,
                "status": status,
            },
        )
        producer.flush()

        # Wait before next status update
        time.sleep(random.uniform(1, 3))
