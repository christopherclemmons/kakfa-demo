import json, time, random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=10,
        )
        break
    except NoBrokersAvailable:
        print(f"⏳ Kafka not ready yet... retrying ({i+1}/10)")
        time.sleep(5)
else:
    raise Exception("Kafka not reachable")

menu_items = ["Pizza", "Burger", "Taco", "Sushi", "Salad", "Wrap", "Wings"]
customers = ["Alex", "Jordan", "Taylor", "Casey", "Sam", "Riley"]

print("🟢 Ordering service started... generating live feed")

order_id = 1
while True:
    burst = random.random() < 0.2
    delay = random.uniform(0.2, 1.5) if burst else random.uniform(2, 5)
    time.sleep(delay)

    order = {
        "order_id": order_id,
        "customer": random.choice(customers),
        "item": random.choice(menu_items),
        "timestamp": time.strftime("%H:%M:%S"),
        "status": "PLACED",
    }

    producer.send("orders", order)
    producer.flush()
    print(
        f"🧾 [{order['timestamp']}] Order {order_id} placed by {order['customer']} for {order['item']}",
        flush=True,
    )
    order_id += 1
