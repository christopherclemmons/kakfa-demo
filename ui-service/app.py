from fastapi import FastAPI, WebSocket
from fastapi.responses import FileResponse
from kafka import KafkaConsumer
import asyncio, json

app = FastAPI()


@app.get("/")
def get_index():
    return FileResponse("static/index.html")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    consumer = KafkaConsumer(
        "orders",
        "deliveries",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        group_id="ui-consumer",
    )

    try:
        while True:
            records = consumer.poll(timeout_ms=500)
            for tp, messages in records.items():
                for message in messages:
                    data = message.value
                    await websocket.send_json(data)
            await asyncio.sleep(0.1)
    except Exception as e:
        print("Error:", e)
    finally:
        consumer.close()
        await websocket.close()
