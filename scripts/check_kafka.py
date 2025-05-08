from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "camera1_data", "camera2_data",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest"
)

print("Consommation des messages depuis Kafka...")
for message in consumer:
    data = json.loads(message.value.decode("utf-8"))
    print(f"Topic: {message.topic}, Data: {data}")