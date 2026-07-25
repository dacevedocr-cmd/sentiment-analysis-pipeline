import os
import json
from datetime import datetime, timezone
from kafka import KafkaConsumer
from pymongo import MongoClient

# KAFKA_BROKER should point to your WSL2 host IP (get it with `hostname -I`) — see .env.example
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = "twitter_comments"
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "sentiment_db")
COLLECTION = "twitter_comments"

def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="sentiment_consumer_group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

def create_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION]

def main():
    consumer = create_consumer()
    collection = create_mongo_collection()

    print(f"[CONSUMER] Escuchando topic '{TOPIC}'...")

    for message in consumer:
        data = message.value
        document = {
            "user_id": data["user_id"],
            "comment": data["comment"],
            "timestamp": datetime.now(timezone.utc),
            "kafka_offset": message.offset,
            "kafka_partition": message.partition,
            "processed": False
        }
        result = collection.insert_one(document)
        print(f"  [+] Guardado en MongoDB — id: {result.inserted_id} | {data['comment'][:40]}...")

if __name__ == "__main__":
    main()
