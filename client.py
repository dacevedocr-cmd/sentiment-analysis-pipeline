import os
import json
import random
import time
from kafka import KafkaProducer

# KAFKA_BROKER should point to your WSL2 host IP (get it with `hostname -I`) — see .env.example
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = "twitter_comments"

COMMENTS = [
    "I love this product, it's amazing!",
    "This is the worst experience I've ever had.",
    "Pretty decent, nothing special though.",
    "Absolutely fantastic service, will come back!",
    "I hate waiting so long for a response.",
    "Not bad, but could be better.",
    "Totally satisfied with my purchase!",
    "This is frustrating, nothing works as expected.",
    "Great quality, highly recommend!",
    "Disappointed with the results, expected more.",
    "Excellent customer support, very helpful.",
    "Terrible product, broke after one day.",
    "It's okay, does what it promises.",
    "Best decision I've made, love it!",
    "Very unhappy with this service.",
    "The product works as expected, nothing more.",
    "It's an average experience overall.",
    "Not great, not terrible, just okay.",
]

USERS = [f"user_{i:03d}" for i in range(1, 21)]

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def main():
    producer = create_producer()
    num_messages = random.randint(10, 30)
    client_id = random.choice(USERS)

    print(f"[CLIENT] {client_id} — enviando {num_messages} mensajes...")

    for i in range(num_messages):
        message = {
            "user_id": client_id,
            "comment": random.choice(COMMENTS)
        }
        producer.send(TOPIC, value=message)
        print(f"  [{i+1}/{num_messages}] Enviado: {message['comment'][:40]}...")
        time.sleep(random.uniform(0.5, 3.0))

    producer.flush()
    print(f"[CLIENT] {client_id} — terminó de enviar mensajes.")

if __name__ == "__main__":
    main()
