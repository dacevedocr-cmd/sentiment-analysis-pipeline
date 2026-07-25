import os
import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from pymongo import MongoClient
from bson import ObjectId
import mysql.connector
from datetime import datetime, timezone
import time

# --- Configuración ---
# Credentials are read from environment variables — see .env.example
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "sentiment_db")
COLLECTION = "twitter_comments"

MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "localhost"),
    "port": int(os.environ.get("MYSQL_PORT", 3306)),
    "user": os.environ.get("MYSQL_USER", "sentiment_user"),
    "password": os.environ.get("MYSQL_PASSWORD", ""),
    "database": os.environ.get("DB_NAME", "sentiment_db")
}

MODEL_NAME = "tabularisai/multilingual-sentiment-analysis"
POLL_INTERVAL = 5  # segundos entre polls

# --- Cargar modelo ---
print("[DL] Cargando modelo...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
model.eval()
print(f"[DL] Modelo listo — labels: {model.config.id2label}")

# --- Conexiones ---
mongo_client = MongoClient(MONGO_URI)
collection = mongo_client[DB_NAME][COLLECTION]

mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
mysql_cursor = mysql_conn.cursor()

def predict_sentiment(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
    probs = F.softmax(outputs.logits, dim=-1)
    confidence, predicted_class = torch.max(probs, dim=-1)
    label = model.config.id2label[predicted_class.item()]
    return label, round(confidence.item(), 4)

def insert_mysql(doc, sentiment, confidence):
    sql = """
        INSERT INTO sentiment_results 
        (mongo_id, user_id, comment, sentiment, confidence, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    values = (
        str(doc["_id"]),
        doc["user_id"],
        doc["comment"],
        sentiment,
        confidence,
        doc["timestamp"].replace(tzinfo=None)
    )
    mysql_cursor.execute(sql, values)
    mysql_conn.commit()

def process_pending():
    pending = list(collection.find({"processed": False}))
    if not pending:
        return 0

    for doc in pending:
        sentiment, confidence = predict_sentiment(doc["comment"])

        # Actualizar MongoDB
        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "processed": True,
                "sentiment": sentiment,
                "confidence": confidence,
                "processed_at": datetime.now(timezone.utc)
            }}
        )

        # Insertar en MySQL
        insert_mysql(doc, sentiment, confidence)

        print(f"  [+] '{doc['comment'][:40]}...' → {sentiment} ({confidence})")

    return len(pending)

def main():
    print(f"[DL] Iniciando pipeline — polling cada {POLL_INTERVAL}s...")
    while True:
        count = process_pending()
        if count > 0:
            print(f"[DL] Procesados: {count} documentos.")
        else:
            print("[DL] Sin pendientes, esperando...")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
