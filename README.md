# Real-Time Sentiment Analysis Pipeline (Big Data + Deep Learning)

An end-to-end pipeline that simulates streaming social media comments, ingests them in real time with Kafka, stores them in MongoDB, classifies sentiment with a HuggingFace Deep Learning model running on PyTorch, and visualizes results in a live Metabase dashboard.

> **Note:** Credentials shown in this repo are placeholders. Real values are provided via environment variables — see `.env.example`. No real passwords or secrets are included here.

## Architecture

```
Clients (client.py) → Kafka → Consumer (consumer.py) → MongoDB (data lake)
                                                              ↓
                                              DL Pipeline (dl_pipeline.py)
                                                              ↓
                                                    MySQL (data warehouse) → Metabase
```

The pipeline has three stages:

1. **Streaming ingestion** — simulated users send comments to a Kafka topic; a consumer picks them up and stores the raw messages in MongoDB, acting as a data lake.
2. **Deep Learning processing** — a script polls MongoDB for unprocessed documents, runs each comment through a pretrained sentiment classification model, and writes the structured result to MySQL, acting as a data warehouse.
3. **Visualization** — Metabase connects to MySQL and displays real-time dashboards of sentiment distribution, volume per user, and model confidence.

## Components

| File | Role |
|---|---|
| `client.py` | Kafka producer — simulates users sending comments at random intervals |
| `consumer.py` | Kafka consumer — persists raw messages into MongoDB |
| `dl_pipeline.py` | Polls MongoDB, runs sentiment inference, writes results to MySQL |
| `docker-compose.yml` | Spins up Kafka, MongoDB, MySQL, and Metabase as containers |
| `.env.example` | Template for required environment variables (copy to `.env`, fill in your own values) |

## The Deep Learning model

- **Model:** `tabularisai/multilingual-sentiment-analysis` (HuggingFace)
- **Architecture:** fine-tuned BERT for sentiment classification
- **Output classes:** Very Negative, Negative, Neutral, Positive, Very Positive
- **Implementation:** raw PyTorch with `AutoTokenizer` and `AutoModelForSequenceClassification` — no high-level `transformers.pipeline()` wrapper, to have direct control over tokenization, inference, and softmax confidence extraction

## How to run it

1. Copy `.env.example` to `.env` and fill in your own values (Kafka broker IP, MySQL credentials)
2. Start all services:
   ```bash
   docker compose up -d
   ```
3. In separate terminals, with your Python virtual environment active:
   ```bash
   python consumer.py      # Terminal 1 — listens to Kafka, writes to MongoDB
   python client.py        # Terminal 2 — simulates comments being sent
   python dl_pipeline.py   # Terminal 3 — polls MongoDB, runs inference, writes to MySQL
   ```
4. Open Metabase at `http://localhost:3000` and connect it to the MySQL database to build dashboards

## Dashboards implemented in Metabase

1. **Sentiment distribution** — pie chart of comment counts by sentiment category
2. **Comments per user** — bar chart of message volume per simulated user
3. **Average confidence per sentiment** — bar chart of the model's mean confidence score by category

## What I learned building this

- Designing a full pipeline across four different services (Kafka, MongoDB, MySQL, Metabase) that each play a distinct role — streaming, raw storage, structured storage, and visualization — instead of coupling everything into one script.
- Running Deep Learning inference directly with PyTorch's low-level API instead of a high-level wrapper, to understand exactly what happens between raw text and a confidence score.
- Setting up a local development environment from scratch with WSL2 and native Docker Engine on Linux (rather than Docker Desktop), including recovering from a full Windows reinstall after an early configuration failure — a reminder that infrastructure work rarely goes perfectly on the first try, and that being able to diagnose and rebuild is as valuable as getting it right the first time.
- Replacing hardcoded credentials with environment variables before publishing the code — a basic but essential practice for any project that touches real infrastructure.

## Honest note on scope

This project focuses on **data engineering and pipeline architecture** as much as it does on the Deep Learning model itself. The sentiment classification uses a pretrained, off-the-shelf model rather than one trained from scratch — the value demonstrated here is in designing and running a real streaming pipeline end to end, not in model development.
