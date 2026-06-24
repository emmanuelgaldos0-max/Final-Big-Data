"""
producer_reddit.py
===================
Productor Kafka que extrae comentarios de Reddit sobre política peruana
usando la API de Pushshift (pullpush.io) — no requiere credenciales de Reddit.

Topic de destino: raw-comments
Subreddits: r/peru, r/PeruPolitica
"""

import json
import time
import uuid
import os
import sys
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
PUSHSHIFT_URL = "https://api.pullpush.io/reddit/comment/search"

SUBREDDITS = ["peru", "PeruPolitica", "LatinAmerica"]
SEARCH_TERMS = ["elecciones", "presidente", "corrupto", "gobierno", "voto", "congreso"]
TOPIC = "raw-comments"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def fetch_reddit_comments(subreddit: str, query: str, size: int = 50) -> list:
    """Obtiene comentarios de Reddit via Pushshift API."""
    params = {
        "subreddit": subreddit,
        "q": query,
        "size": size,
        "lang": "es",
    }
    try:
        resp = requests.get(PUSHSHIFT_URL, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("data", [])
    except Exception as e:
        print(f"[ERROR] Pushshift API: {e}")
    return []


def build_message(comment: dict) -> dict:
    return {
        "post_id": comment.get("id", str(uuid.uuid4())),
        "text": comment.get("body", ""),
        "source": "reddit",
        "author": comment.get("author", "unknown"),
        "subreddit": comment.get("subreddit", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "original_created_utc": comment.get("created_utc", 0),
        "score": comment.get("score", 0),
        "lang": "es",
        "metadata": {
            "permalink": comment.get("permalink", ""),
            "parent_id": comment.get("parent_id", ""),
        }
    }


def main():
    producer = create_producer()
    print(f"[REDDIT PRODUCER] Conectado a Kafka: {BOOTSTRAP_SERVERS}")
    print(f"[REDDIT PRODUCER] Extrayendo de: {SUBREDDITS}")

    sent = 0
    cycle = 0
    seen = set()   # ids ya enviados -> evita repetir el mismo comentario entre ciclos

    try:
        while True:
            cycle += 1
            print(f"\n[Ciclo {cycle}] Consultando Pushshift API...")

            for subreddit in SUBREDDITS:
                query = SEARCH_TERMS[cycle % len(SEARCH_TERMS)]
                comments = fetch_reddit_comments(subreddit, query, size=25)
                nuevos = 0
                for comment in comments:
                    text = comment.get("body", "").strip()
                    cid = comment.get("id", "")
                    if not text or len(text) < 10 or text == "[deleted]":
                        continue
                    if cid and cid in seen:      # ya lo enviamos antes -> no repetir
                        continue
                    if cid:
                        seen.add(cid)

                    msg = build_message(comment)
                    producer.send(TOPIC, key=msg["post_id"], value=msg)
                    sent += 1
                    nuevos += 1
                    time.sleep(0.05)
                print(f"  r/{subreddit} + '{query}': {len(comments)} recibidos, {nuevos} nuevos (únicos)")

            producer.flush()
            print(f"[REDDIT PRODUCER] Total enviados: {sent} (unicos). Esperando 30s...")
            time.sleep(30)  # intervalo entre ciclos (respeta rate limit)

    except KeyboardInterrupt:
        print(f"\n[REDDIT PRODUCER] Detenido. Total enviados: {sent}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
