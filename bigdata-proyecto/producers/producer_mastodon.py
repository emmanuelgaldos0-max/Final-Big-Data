"""
producer_mastodon.py
====================
Productor Kafka que extrae publicaciones en vivo de Mastodon (red social federada)
por hashtags relacionados con la política peruana, usando la API pública de
timelines por etiqueta — NO requiere credenciales ni token.

Fuente:  https://<instancia>/api/v1/timelines/tag/<hashtag>
Topic de destino: raw-tweets   (Mastodon es microblog, tipo tweet)
"""

import json
import os
import re
import time
import uuid
import html
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
INSTANCE = os.environ.get("MASTODON_INSTANCE", "https://mastodon.social")
TOPIC = "raw-tweets"

# Hashtags enfocados en Perú / política peruana (los más limpios según prueba)
HASHTAGS = ["PeruPolitica", "peru", "Boluarte", "DinaBoluarte", "Fujimori",
            "Castillo", "PeruElecciones", "Lima", "congresoPeru"]

# Idiomas aceptados (Mastodon trae el campo 'language'); priorizamos español
LANGS_OK = {"es", None, "", "und"}

_TAG_RE = re.compile(r"<[^>]+>")


def clean_html(raw: str) -> str:
    """Quita etiquetas HTML y normaliza entidades del contenido de Mastodon."""
    txt = _TAG_RE.sub(" ", raw or "")
    txt = html.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def fetch_tag(tag: str, limit: int = 20) -> list:
    url = f"{INSTANCE}/api/v1/timelines/tag/{tag}"
    try:
        resp = requests.get(url, params={"limit": limit}, timeout=15,
                            headers={"User-Agent": "bigdata-proyecto/1.0"})
        if resp.status_code == 200:
            return resp.json()
        print(f"[ERROR] #{tag}: HTTP {resp.status_code}")
    except Exception as e:
        print(f"[ERROR] #{tag}: {e}")
    return []


def build_message(post: dict, tag: str) -> dict:
    acct = post.get("account", {})
    return {
        "post_id": "mstdn_" + str(post.get("id", uuid.uuid4())),
        "text": clean_html(post.get("content", "")),
        "source": "mastodon",
        "author": acct.get("username", "unknown"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lang": post.get("language") or "es",
        "metadata": {
            "hashtag": tag,
            "url": post.get("url", ""),
            "favourites": post.get("favourites_count", 0),
            "reblogs": post.get("reblogs_count", 0),
            "created_at": post.get("created_at", ""),
        },
    }


def main():
    producer = create_producer()
    print(f"[MASTODON PRODUCER] Kafka: {BOOTSTRAP_SERVERS} | instancia: {INSTANCE}")
    print(f"[MASTODON PRODUCER] Hashtags: {HASHTAGS}")

    seen = set()   # ids ya enviados -> sin repetir entre ciclos
    sent = 0
    cycle = 0

    try:
        while True:
            cycle += 1
            print(f"\n[Ciclo {cycle}] Consultando Mastodon...")
            for tag in HASHTAGS:
                posts = fetch_tag(tag, limit=20)
                nuevos = 0
                for post in posts:
                    pid = post.get("id")
                    if not pid or pid in seen:
                        continue
                    if (post.get("language") or "es") not in LANGS_OK:
                        continue
                    text = clean_html(post.get("content", ""))
                    if len(text) < 10:
                        continue
                    seen.add(pid)
                    msg = build_message(post, tag)
                    producer.send(TOPIC, key=msg["post_id"], value=msg)
                    sent += 1
                    nuevos += 1
                    time.sleep(0.05)
                print(f"  #{tag}: {len(posts)} recibidos, {nuevos} nuevos (únicos)")
                time.sleep(1)  # cortesía con la API entre hashtags
            producer.flush()
            print(f"[MASTODON PRODUCER] Total enviados: {sent} (únicos). Esperando 45s...")
            time.sleep(45)
    except KeyboardInterrupt:
        print(f"\n[MASTODON PRODUCER] Detenido. Total enviados: {sent}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
