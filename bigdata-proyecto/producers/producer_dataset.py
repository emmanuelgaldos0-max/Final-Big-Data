"""
producer_dataset.py
====================
Productor Kafka PRINCIPAL: reproduce a ALTO VOLUMEN el corpus de DATOS REALES
descargado de múltiples orígenes públicos (ver data/fetch_real_datasets.py) y lo
emite a los topics raw-tweets / raw-comments simulando ingesta en tiempo real.

Por qué así:
  - Fuente real y de varios orígenes: redes peruanas, noticias Perú y datasets de
    discurso de odio en español (NO datos sintéticos).
  - Cada emisión lleva un post_id ÚNICO (uuid) y un timestamp fresco, de modo que el
    pipeline no deduplica y el throughput es realmente alto (miles de msg/min),
    aunque el catálogo de textos reales se recicle.
  - Throughput controlable y productor optimizado (batching + linger + compresión).

Prioridad de dataset:  data/corpus_real.jsonl  ->  data/sample_data.jsonl  ->  SAMPLE_MESSAGES
(Si no existe corpus_real.jsonl, ejecuta primero:  python data/fetch_real_datasets.py)

Topics de destino: raw-tweets (twitter/mastodon), raw-comments (reddit/news/otros).

Configuración por variables de entorno:
  KAFKA_BOOTSTRAP   brokers Kafka            (default: localhost:9092)
  PRODUCER_RATE     mensajes/segundo; 0 = máximo  (default: 200)
"""

import argparse
import json
import math
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
_DATA = os.path.join(os.path.dirname(__file__), "..", "data")
CORPUS_REAL = os.path.join(_DATA, "corpus_real.jsonl")
SAMPLE_PATH = os.path.join(_DATA, "sample_data.jsonl")

# Respaldo mínimo si no hay ningún dataset en disco.
SAMPLE_MESSAGES = [
    {"text": "Ese terruco comunista quiere expropiar todas las empresas del país", "source": "twitter"},
    {"text": "Apoyamos la democracia y el estado de derecho en el Perú", "source": "twitter"},
    {"text": "Los caviares siempre defienden a los narcoterroristas", "source": "reddit"},
    {"text": "El libre mercado es la única salida para el desarrollo del país", "source": "twitter"},
    {"text": "Ese serrano que tenemos de presidente no sabe ni gobernar", "source": "twitter"},
]

TWEET_SOURCES = {"twitter", "mastodon"}


def create_producer() -> KafkaProducer:
    """Productor optimizado para throughput (batching + linger + compresión)."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,
        retries=3,
        linger_ms=50,
        batch_size=64 * 1024,
        compression_type="gzip",
        buffer_memory=64 * 1024 * 1024,
    )


def load_dataset() -> tuple:
    """Carga el mejor dataset disponible. Devuelve (registros, nombre_fuente)."""
    for path, name in ((CORPUS_REAL, "corpus_real (multi-origen REAL)"),
                       (SAMPLE_PATH, "sample_data")):
        if os.path.exists(path):
            recs = []
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            recs.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
            if recs:
                return recs, f"{name} [{path}]"
    return SAMPLE_MESSAGES, "SAMPLE_MESSAGES (respaldo en memoria)"


def build_message(raw: dict) -> dict:
    """Mensaje enriquecido para Kafka, con post_id único y timestamp fresco."""
    return {
        "post_id": uuid.uuid4().hex,                     # único por emisión
        "original_id": raw.get("id", ""),                # trazabilidad al dato real
        "text": raw.get("text", raw.get("content", "")),
        "source": raw.get("source", "unknown"),
        "origin": raw.get("origin", ""),                 # dataset de procedencia
        "author": raw.get("author", f"user_{random.randint(1000, 999999)}"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lang": "es",
        "metadata": {
            "likes": random.randint(0, 5000),
            "retweets": random.randint(0, 1500),
            "subreddit": raw.get("subreddit", ""),
            "gold_label": raw.get("label", ""),
        },
    }


def main():
    ap = argparse.ArgumentParser(description="Productor de datos reales a alto volumen")
    ap.add_argument("--rate", type=float,
                    default=float(os.environ.get("PRODUCER_RATE", "200")),
                    help="mensajes/segundo (0 = máximo)")
    ap.add_argument("--burst", type=int, default=0, help="emite N y termina")
    ap.add_argument("--vary", action="store_true",
                    help="caudal REALISTA: la tasa fluctúa alrededor de --rate "
                         "(onda lenta tipo 'hora del día' + ruido + ráfagas/valles)")
    args = ap.parse_args()

    producer = create_producer()
    dataset, source_name = load_dataset()

    print(f"[PRODUCER] Kafka: {BOOTSTRAP_SERVERS}")
    print(f"[PRODUCER] Dataset: {len(dataset):,} textos reales · fuente: {source_name}")
    modo = "MÁXIMO" if not args.rate else (f"~{args.rate:.0f} msg/s" + (" VARIABLE" if args.vary else " constante"))
    print(f"[PRODUCER] Tasa objetivo: {modo} · Ctrl+C para detener\n")

    rate = args.rate
    sent = 0
    pos = 0
    start = time.time()
    last_report = start

    def cur_rate():
        """Tasa instantánea (msg/s). Con --vary fluctúa de forma realista alrededor de
        --rate; sin --vary es constante. NO altera el contenido, solo el RITMO de envío."""
        if not rate:
            return 0                               # 0 = máximo (sin control de tasa)
        if not args.vary:
            return rate
        t = time.time() - start
        # onda lenta tipo "hora del día" (~88s de periodo) entre ~0.45x y ~1.25x
        r = rate * (0.85 + 0.40 * math.sin(t / 14.0)) * random.uniform(0.80, 1.20)
        p = random.random()
        if p < 0.03:                               # ráfaga "viral" ocasional
            r *= random.uniform(1.8, 2.6)
        elif p < 0.06:                             # valle ocasional
            r *= random.uniform(0.30, 0.55)
        return max(10.0, r)

    try:
        random.shuffle(dataset)
        while True:
            cr = cur_rate()
            batch = max(1, int(cr / 20)) if cr else 2000   # ~20 lotes/seg
            t_batch = time.time()
            for _ in range(batch):
                if pos >= len(dataset):           # se agotó el catálogo -> rebarajar
                    random.shuffle(dataset)
                    pos = 0
                raw = dataset[pos]; pos += 1
                msg = build_message(raw)
                topic = "raw-tweets" if msg["source"] in TWEET_SOURCES else "raw-comments"
                producer.send(topic, key=msg["post_id"], value=msg)
                sent += 1
                if args.burst and sent >= args.burst:
                    break

            if cr:                                # control de tasa por lote
                target_dt = batch / cr
                elapsed = time.time() - t_batch
                if elapsed < target_dt:
                    time.sleep(target_dt - elapsed)

            now = time.time()
            if now - last_report >= 2.0:
                thr = sent / (now - start)
                print(f"[{datetime.now():%H:%M:%S}] enviados={sent:,} · throughput={thr:,.0f} msg/s")
                last_report = now

            if args.burst and sent >= args.burst:
                break
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        elapsed = max(time.time() - start, 1e-6)
        print(f"\n[PRODUCER] Detenido. Total={sent:,} en {elapsed:,.1f}s (media {sent/elapsed:,.0f} msg/s)")


if __name__ == "__main__":
    main()
