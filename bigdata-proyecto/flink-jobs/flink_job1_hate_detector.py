"""
flink_job1_hate_detector.py
============================
JOB FLINK #1: Detección de discurso de odio en tiempo real (PyFlink — DataStream API)

Nombre: HateSpeechStreamDetector
Qué hace: Consume eventos de raw-tweets y raw-comments con un KafkaSource de Flink,
          aplica el pipeline NLP evento-a-evento dentro de un RichMapFunction y:
            - publica el resultado enriquecido en el topic classified-hate (KafkaSink),
            - persiste cada evento clasificado a JSONL (FileSink) para los jobs Spark,
            - actualiza contadores y feed en Redis para el dashboard.
Entrada: Topics Kafka — raw-tweets, raw-comments
Salida:  Topic Kafka classified-hate | JSONL en data/classified/ | Redis (hate:live, metrics:*)
Capacidad técnica: Procesamiento en streaming evento-a-evento (true streaming) con
          enriquecimiento NLP y fan-out a múltiples sumideros.
Por qué Flink y no Spark: requiere latencia < 2 s por evento. Spark Structured Streaming
          procesa por micro-batches, lo que añade latencia; Flink procesa cada evento al
          llegar. Además el FileSink event-time alimenta la capa batch de Spark.
"""

import json
import os
import sys
import time

# Permitir importar el módulo NLP del repo y flink_common
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from pyflink.datastream.functions import MapFunction

from flink_common import (
    REDIS_HOST,
    REDIS_PORT,
    Types,
    event_time_watermarks,
    get_env,
    jsonl_file_sink,
    kafka_sink,
    kafka_source,
    now_iso,
    parse_iso_to_epoch_ms,
    redis_client,
)

INPUT_TOPICS = ["raw-tweets", "raw-comments"]
OUTPUT_TOPIC = "classified-hate"


class HateClassifier(MapFunction):
    """Aplica el pipeline NLP a cada evento y actualiza Redis como efecto lateral."""

    def __init__(self, redis_host, redis_port):
        # Capturado al construir el job en el cliente; viaja serializado al TaskManager.
        self._redis_host = redis_host
        self._redis_port = redis_port

    def open(self, runtime_context):
        # Import diferido: se ejecuta en cada TaskManager al iniciar la tarea.
        # En cluster, el módulo se envía con -pyfs y queda plano (nlp_pipeline);
        # en local es el paquete nlp.nlp_pipeline. Soportamos ambos.
        try:
            from nlp.nlp_pipeline import full_analysis
        except ImportError:
            from nlp_pipeline import full_analysis

        self._analyze = full_analysis
        self._redis = redis_client(self._redis_host, self._redis_port)

    def _update_redis(self, result: dict):
        r = self._redis
        pipe = r.pipeline()

        if result["is_hate_speech"] or result["is_terruco"]:
            pipe.lpush(
                "hate:live",
                json.dumps(
                    {
                        "text": result["text_preview"],
                        "toxicity": result["toxicity_score"],
                        "types": result["discrimination_types"],
                        "is_terruco": result["is_terruco"],
                        "source": result.get("source", ""),
                        "origin": result.get("origin", ""),
                        "processed_at": result["processed_at"],
                    },
                    ensure_ascii=False,
                ),
            )
            pipe.ltrim("hate:live", 0, 99)

        pipe.incr("metrics:total_processed")
        if result["is_hate_speech"]:
            pipe.incr("metrics:hate_count")
        if result["is_terruco"]:
            pipe.incr("metrics:terruco_count")
        if result["discrimination_types"]:
            pipe.incr("metrics:discrimination_count")

        # Volumen por procedencia real (alimenta el panel "fuentes" del dashboard)
        src = result.get("source") or "unknown"
        pipe.incr(f"metrics:source:{src}")
        origin = result.get("origin")
        if origin:
            pipe.incr(f"metrics:origin:{origin}")

        # Throughput por ventana de 60s (clave efímera que lee el dashboard)
        bucket = int(time.time() // 60)
        pipe.incr(f"metrics:throughput:{bucket}")
        pipe.expire(f"metrics:throughput:{bucket}", 120)

        # Serie temporal REAL por segundo y POR TIPO (medida en el servidor): un hash por
        # segundo con conteo de total/odio/terruqueo/discriminación/político. El dashboard
        # la lee para graficar la evolución de la detección (últimos seg/min) sin jitter.
        sec = int(time.time())
        rk = f"metrics:rate:{sec}"
        pipe.hincrby(rk, "total", 1)
        if result["is_hate_speech"]:
            pipe.hincrby(rk, "hate", 1)
        if result["is_terruco"]:
            pipe.hincrby(rk, "terruco", 1)
        if result["discrimination_types"]:
            pipe.hincrby(rk, "discrim", 1)
        if result.get("political_classification", {}).get("polarization_index", 0) > 0.3:
            pipe.hincrby(rk, "political", 1)
        pipe.expire(rk, 400)            # ~6.5 min de historia (ventana máx. 5 min)
        pipe.execute()

    def map(self, value: str) -> str:
        msg = json.loads(value)
        text = msg.get("text", "")

        analysis = self._analyze(text, msg.get("source", "unknown"), msg.get("post_id", ""))

        # Latencia extremo-a-extremo: desde el timestamp del productor hasta ahora.
        event_ms = parse_iso_to_epoch_ms(msg.get("timestamp", ""))
        latency_ms = round(time.time() * 1000 - event_ms, 2) if event_ms else 0.0

        # Registro enriquecido y completo (incluye author/timestamp para Spark batch).
        record = {
            **analysis,
            "text": text,
            "author": msg.get("author", ""),
            "origin": msg.get("origin", ""),
            "timestamp": msg.get("timestamp", ""),
            "processed_at": now_iso(),
            "latency_ms": latency_ms,
        }

        try:
            self._update_redis(record)
        except Exception as exc:  # Redis no debe tumbar el stream
            print(f"[JOB1][REDIS-ERROR] {exc}")

        return json.dumps(record, ensure_ascii=False)


def main():
    # Checkpointing cada 30s: necesario para que el FileSink finalice los .json
    env = get_env(checkpoint_ms=30_000)

    source = kafka_source(INPUT_TOPICS, group_id="flink-job1-hate-detector")
    raw = env.from_source(
        source, watermark_strategy=event_time_watermarks(), source_name="kafka-raw"
    )

    # REDIS_HOST/PORT se capturan AQUÍ (cliente) y viajan al TaskManager dentro de la función.
    classified = raw.map(
        HateClassifier(REDIS_HOST, REDIS_PORT), output_type=Types.STRING()
    ).name("nlp-classify")

    # Sumidero 1: topic classified-hate (lo consumen Job2, Job4 y Spark vía Kafka)
    classified.sink_to(kafka_sink(OUTPUT_TOPIC)).name("sink-classified-hate")

    # Sumidero 2 (OPCIONAL): JSONL en disco. En cluster multi-nodo NO se usa (Spark lee de
    # Kafka, opción A), porque el FileSink escribiría en el disco local de cada TaskManager.
    # Actívalo solo en ejecución single-node: CLASSIFIED_SINK_JSONL=1
    if os.environ.get("CLASSIFIED_SINK_JSONL", "0") == "1":
        classified.sink_to(jsonl_file_sink()).name("sink-jsonl")
        print("[JOB1] FileSink JSONL ACTIVADO (single-node)")

    print("[JOB1] HateSpeechStreamDetector — raw-tweets,raw-comments -> classified-hate + Redis")
    env.execute("HateSpeechStreamDetector")


if __name__ == "__main__":
    main()
