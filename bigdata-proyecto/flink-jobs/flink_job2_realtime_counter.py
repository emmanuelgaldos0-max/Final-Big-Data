"""
flink_job2_realtime_counter.py
==============================
JOB FLINK #2: Contador de tendencias en ventana deslizante (PyFlink — DataStream API)

Nombre: SlidingWindowTrendCounter
Qué hace: Cuenta la frecuencia de cada tipo de discurso (hate, terruco, discriminación,
          político) usando una ventana DESLIZANTE de 60 s con slide de 10 s
          (SlidingProcessingTimeWindows). Detecta picos comparando contra el histórico
          reciente y publica las tendencias en Redis para el dashboard.
Entrada: Topic Kafka — classified-hate
Salida:  Redis (metrics:window:trends, metrics:window:peak) | Topic Kafka — metrics
Capacidad técnica: Ventanas deslizantes con estado (stateful sliding windows).
Por qué Flink: las sliding windows con estado y solapamiento son una operación nativa
          de Flink que recalcula el agregado cada 10 s. Spark batch no puede producir
          tendencias con granularidad de segundos sobre un stream continuo.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from pyflink.common import Time
from pyflink.datastream.functions import ProcessAllWindowFunction
from pyflink.datastream.window import SlidingProcessingTimeWindows

from flink_common import (
    REDIS_HOST,
    REDIS_PORT,
    Types,
    WatermarkStrategy,
    get_env,
    kafka_sink,
    kafka_source,
    now_iso,
    redis_client,
)

WINDOW_SECONDS = 60
SLIDE_SECONDS = 10


class TrendWindow(ProcessAllWindowFunction):
    """Agrega todos los eventos de la ventana y escribe tendencias + picos en Redis."""

    def __init__(self, redis_host, redis_port):
        # Capturado en el cliente; viaja al TaskManager (su env no tiene REDIS_HOST).
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis = None

    def _r(self):
        if self._redis is None:
            self._redis = redis_client(self._redis_host, self._redis_port)
        return self._redis

    def process(self, context, elements):
        counts = {"hate": 0, "terruco": 0, "discrimination": 0, "political": 0}
        for value in elements:
            try:
                rec = json.loads(value)
            except Exception:
                continue
            if rec.get("is_hate_speech"):
                counts["hate"] += 1
            if rec.get("is_terruco"):
                counts["terruco"] += 1
            if rec.get("discrimination_types"):
                counts["discrimination"] += 1
            pol = rec.get("political_classification", {})
            if pol.get("polarization_index", 0) > 0.3:
                counts["political"] += 1

        total = sum(counts.values())
        window_data = {
            "timestamp": now_iso(),
            "window_seconds": WINDOW_SECONDS,
            "hate_count": counts["hate"],
            "terruco_count": counts["terruco"],
            "discrimination_count": counts["discrimination"],
            "political_count": counts["political"],
            "total": total,
        }

        r = self._r()
        r.set("metrics:window:trends", json.dumps(window_data), ex=120)

        # Detección de pico: total > 2x el promedio de las últimas ventanas
        r.lpush("metrics:window:history", total)
        r.ltrim("metrics:window:history", 0, 11)  # ~2 min de historial
        history = [int(x) for x in r.lrange("metrics:window:history", 0, -1)]
        avg = sum(history) / max(len(history), 1)
        if total > avg * 2 and total > 5:
            r.set(
                "metrics:window:peak",
                json.dumps(
                    {
                        "detected_at": now_iso(),
                        "current_count": total,
                        "average": round(avg, 1),
                        "multiplier": round(total / max(avg, 1), 1),
                    }
                ),
                ex=300,
            )
            print(f"[JOB2] PICO detectado: {total} eventos (avg {avg:.1f})")

        print(f"[JOB2] Ventana[{WINDOW_SECONDS}s]: {window_data}")
        yield json.dumps(window_data, ensure_ascii=False)


def main():
    env = get_env()

    source = kafka_source("classified-hate", group_id="flink-job2-window-counter")
    classified = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "kafka-classified"
    )

    (
        classified.window_all(
            SlidingProcessingTimeWindows.of(Time.seconds(WINDOW_SECONDS), Time.seconds(SLIDE_SECONDS))
        )
        .process(TrendWindow(REDIS_HOST, REDIS_PORT), output_type=Types.STRING())
        .name("sliding-trends")
        .sink_to(kafka_sink("metrics"))
        .name("sink-metrics")
    )

    print("[JOB2] SlidingWindowTrendCounter — ventana 60s / slide 10s")
    env.execute("SlidingWindowTrendCounter")


if __name__ == "__main__":
    main()
