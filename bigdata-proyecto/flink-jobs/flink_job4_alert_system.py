"""
flink_job4_alert_system.py
==========================
JOB FLINK #4: Sistema de alertas por ráfaga (PyFlink — DataStream API)

Nombre: DynamicAlertSystem
Qué hace: Detecta RÁFAGAS de contenido tóxico usando una ventana TUMBLING (no solapada)
          de 30 s. Si en la ventana hay >= HATE_THRESHOLD mensajes de odio/terruqueo,
          emite una alerta de burst al topic Kafka 'alerts' y a Redis.
Entrada: Topic Kafka — classified-hate
Salida:  Topic Kafka — alerts | Redis (alerts:bursts, alerts:latest)
Capacidad técnica: Tumbling windows con agregación de estado por ventana.
Por qué Flink: las tumbling windows con conteo de estado por ventana son nativas de
          Flink. Detectar "N eventos en 30 s" requiere mantener estado entre eventos en
          tiempo real, algo imposible en un job batch de Spark.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from pyflink.common import Time
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows

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

WINDOW_SECONDS = 30
HATE_THRESHOLD = 5  # alerta si hay >=5 mensajes tóxicos en la ventana


class BurstDetector(ProcessWindowFunction):
    def __init__(self, redis_host, redis_port):
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis = None

    def _r(self):
        if self._redis is None:
            self._redis = redis_client(self._redis_host, self._redis_port)
        return self._redis

    def process(self, key, context, elements):
        hate = terruco = 0
        for value in elements:
            try:
                rec = json.loads(value)
            except Exception:
                continue
            if rec.get("is_hate_speech"):
                hate += 1
            if rec.get("is_terruco"):
                terruco += 1

        total_toxic = hate + terruco
        if total_toxic >= HATE_THRESHOLD:
            try:
                win = context.window()
                window_end = getattr(win, "end", None) or win.get_end()
            except Exception:
                window_end = None
            alert = {
                "type": "BURST_DETECTED",
                "window_end": window_end,
                "hate_count": hate,
                "terruco_count": terruco,
                "total_toxic": total_toxic,
                "threshold": HATE_THRESHOLD,
                "window_seconds": WINDOW_SECONDS,
                "detected_at": now_iso(),
            }
            r = self._r()
            r.lpush("alerts:bursts", json.dumps(alert, ensure_ascii=False))
            r.ltrim("alerts:bursts", 0, 49)
            r.set("alerts:latest", json.dumps(alert, ensure_ascii=False), ex=300)
            print(f"[JOB4] ALERTA ráfaga: {total_toxic} tóxicos en {WINDOW_SECONDS}s")
            yield json.dumps(alert, ensure_ascii=False)


def main():
    env = get_env()

    source = kafka_source("classified-hate", group_id="flink-job4-alert-system")
    classified = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-classified")

    (
        classified.key_by(lambda v: "all", key_type=Types.STRING())
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .process(BurstDetector(REDIS_HOST, REDIS_PORT), output_type=Types.STRING())
        .name("burst-detector")
        .sink_to(kafka_sink("alerts"))
        .name("sink-burst-alerts")
    )

    print(f"[JOB4] DynamicAlertSystem — umbral {HATE_THRESHOLD} en {WINDOW_SECONDS}s")
    env.execute("DynamicAlertSystem")


if __name__ == "__main__":
    main()
