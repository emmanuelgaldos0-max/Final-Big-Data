"""
flink_job3_latency_monitor.py
=============================
JOB FLINK #3: Monitor de latencia del sistema / SLA (PyFlink — DataStream API)

Nombre: SystemLatencyMonitor
Qué hace: Mide la latencia extremo-a-extremo de cada evento comparando su EVENT-TIME
          (timestamp asignado por el productor, vía WatermarkStrategy) contra el
          PROCESSING-TIME del operador. Mantiene avg/p95, cuenta violaciones del SLA
          (2000 ms) y emite una alerta por cada violación.
Entrada: Topics Kafka — raw-tweets, raw-comments (con campo timestamp)
Salida:  Redis (metrics:latency, alerts:sla) | Topic Kafka — alerts
Capacidad técnica: Distinción nativa entre event-time y processing-time + detección de
          violaciones de SLA en tiempo real.
Por qué Flink: Flink modela event-time y processing-time como conceptos de primera clase
          (timestamps + watermarks). Un job batch de Spark no puede detectar en vivo que
          un evento llegó tarde respecto a su SLA.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from pyflink.datastream.functions import ProcessFunction

from flink_common import (
    REDIS_HOST,
    REDIS_PORT,
    Types,
    event_time_watermarks,
    get_env,
    kafka_sink,
    kafka_source,
    now_iso,
    redis_client,
)

SLA_MS = 2000
INPUT_TOPICS = ["raw-tweets", "raw-comments"]


class LatencyMonitor(ProcessFunction):
    def __init__(self, redis_host, redis_port):
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis = None
        self._history = []
        self._processed = 0
        self._violations = 0

    def _r(self):
        if self._redis is None:
            self._redis = redis_client(self._redis_host, self._redis_port)
        return self._redis

    def process_element(self, value, ctx: "ProcessFunction.Context"):
        # event-time (ms) asignado por el TimestampAssigner vs processing-time del operador
        event_ts = ctx.timestamp()
        proc_ts = ctx.timer_service().current_processing_time()
        latency_ms = float(proc_ts - event_ts) if event_ts else 0.0

        self._history.append(latency_ms)
        if len(self._history) > 1000:
            self._history.pop(0)
        self._processed += 1

        avg = sum(self._history) / len(self._history)
        if len(self._history) > 20:
            p95 = sorted(self._history)[int(len(self._history) * 0.95)]
        else:
            p95 = latency_ms

        r = self._r()
        r.set(
            "metrics:latency",
            json.dumps(
                {
                    "avg_latency_ms": round(avg, 2),
                    "p95_latency_ms": round(p95, 2),
                    "last_latency_ms": round(latency_ms, 2),
                    "sla_violations": self._violations,
                    "sla_violation_rate": round(self._violations / max(self._processed, 1) * 100, 2),
                    "processed": self._processed,
                    "updated_at": now_iso(),
                }
            ),
            ex=60,
        )

        if latency_ms > SLA_MS:
            self._violations += 1
            try:
                msg = json.loads(value)
                post_id = msg.get("post_id", "")
            except Exception:
                post_id = ""
            alert = {
                "type": "SLA_VIOLATION",
                "latency_ms": round(latency_ms, 2),
                "sla_ms": SLA_MS,
                "post_id": post_id,
                "timestamp": now_iso(),
            }
            r.lpush("alerts:sla", json.dumps(alert))
            r.ltrim("alerts:sla", 0, 49)
            print(f"[JOB3] SLA violado: {latency_ms:.0f}ms > {SLA_MS}ms")
            yield json.dumps(alert, ensure_ascii=False)  # solo las violaciones van al topic


def main():
    env = get_env()

    source = kafka_source(INPUT_TOPICS, group_id="flink-job3-latency-monitor")
    raw = env.from_source(source, event_time_watermarks(), "kafka-raw")

    (
        raw.process(LatencyMonitor(REDIS_HOST, REDIS_PORT), output_type=Types.STRING())
        .name("latency-monitor")
        .sink_to(kafka_sink("alerts"))
        .name("sink-sla-alerts")
    )

    print(f"[JOB3] SystemLatencyMonitor — SLA {SLA_MS}ms (event-time vs processing-time)")
    env.execute("SystemLatencyMonitor")


if __name__ == "__main__":
    main()
