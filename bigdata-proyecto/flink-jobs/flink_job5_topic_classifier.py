"""
flink_job5_topic_classifier.py
==============================
JOB FLINK #5: Clasificador de temas políticos en tiempo real (PyFlink — DataStream API)

Nombre: PoliticalTopicStreamClassifier
Qué hace: Enriquece cada evento clasificándolo en subtemas políticos del Perú
          (economía, seguridad, corrupción, derechos humanos, medioambiente, educación)
          mediante un MapFunction evento-a-evento, y mantiene contadores y muestras
          recientes por tema en Redis para el dashboard.
Entrada: Topics Kafka — raw-tweets, raw-comments
Salida:  Redis (topics:counts:*, topics:recent:*) | Topic Kafka — metrics
Capacidad técnica: Enriquecimiento de stream evento-a-evento con clasificación
          multi-etiqueta y actualización de estado en Redis sin latencia de micro-batch.
Por qué Flink: el enriquecimiento por evento mantiene el dashboard al día al instante.
          Spark Structured Streaming agruparía en micro-batches, añadiendo retraso visible.
"""

import json
import os
import re
import sys
import unicodedata

sys.path.insert(0, os.path.dirname(__file__))

from pyflink.datastream.functions import MapFunction

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

INPUT_TOPICS = ["raw-tweets", "raw-comments"]

# Léxico por tema. Palabras SIN acento y en minúscula (el texto se normaliza igual antes
# de comparar). Se incluye un tema "politica" — central en un contexto electoral peruano —
# con nombres de actores, instituciones y términos de campaña, que antes faltaba y mandaba
# casi todo el contenido político a "otros".
TOPIC_KEYWORDS = {
    "politica": ["politica", "politico", "gobierno", "congreso", "presidente", "presidenta",
                 "ministro", "ministra", "elecciones", "eleccion", "electoral", "candidato",
                 "candidata", "campana", "partido", "voto", "votar", "votos", "izquierda",
                 "derecha", "comunista", "caviar", "terruco", "terruqueo", "terrorista",
                 "terrorismo", "sendero", "senderista",
                 "fujimori", "keiko", "castillo", "cerron", "boluarte", "dina", "vizcarra",
                 "toledo", "humala", "fuerza popular", "peru libre", "golpe", "vacancia"],
    "economia": ["economia", "inflacion", "precio", "precios", "sueldo", "sueldos", "trabajo",
                 "empleo", "desempleo", "dolar", "pobreza", "inversion", "impuesto", "impuestos",
                 "tributaria", "gas", "combustible", "canasta", "salario", "recesion"],
    "seguridad": ["crimen", "delincuencia", "policia", "homicidio", "robo", "robos", "extorsion",
                  "narcotrafico", "sicario", "sicariato", "inseguridad", "pandilla", "asesinato",
                  "secuestro", "balacera", "criminalidad", "hampa"],
    "corrupcion": ["corrupto", "corrupcion", "coima", "soborno", "lavado", "fiscalia", "prision",
                   "investigado", "fiscal", "odebrecht", "mafia", "peculado", "impunidad"],
    "derechos_humanos": ["derechos", "libertad", "democracia", "dictadura", "represion",
                         "manifestacion", "protesta", "protestas", "detencion", "tortura",
                         "racismo", "racista", "discriminacion", "xenofobia", "machismo",
                         "feminicidio", "igualdad", "muertos", "fallecidos"],
    "medioambiente": ["clima", "amazonia", "mineria", "contaminacion", "petroleo", "deforestacion",
                      "rio", "rios", "indigena", "indigenas", "territorio", "derrame", "ambiental",
                      "selva"],
    "educacion": ["escuela", "universidad", "maestro", "maestros", "educacion", "estudiante",
                  "estudiantes", "curriculum", "beca", "becas", "colegio", "docente", "sunedu"],
}


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


# Un patrón por tema: palabra completa (\b), ya normalizado sin acentos. Evita falsos
# positivos por subcadena (p.ej. "robo" dentro de "robot", "rios" dentro de "barrios").
_TOPIC_PATTERNS = {
    topic: re.compile(r"\b(?:" + "|".join(re.escape(k) for k in kws) + r")\b")
    for topic, kws in TOPIC_KEYWORDS.items()
}


def classify_topics(text: str) -> list:
    norm = _strip_accents(text.lower())
    found = [topic for topic, pat in _TOPIC_PATTERNS.items() if pat.search(norm)]
    return found if found else ["otros"]


class TopicClassifier(MapFunction):
    def __init__(self, redis_host, redis_port):
        self._redis_host = redis_host
        self._redis_port = redis_port

    def open(self, runtime_context):
        self._redis = redis_client(self._redis_host, self._redis_port)

    def map(self, value: str) -> str:
        msg = json.loads(value)
        text = msg.get("text", "")
        topics = classify_topics(text)

        pipe = self._redis.pipeline()
        for topic in topics:
            pipe.incr(f"topics:counts:{topic}")
            pipe.lpush(
                f"topics:recent:{topic}",
                json.dumps(
                    {"text": text[:80], "source": msg.get("source", ""), "timestamp": now_iso()},
                    ensure_ascii=False,
                ),
            )
            pipe.ltrim(f"topics:recent:{topic}", 0, 19)
        pipe.execute()

        return json.dumps(
            {"post_id": msg.get("post_id", ""), "topics": topics, "classified_at": now_iso()},
            ensure_ascii=False,
        )


def main():
    env = get_env()

    source = kafka_source(INPUT_TOPICS, group_id="flink-job5-topic-classifier")
    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-raw")

    (
        raw.map(TopicClassifier(REDIS_HOST, REDIS_PORT), output_type=Types.STRING())
        .name("topic-classifier")
        .sink_to(kafka_sink("metrics"))
        .name("sink-topic-metrics")
    )

    print("[JOB5] PoliticalTopicStreamClassifier — enriquecimiento multi-etiqueta")
    env.execute("PoliticalTopicStreamClassifier")


if __name__ == "__main__":
    main()
