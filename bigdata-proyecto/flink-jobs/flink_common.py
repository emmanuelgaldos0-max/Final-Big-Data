"""
flink_common.py
===============
Utilidades compartidas por los 5 jobs PyFlink (Apache Flink real, DataStream API).

Aquí se centraliza:
- Construcción del entorno de ejecución (StreamExecutionEnvironment) y carga del
  jar del conector Kafka (flink-sql-connector-kafka).
- Fuentes y sumideros Kafka (KafkaSource / KafkaSink) que trabajan con JSON como
  String (patrón robusto en PyFlink: el tipo en el grafo es STRING y cada función
  parsea/serializa el JSON internamente).
- Sumidero de archivos (FileSink) que persiste el stream clasificado a JSONL para
  que los jobs batch de Spark lo lean como dataset histórico real.
- Cliente Redis (la escritura a Redis se hace como efecto lateral dentro de
  RichMapFunction / ProcessFunction, ya que PyFlink no expone SinkFunction en Python).

Configuración por variables de entorno (mismos defaults que el resto del proyecto):
  KAFKA_BOOTSTRAP        -> brokers Kafka            (default: localhost:9092)
  REDIS_HOST / REDIS_PORT-> Redis                    (default: localhost:6379)
  FLINK_CONNECTOR_JARS   -> ruta(s) a los jar de conectores, separadas por ':'
  CLASSIFIED_PATH        -> carpeta de salida JSONL  (default: ../data/classified)
"""

import os
import sys
import json
from datetime import datetime, timezone
from pathlib import Path

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.connectors.file_system import (
    FileSink,
    OnCheckpointRollingPolicy,
    OutputFileConfig,
)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CLASSIFIED_PATH = os.environ.get(
    "CLASSIFIED_PATH", os.path.join(_REPO_ROOT, "data", "classified")
)


def _as_file_uri(path: str) -> str:
    """
    Convierte una ruta local en URI file:// absoluta y VÁLIDA (espacios → %20, etc.).
    Flink valida la URI al registrar jars/sinks; un 'file://' crudo con espacios falla.
    """
    return Path(os.path.abspath(path)).as_uri()


def _connector_jar_urls() -> list:
    """Lee FLINK_CONNECTOR_JARS y devuelve la lista de URLs file:// de los jars."""
    raw = os.environ.get("FLINK_CONNECTOR_JARS", "").strip()
    urls = []
    for part in raw.split(os.pathsep):
        part = part.strip()
        if part:
            urls.append(_as_file_uri(part))
    return urls


def get_env(parallelism: int = None, checkpoint_ms: int = None) -> StreamExecutionEnvironment:
    """
    Crea el entorno PyFlink. Añade el jar del conector Kafka si FLINK_CONNECTOR_JARS
    está definido (necesario al ejecutar en modo local/minicluster; en un cluster
    con el jar en /opt/flink/lib no hace falta).
    """
    env = StreamExecutionEnvironment.get_execution_environment()

    jars = _connector_jar_urls()
    if jars:
        env.add_jars(*jars)

    if parallelism:
        env.set_parallelism(parallelism)

    # Intérprete de Python para las UDF.
    #  - En CLUSTER (multi-nodo): NO lo fijamos aquí, porque la ruta del venv difiere en
    #    cada máquina. Cada TaskManager usa su propio `python.executable` de su config.yaml,
    #    así el TM de Ubuntu y el de la Mac usan cada uno su venv local.
    #  - En LOCAL (minicluster): exportar PYFLINK_PYTHON con la ruta del venv (Flink invoca
    #    "python" por defecto, que en Ubuntu no existe — solo python3/el del venv).
    python_exec = os.environ.get("PYFLINK_PYTHON")
    if python_exec:
        env.set_python_executable(python_exec)

    # El FileSink en formato fila finaliza los part-files en cada checkpoint;
    # sin checkpointing los archivos quedan como .inprogress y Spark no los lee.
    if checkpoint_ms:
        env.enable_checkpointing(checkpoint_ms)

    return env


# ---------------------------------------------------------------------------
# Kafka source / sink (JSON como String)
# ---------------------------------------------------------------------------
def kafka_source(topics, group_id: str, from_earliest: bool = False) -> KafkaSource:
    if isinstance(topics, str):
        topics = [topics]
    offsets = (
        KafkaOffsetsInitializer.earliest()
        if from_earliest
        else KafkaOffsetsInitializer.latest()
    )
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(*topics)
        .set_group_id(group_id)
        .set_starting_offsets(offsets)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def kafka_sink(topic: str) -> KafkaSink:
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def jsonl_file_sink(base_path: str = None) -> FileSink:
    """
    Sumidero de archivos: escribe cada elemento (línea JSON) a JSONL.
    Spark lee esta carpeta de forma recursiva como dataset histórico real.
    """
    base_path = base_path or CLASSIFIED_PATH
    output_cfg = (
        OutputFileConfig.builder()
        .with_part_prefix("classified")
        .with_part_suffix(".json")
        .build()
    )
    return (
        FileSink.for_row_format(_as_file_uri(base_path), Encoder.simple_string_encoder("UTF-8"))
        # Finaliza los part-files en cada checkpoint: así Spark ve datos nuevos cada
        # ~30s en vez de esperar a los 128MB/60s de la política por defecto.
        .with_rolling_policy(OnCheckpointRollingPolicy.build())
        .with_output_file_config(output_cfg)
        .build()
    )


# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
def redis_client(host=None, port=None):
    """
    Cliente Redis. Se instancia dentro de open()/process() de cada función EN EL
    TaskManager. En cluster, host/port deben pasarse explícitos (capturados al construir
    el job en el cliente), porque el env del TaskManager remoto no tiene REDIS_HOST y
    caería a 'localhost' (apuntando al nodo equivocado).
    """
    import redis

    return redis.Redis(
        host=host or REDIS_HOST, port=port or REDIS_PORT, db=0, decode_responses=True
    )


# ---------------------------------------------------------------------------
# Helpers de tiempo / parsing
# ---------------------------------------------------------------------------
def parse_iso_to_epoch_ms(ts: str) -> int:
    """Convierte un timestamp ISO-8601 a epoch en milisegundos (0 si falla)."""
    if not ts:
        return 0
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class EventTimestampAssigner(TimestampAssigner):
    """Extrae el event-time desde el campo 'timestamp' del mensaje JSON."""

    def extract_timestamp(self, value, record_timestamp):
        try:
            msg = json.loads(value)
            return parse_iso_to_epoch_ms(msg.get("timestamp", ""))
        except Exception:
            return record_timestamp


def event_time_watermarks(max_out_of_orderness_s: int = 5) -> WatermarkStrategy:
    return WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(max_out_of_orderness_s)
    ).with_timestamp_assigner(EventTimestampAssigner())


# Re-exports para que los jobs importen todo desde aquí
__all__ = [
    "Types",
    "WatermarkStrategy",
    "BOOTSTRAP_SERVERS",
    "REDIS_HOST",
    "REDIS_PORT",
    "CLASSIFIED_PATH",
    "get_env",
    "kafka_source",
    "kafka_sink",
    "jsonl_file_sink",
    "redis_client",
    "parse_iso_to_epoch_ms",
    "now_iso",
    "event_time_watermarks",
]
