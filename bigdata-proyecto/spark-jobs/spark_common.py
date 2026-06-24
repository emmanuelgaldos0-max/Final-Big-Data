"""
spark_common.py
===============
Utilidades compartidas por los 5 jobs batch de Spark.

- get_spark(): crea la SparkSession apuntando al Spark master del cluster.
- CLASSIFIED_SCHEMA: esquema del registro clasificado que escribe el Job Flink #1
  (FileSink JSONL en data/classified/). Es la MISMA estructura que produce
  nlp.nlp_pipeline.full_analysis + los campos author/timestamp/processed_at.
- load_classified(): lee ese JSONL real de forma recursiva. Si todavía no existen
  datos (el stream no ha corrido), genera un dataset SINTÉTICO de demostración
  reutilizando el pipeline NLP real, y lo deja claramente registrado en el log
  (`source = synthetic`) para no falsear la procedencia.

Variables de entorno:
  SPARK_MASTER     -> URL del Spark master   (default: spark://localhost:7077)
  CLASSIFIED_PATH  -> carpeta JSONL de Flink (default: ../data/classified)
  OUTPUT_PATH      -> carpeta de reportes     (default: ../data/reports)
"""

import glob
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://localhost:7077")
CLASSIFIED_PATH = os.environ.get("CLASSIFIED_PATH", os.path.join(_REPO_ROOT, "data", "classified"))
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", os.path.join(_REPO_ROOT, "data", "reports"))

# Origen del corpus clasificado:
#   "kafka" -> lee el topic classified-hate (centralizado; ideal en cluster multi-nodo)
#   "jsonl" -> lee los JSONL de data/classified (single-node / local)
#   "auto"  -> intenta Kafka; si no hay datos cae a JSONL; si tampoco, sintético
CLASSIFIED_SOURCE = os.environ.get("CLASSIFIED_SOURCE", "auto").lower()
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CLASSIFIED_TOPIC = os.environ.get("CLASSIFIED_TOPIC", "classified-hate")
# Paquete del conector Kafka para Spark SQL (debe coincidir con la versión de Spark).
SPARK_KAFKA_PACKAGE = os.environ.get(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)

# Esquema del registro clasificado (debe coincidir con flink_job1_hate_detector)
_SENTIMENT = StructType([
    StructField("score", DoubleType()),
    StructField("label", StringType()),
])
_POLITICAL = StructType([
    StructField("label", StringType()),
    StructField("scores", MapType(StringType(), LongType())),
    StructField("polarization_index", DoubleType()),
])
CLASSIFIED_SCHEMA = StructType([
    StructField("post_id", StringType()),
    StructField("source", StringType()),
    StructField("author", StringType()),
    StructField("text", StringType()),
    StructField("text_preview", StringType()),
    StructField("is_hate_speech", BooleanType()),
    StructField("hate_words", ArrayType(StringType())),
    StructField("is_terruco", BooleanType()),
    StructField("terruco_matches", ArrayType(StringType())),
    StructField("sentiment", _SENTIMENT),
    StructField("political_classification", _POLITICAL),
    StructField("discrimination_types", ArrayType(StringType())),
    StructField("toxicity_score", DoubleType()),
    StructField("needs_review", BooleanType()),
    StructField("timestamp", StringType()),
    StructField("processed_at", StringType()),
    StructField("latency_ms", DoubleType()),
])


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.executor.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
    )
    # Si el nodo tiene Hadoop/HDFS instalado, Spark hereda fs.defaultFS=hdfs://... y
    # buscaría los JSONL en HDFS. En esta arquitectura Flink los escribe en DISCO LOCAL,
    # así que forzamos el filesystem local salvo que se indique HDFS explícitamente.
    if os.environ.get("FS_DEFAULT", "local").lower() == "local":
        builder = builder.config("spark.hadoop.fs.defaultFS", "file:///")
    # Conector Kafka para Spark SQL (necesario si vamos a leer el topic classified-hate).
    if CLASSIFIED_SOURCE in ("kafka", "auto"):
        builder = builder.config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _has_classified_files() -> bool:
    if not os.path.isdir(CLASSIFIED_PATH):
        return False
    files = glob.glob(os.path.join(CLASSIFIED_PATH, "**", "*.json"), recursive=True)
    return any(".inprogress" not in f for f in files)


def _synthetic_dataframe(spark):
    """Dataset de demostración generado con el pipeline NLP real (no datos reales)."""
    import random
    import uuid
    from datetime import datetime, timezone

    from nlp.nlp_pipeline import full_analysis

    sample_texts = [
        ("Este gobierno comunista nos va a llevar a la ruina, terrucos de mierda", "twitter"),
        ("Apoyamos la democracia y el estado de derecho en el Peru", "twitter"),
        ("Los caviares siempre defienden a los narcoterroristas, una verguenza", "reddit"),
        ("El libre mercado es la unica salida para el desarrollo del pais", "twitter"),
        ("Ese serrano que tenemos de presidente no sabe ni leer", "twitter"),
        ("La inversion privada genera empleo, no el estatismo comunista", "reddit"),
        ("Juntos podemos construir un Peru mas justo e igualitario para todos", "twitter"),
        ("Todos los cholos son iguales, no sirven para gobernar", "twitter"),
        ("La educacion publica necesita mas inversion urgentemente", "reddit"),
        ("Esa candidata feminazi quiere destruir la familia peruana tradicional", "twitter"),
        ("Chavistas infiltrados quieren expropiar las empresas como Fujimori temia", "twitter"),
        ("Fuera los senderistas del gobierno, no al narcoterrorismo de Castillo", "twitter"),
    ]
    authors = [f"user_{i}" for i in range(120)]
    rows = []
    for i in range(4000):
        text, src = random.choice(sample_texts)
        post_id = str(uuid.uuid4())
        analysis = full_analysis(text, src, post_id)
        ts = datetime(2024, random.randint(1, 6), random.randint(1, 28),
                      random.randint(0, 23), random.randint(0, 59), tzinfo=timezone.utc).isoformat()
        rows.append({
            **analysis,
            "text": text,
            "author": random.choice(authors),
            "timestamp": ts,
            "processed_at": ts,
            "latency_ms": round(random.uniform(20, 400), 2),
        })
    return spark.createDataFrame(rows, schema=CLASSIFIED_SCHEMA)


def _load_from_kafka(spark):
    """
    Lee el topic classified-hate de Kafka en modo BATCH (earliest..latest) y parsea el
    JSON. Centralizado en el broker → funciona igual desde cualquier nodo del cluster
    (no depende de discos locales repartidos). Devuelve DataFrame o None si no hay datos.
    """
    from pyspark.sql.functions import col, from_json

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", CLASSIFIED_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    df = (
        raw.select(from_json(col("value").cast("string"), CLASSIFIED_SCHEMA).alias("r"))
        .select("r.*")
        .filter(col("post_id").isNotNull())
        # at-least-once: el stream puede reemitir; deduplicamos por post_id.
        .dropDuplicates(["post_id"])
    )
    return df if df.head(1) else None


def _load_from_jsonl(spark):
    """Lee los JSONL de data/classified (single-node). Devuelve DataFrame o None."""
    if not _has_classified_files():
        return None
    df = (
        spark.read.schema(CLASSIFIED_SCHEMA)
        .option("recursiveFileLookup", "true")
        .json(CLASSIFIED_PATH)
    ).filter("post_id is not null")
    return df if df.head(1) else None


def load_classified(spark):
    """
    Devuelve (DataFrame, is_real). El origen se elige con CLASSIFIED_SOURCE:
      - "kafka": topic classified-hate (recomendado en cluster).
      - "jsonl": archivos JSONL locales.
      - "auto" : intenta Kafka, luego JSONL, luego dataset sintético de demo.
    """
    if CLASSIFIED_SOURCE == "kafka":
        df = _load_from_kafka(spark)
        if df is not None:
            print(f"[SPARK] Datos REALES desde Kafka topic '{CLASSIFIED_TOPIC}' @ {KAFKA_BOOTSTRAP}")
            return df, True
        print("[SPARK] AVISO: topic Kafka vacio -> dataset SINTETICO de demo")
        return _synthetic_dataframe(spark), False

    if CLASSIFIED_SOURCE == "jsonl":
        df = _load_from_jsonl(spark)
        if df is not None:
            print(f"[SPARK] Datos REALES desde {CLASSIFIED_PATH}")
            return df, True
        print("[SPARK] AVISO: sin JSONL -> dataset SINTETICO de demo")
        return _synthetic_dataframe(spark), False

    # auto
    try:
        df = _load_from_kafka(spark)
        if df is not None:
            print(f"[SPARK] Datos REALES desde Kafka topic '{CLASSIFIED_TOPIC}' @ {KAFKA_BOOTSTRAP}")
            return df, True
    except Exception as exc:
        print(f"[SPARK] Kafka no disponible ({exc}); intento JSONL")
    df = _load_from_jsonl(spark)
    if df is not None:
        print(f"[SPARK] Datos REALES desde {CLASSIFIED_PATH}")
        return df, True
    print("[SPARK] AVISO: sin datos del stream todavia -> usando dataset SINTETICO de demo")
    return _synthetic_dataframe(spark), False


def write_report(report: dict, filename: str):
    import json

    os.makedirs(OUTPUT_PATH, exist_ok=True)
    full = os.path.join(OUTPUT_PATH, filename)
    with open(full, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    print(f"[SPARK] Reporte guardado: {full}")
