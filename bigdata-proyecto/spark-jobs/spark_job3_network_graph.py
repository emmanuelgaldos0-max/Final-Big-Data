"""
spark_job3_network_graph.py
===========================
JOB SPARK #3: Grafo de co-ocurrencia de términos discriminatorios

Nombre: DiscriminationCoOccurrenceGraph
Qué hace: Construye un grafo de co-ocurrencia de términos discriminatorios sobre el
          corpus completo: qué insultos/etiquetas aparecen juntos con mayor frecuencia.
          Calcula nodos (con grado) y aristas ponderadas para visualización.
Entrada:  data/classified/ (texto de los mensajes)
Salida:   data/reports/cooccurrence_graph.json (nodes + edges)
Capacidad técnica: Generación de pares y agregación distribuida tipo grafo (explode +
          groupBy), patrón de GraphFrames sobre el corpus completo.
Por qué Spark: requiere combinar términos por documento sobre TODO el corpus y agregar
          las co-ocurrencias globalmente — un cálculo batch sobre datos en reposo.
"""

import itertools
import os
import sys
from collections import defaultdict

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

sys.path.insert(0, os.path.dirname(__file__))
from spark_common import get_spark, load_classified, write_report

TARGET_WORDS = ["terruco", "cholo", "comunista", "caviar", "serrano", "feminazi",
                "corrupto", "chavista", "narco", "senderista", "ladron", "chusma"]


def main():
    spark = get_spark("DiscriminationCoOccurrenceGraph")
    df, is_real = load_classified(spark)
    df = df.filter(F.col("text").isNotNull())

    @F.udf(returnType=ArrayType(StringType()))
    def found_targets(text):
        t = (text or "").lower()
        return [w for w in TARGET_WORDS if w in t]

    @F.udf(returnType=ArrayType(ArrayType(StringType())))
    def make_pairs(words):
        if not words or len(words) < 2:
            return []
        return [list(p) for p in itertools.combinations(sorted(set(words)), 2)]

    pairs = (
        df.withColumn("found", found_targets(F.col("text")))
        .withColumn("pairs", make_pairs(F.col("found")))
        .select(F.explode("pairs").alias("pair"))
        .withColumn("word1", F.col("pair")[0])
        .withColumn("word2", F.col("pair")[1])
    )

    cooccurrence = pairs.groupBy("word1", "word2").count().orderBy(F.col("count").desc())
    edges = cooccurrence.toPandas()

    degree = defaultdict(int)
    for _, row in edges.iterrows():
        degree[row["word1"]] += int(row["count"])
        degree[row["word2"]] += int(row["count"])

    graph = {
        "data_source": "real" if is_real else "synthetic",
        "nodes": [{"id": w, "label": w, "degree": d} for w, d in degree.items()],
        "edges": edges.to_dict(orient="records"),
    }
    write_report(graph, "cooccurrence_graph.json")
    print(f"[JOB3-SPARK] Grafo: {len(graph['nodes'])} nodos, {len(graph['edges'])} aristas")
    spark.stop()


if __name__ == "__main__":
    main()
