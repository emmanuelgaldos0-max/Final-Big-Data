"""
spark_job1_historical_analysis.py
=================================
JOB SPARK #1: Análisis histórico de discurso de odio

Nombre: HistoricalHateAnalysis
Qué hace: Lee TODO el corpus clasificado persistido por Flink (data/classified/*.json),
          calcula la distribución temporal (por día y por hora) y por fuente, e
          identifica el volumen histórico de odio/terruqueo.
Entrada:  data/classified/ (JSONL escrito por flink_job1)
Salida:   data/reports/historical_hate_report.json
Capacidad técnica: Agregaciones distribuidas (groupBy) sobre el dataset completo.
Por qué Spark y no Flink: es análisis sobre datos en reposo (batch). Flink está
          optimizado para streams continuos; Spark escanea todas las particiones en
          paralelo y agrega de forma eficiente sobre el histórico completo.
"""

import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))
from spark_common import get_spark, load_classified, write_report


def main():
    spark = get_spark("HistoricalHateAnalysis")
    df, is_real = load_classified(spark)

    df = df.withColumn("date", F.to_date("processed_at")).withColumn(
        "hour", F.hour(F.to_timestamp("processed_at"))
    )
    total = df.count()
    print(f"[JOB1-SPARK] Total registros: {total} (datos {'reales' if is_real else 'sinteticos'})")

    by_source = df.groupBy("source").agg(
        F.count("*").alias("total"),
        F.sum(F.col("is_hate_speech").cast("int")).alias("hate_count"),
        F.avg("toxicity_score").alias("avg_toxicity"),
    ).toPandas()

    daily_trend = df.groupBy("date").agg(
        F.count("*").alias("total"),
        F.sum(F.col("is_hate_speech").cast("int")).alias("hate_count"),
        F.avg("toxicity_score").alias("avg_toxicity"),
    ).orderBy("date").toPandas()

    hourly = df.groupBy("hour").agg(
        F.count("*").alias("total"),
        F.sum(F.col("is_hate_speech").cast("int")).alias("hate_count"),
    ).orderBy("hour").toPandas()

    report = {
        "data_source": "real" if is_real else "synthetic",
        "total_records": total,
        "hate_speech_total": int(df.filter(F.col("is_hate_speech")).count()),
        "terruco_total": int(df.filter(F.col("is_terruco")).count()),
        "avg_toxicity_global": float(df.agg(F.avg("toxicity_score")).collect()[0][0] or 0),
        "by_source": by_source.to_dict(orient="records"),
        "daily_trend": daily_trend.to_dict(orient="records"),
        "hourly_distribution": hourly.to_dict(orient="records"),
    }

    write_report(report, "historical_hate_report.json")
    print(f"  Hate: {report['hate_speech_total']} | Terruco: {report['terruco_total']}")
    spark.stop()


if __name__ == "__main__":
    main()
