"""
spark_job5_user_profiling.py
============================
JOB SPARK #5: Perfilado de usuarios por comportamiento discriminatorio

Nombre: ToxicUserProfiler
Qué hace: Agrega el comportamiento histórico por autor para identificar perfiles de alto
          riesgo: usuarios que publican contenido tóxico de forma sistemática. Calcula un
          score de riesgo ponderado y rankea con window functions.
Entrada:  data/classified/ (mensajes con campo author)
Salida:   data/reports/user_profiles.json
Capacidad técnica: Agregaciones por grupo + window functions (rank) distribuidas.
Por qué Spark: el score depende de TODO el historial de cada usuario; recalcularlo
          requiere ver todos sus posts a la vez — análisis forense batch, no streaming.
"""

import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(__file__))
from spark_common import get_spark, load_classified, write_report


def main():
    spark = get_spark("ToxicUserProfiler")
    df, is_real = load_classified(spark)
    df = df.filter(F.col("author").isNotNull() & (F.col("author") != ""))

    user_stats = df.groupBy("author").agg(
        F.count("*").alias("total_posts"),
        F.sum(F.col("is_hate_speech").cast("int")).alias("hate_posts"),
        F.sum(F.col("is_terruco").cast("int")).alias("terruco_posts"),
        F.avg("toxicity_score").alias("avg_toxicity"),
        F.max("toxicity_score").alias("max_toxicity"),
        F.countDistinct("source").alias("platforms_used"),
        F.min("processed_at").alias("first_seen"),
        F.max("processed_at").alias("last_seen"),
    )

    user_stats = user_stats.withColumn(
        "risk_score",
        F.round(
            F.col("avg_toxicity") * 0.4
            + (F.col("hate_posts") / F.col("total_posts")) * 0.35
            + (F.col("terruco_posts") / F.col("total_posts")) * 0.25,
            3,
        ),
    )

    ranked = user_stats.withColumn("risk_rank", F.rank().over(Window.orderBy(F.col("risk_score").desc())))
    top_risky = ranked.filter(F.col("risk_score") > 0.4).orderBy(F.col("risk_score").desc()).limit(100)

    report = {
        "data_source": "real" if is_real else "synthetic",
        "total_users_analyzed": df.select("author").distinct().count(),
        "high_risk_users": top_risky.toPandas().to_dict(orient="records"),
    }
    write_report(report, "user_profiles.json")
    print(f"[JOB5-SPARK] Usuarios de alto riesgo: {len(report['high_risk_users'])}")
    spark.stop()


if __name__ == "__main__":
    main()
