"""
spark_job4_sentiment_report.py
==============================
JOB SPARK #4: Reporte de sentimiento agregado por partido/candidato

Nombre: PoliticalSentimentReport
Qué hace: Detecta menciones de partidos/candidatos en el texto del corpus, y agrega por
          cada uno la distribución de sentimiento (positivo/negativo/neutro), la toxicidad
          media y la tasa de negatividad. Genera un ranking por toxicidad.
Entrada:  data/classified/ (texto + sentiment.label + toxicity_score)
Salida:   data/reports/sentiment_by_party.json
Capacidad técnica: Agregaciones complejas con Spark SQL (CASE/AVG/GROUP BY) sobre el
          corpus completo, con una mención expandida por partido.
Por qué Spark: combina detección de menciones y múltiples agregaciones sobre todo el
          histórico — un reporte batch, no un cálculo en streaming.
"""

import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

sys.path.insert(0, os.path.dirname(__file__))
from spark_common import get_spark, load_classified, write_report

PARTIES = {
    "Peru Libre": ["castillo", "peru libre", "cerron"],
    "Fuerza Popular": ["fujimori", "fuerza popular", "keiko"],
    "Renovacion Popular": ["lopez aliaga", "renovacion popular", "porky"],
    "Alianza para el Progreso": ["acuna", "app"],
    "Partido Morado": ["partido morado", "sagasti"],
}


def main():
    spark = get_spark("PoliticalSentimentReport")
    df, is_real = load_classified(spark)
    df = df.filter(F.col("text").isNotNull()).withColumn("sent_label", F.col("sentiment.label"))

    @F.udf(returnType=ArrayType(StringType()))
    def parties_in(text):
        t = (text or "").lower()
        return [party for party, kws in PARTIES.items() if any(kw in t for kw in kws)]

    mentions = (
        df.withColumn("party", F.explode(parties_in(F.col("text"))))
        .select("party", "sent_label", "toxicity_score")
    )
    mentions.createOrReplaceTempView("mentions")

    result = spark.sql(
        """
        SELECT party,
               COUNT(*) AS total_mentions,
               SUM(CASE WHEN sent_label = 'positivo' THEN 1 ELSE 0 END) AS positive,
               SUM(CASE WHEN sent_label = 'negativo' THEN 1 ELSE 0 END) AS negative,
               SUM(CASE WHEN sent_label = 'neutro'   THEN 1 ELSE 0 END) AS neutral,
               ROUND(AVG(toxicity_score), 3) AS avg_toxicity,
               ROUND(SUM(CASE WHEN sent_label = 'negativo' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS negativity_rate
        FROM mentions
        GROUP BY party
        ORDER BY avg_toxicity DESC
        """
    ).toPandas()

    report = {
        "data_source": "real" if is_real else "synthetic",
        "total_mentions": int(result["total_mentions"].sum()) if not result.empty else 0,
        "parties": result.to_dict(orient="records"),
    }
    write_report(report, "sentiment_by_party.json")
    print(result.to_string())
    spark.stop()


if __name__ == "__main__":
    main()
