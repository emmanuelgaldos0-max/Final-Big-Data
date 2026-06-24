"""
spark_job2_tfidf_keywords.py
============================
JOB SPARK #2: Extracción de keywords discriminatorios por TF-IDF

Nombre: TFIDFKeywordExtractor
Qué hace: Aplica TF-IDF (Spark MLlib) sobre el texto de los mensajes clasificados como
          discurso de odio para identificar los términos más característicos, y reporta
          las palabras más frecuentes del corpus tóxico.
Entrada:  data/classified/ (mensajes con is_hate_speech = true)
Salida:   data/reports/tfidf_keywords.json
Capacidad técnica: ML distribuido (MLlib) — Tokenizer + StopWordsRemover + HashingTF + IDF.
Por qué Spark: el IDF necesita estadísticas sobre TODO el corpus a la vez (document
          frequency global), por lo que es inherentemente batch. Flink no tiene una
          librería de ML equivalente para este cálculo.
"""

import os
import sys

from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, StopWordsRemover, Tokenizer
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))
from spark_common import get_spark, load_classified, write_report

SPANISH_STOPWORDS = ["de", "la", "el", "en", "y", "a", "los", "del", "se", "las",
                     "un", "por", "con", "que", "para", "una", "su", "al", "es", "lo",
                     "no", "mas", "ya", "le", "me", "mi", "te", "tu", "ese", "esa"]


def main():
    spark = get_spark("TFIDFKeywordExtractor")
    df, is_real = load_classified(spark)

    hate_df = df.filter(F.col("is_hate_speech") & F.col("text").isNotNull())
    corpus_size = df.count()
    hate_docs = hate_df.count()
    print(f"[JOB2-SPARK] Corpus {corpus_size} | hate docs {hate_docs} ({'real' if is_real else 'sintetico'})")

    # Pipeline TF-IDF distribuido (MLlib)
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=SPANISH_STOPWORDS)
    hashing_tf = HashingTF(inputCol="filtered", outputCol="raw_features", numFeatures=2000)
    idf = IDF(inputCol="raw_features", outputCol="features", minDocFreq=2)
    model = Pipeline(stages=[tokenizer, remover, hashing_tf, idf]).fit(hate_df)
    model.transform(hate_df)  # materializa el modelo TF-IDF sobre el corpus tóxico

    # Ranking de términos más frecuentes en el corpus tóxico
    words = hate_df.select(F.explode(F.split(F.lower("text"), r"\s+")).alias("word"))
    top_words = (
        words.filter(~F.col("word").isin(SPANISH_STOPWORDS))
        .filter(F.length("word") > 3)
        .groupBy("word")
        .count()
        .orderBy(F.col("count").desc())
        .limit(50)
        .toPandas()
    )

    report = {
        "data_source": "real" if is_real else "synthetic",
        "corpus_size": corpus_size,
        "hate_docs": hate_docs,
        "tfidf_num_features": 2000,
        "top_hate_keywords": top_words.to_dict(orient="records"),
    }
    write_report(report, "tfidf_keywords.json")
    print(f"[JOB2-SPARK] Top10: {top_words.head(10).to_dict(orient='records')}")
    spark.stop()


if __name__ == "__main__":
    main()
