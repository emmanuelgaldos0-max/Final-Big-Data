# Documentación de Arquitectura — Big Data Pipeline

## Arquitectura del Cluster

### Nodo Master (t3.xlarge — 4 vCPU, 16 GB RAM)
**Rol**: Coordinación general y servicios de control
- **Zookeeper**: Coordinación de Kafka (puerto 2181)
- **Kafka Broker líder** (broker.id=0, puerto 9092): Gestiona la distribución de topics
- **Flink JobManager** (puerto 8081): Planifica y distribuye jobs de streaming
- **Spark Master** (puerto 8080): Asigna tareas batch a los workers
- **Flask Dashboard** (puerto 5000): Sirve el panel web con resultados
- **Productores Kafka**: Inyectan datos desde APIs y datasets

**Justificación**: El master no procesa datos pesados, solo coordina. Necesita 16GB RAM
para correr simultáneamente Zookeeper, Kafka, Flink JobManager y Spark Master sin swap.

### Worker 1 (t3.large — 2 vCPU, 8 GB RAM)
**Rol**: Procesamiento de streaming en tiempo real
- **Kafka Broker réplica** (broker.id=1): Replicación de mensajes para tolerancia a fallos
- **Flink TaskManager** (4 task slots): Ejecuta los 5 jobs de streaming paralelos

**Justificación**: Dedicado a Flink garantiza baja latencia en el procesamiento de eventos.
Aislar Flink en su propio nodo evita interferencia con jobs Spark que usan CPU/RAM intensivo.

### Worker 2 (t3.large — 2 vCPU, 8 GB RAM)
**Rol**: Procesamiento batch y almacenamiento de resultados
- **Kafka Broker réplica** (broker.id=2): Segunda réplica para alta disponibilidad
- **Spark Worker** (2 cores, 4GB RAM): Ejecuta los 5 jobs batch programados
- **Redis** (puerto 6379): Almacenamiento in-memory de resultados para el Dashboard

**Justificación**: Spark batch consume RAM intensivamente durante la ejecución.
Redis en el mismo nodo que Spark permite que los jobs escriban resultados localmente
con latencia mínima antes de que el dashboard los lea desde el master.

---

## Diagrama de Flujo del Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FUENTES DE DATOS                            │
│  [HatEval Dataset]  [Reddit Pullpush API]  [Twitter Dataset Zenodo] │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ producer_dataset.py / producer_reddit.py
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      APACHE KAFKA (Broker Cluster)                   │
│   Topic: raw-tweets (3 particiones, RF=2)                           │
│   Topic: raw-comments (3 particiones, RF=2)                         │
│   Topic: classified-hate (3 particiones, RF=2)                      │
│   Topic: alerts (1 partición, RF=1)                                 │
│   Topic: metrics (1 partición, RF=1)                                │
└──────────────────┬──────────────────────────────────────────────────┘
                   │
        ┌──────────┴──────────┐
        ▼                     ▼
┌───────────────┐   ┌─────────────────┐
│ APACHE FLINK  │   │  APACHE SPARK   │
│ (Streaming)   │   │  (Batch)        │
│               │   │                 │
│ Job1: Hate    │   │ Job1: Histórico │
│ Job2: Window  │   │ Job2: TF-IDF    │
│ Job3: Latency │   │ Job3: Graph     │
│ Job4: Alerts  │   │ Job4: Sentiment │
│ Job5: Topics  │   │ Job5: Profiles  │
└───────┬───────┘   └────────┬────────┘
        │                    │
        └──────────┬─────────┘
                   ▼
         ┌─────────────────┐
         │      REDIS      │
         │  (Resultados    │
         │   en tiempo     │
         │    real)        │
         └────────┬────────┘
                  ▼
         ┌─────────────────┐
         │   DASHBOARD     │
         │  Flask + SSE    │
         │  Chart.js       │
         │  puerto 5000    │
         └─────────────────┘
```

### Persistencia para la capa batch (Flink → disco → Spark)

El **Job Flink #1** no solo publica en `classified-hate` y Redis: también escribe cada
evento clasificado a **JSONL** en `data/classified/` mediante un `FileSink` (finalizado en
cada checkpoint). Esa carpeta es el **dataset histórico real** que leen los 5 jobs de Spark
(`spark_common.load_classified`). Así el batch analiza datos producidos por el propio
streaming, no datos sintéticos. Si aún no ha corrido el stream, los jobs Spark caen a un
dataset sintético de demo claramente marcado en el reporte (`data_source: synthetic`).

---

## Documentación de Jobs

### Jobs Flink (Streaming)

| # | Nombre | Input | Output | Capacidad Técnica | Por qué Flink |
|---|--------|-------|--------|-------------------|---------------|
| 1 | HateSpeechStreamDetector | raw-tweets, raw-comments | classified-hate, Redis:hate:live | Pipeline NLP evento-a-evento | Latencia < 2s por mensaje, true streaming |
| 2 | SlidingWindowTrendCounter | classified-hate | Redis:metrics:window | Sliding windows stateful | Ventanas deslizantes con estado nativo |
| 3 | SystemLatencyMonitor | raw-tweets, raw-comments | Redis:metrics:latency | Event-time vs processing-time | Flink distingue ambos tiempos nativamente |
| 4 | DynamicAlertSystem | classified-hate | Kafka:alerts, Redis:alerts | Tumbling windows + side outputs | Stateful burst detection en tiempo real |
| 5 | PoliticalTopicClassifier | raw-tweets, raw-comments | Redis:topics:counts | Enriquecimiento multi-label en stream | Clasificación por evento sin micro-batch lag |

### Jobs Spark (Batch)

| # | Nombre | Input | Output | Capacidad Técnica | Por qué Spark |
|---|--------|-------|--------|-------------------|---------------|
| 1 | HistoricalHateAnalysis | data/classified/*.jsonl | reports/historical_hate_report.json | GroupBy + agregaciones sobre dataset completo | Escaneo paralelo de todo el corpus histórico |
| 2 | TFIDFKeywordExtractor | Corpus hate_speech=True | reports/tfidf_keywords.json | MLlib TF-IDF distribuido | IDF requiere ver todo el corpus a la vez |
| 3 | DiscriminationCoOccurrenceGraph | Corpus hate | reports/cooccurrence_graph.json | GraphFrames — análisis de grafos | Joins cartesianos sobre corpus completo |
| 4 | PoliticalSentimentReport | Mensajes clasificados | reports/sentiment_by_party.json | SparkSQL window functions complejas | Múltiples agregaciones sobre big data |
| 5 | ToxicUserProfiler | Mensajes con author | reports/user_profiles.json | Window functions + ranking distribuido | Historial completo por usuario — batch |

---

## Técnicas NLP Implementadas

El pipeline NLP (`nlp/nlp_pipeline.py`) es **100% basado en léxico y expresiones regulares**
en español (sin modelos de ML pesados), lo cual es deliberado: es ligero, explicable y
suficiente para el "NLP básico" que pide el enunciado. Las técnicas son:

1. **Tokenización y limpieza** (regex / `str`): minúsculas, eliminación de URLs, menciones (@), normalización de hashtags y caracteres especiales.
2. **Detección por léxico de odio**: diccionario contextualizado para Perú (racismo, clasismo, misoginia).
3. **Detección de terruqueo** (regex): patrones para identificar acusaciones políticas de terrorismo.
4. **Análisis de sentimiento** (léxico ponderado): scoring positivo/negativo con manejo de negaciones en español.
5. **Clasificación de polarización política** (keywords): izquierda/derecha con índice de polarización.
6. **Clasificación de tipo de discriminación** (multi-etiqueta): racial, género, clase social, regional.

Sobre el corpus completo, la capa **Spark** añade dos técnicas distribuidas:

7. **TF-IDF** (Spark MLlib): `Tokenizer → StopWordsRemover → HashingTF → IDF` sobre el corpus tóxico (Job Spark #2).
8. **Co-ocurrencia de términos** (Spark SQL: `explode` + `groupBy`, patrón GraphFrames): grafo de insultos/etiquetas que aparecen juntos (Job Spark #3).

---

## Métricas del Sistema

- **Throughput**: Objetivo > 5 msg/s (medido en Kafka consumer lag y contador Flink)
- **Latencia promedio**: Objetivo < 500ms (Flink Job1, desde ingesta hasta clasificación)
- **Latencia P95**: Objetivo < 2000ms (SLA del sistema)
- **Violaciones de SLA**: Monitoreadas en tiempo real por Flink Job3
