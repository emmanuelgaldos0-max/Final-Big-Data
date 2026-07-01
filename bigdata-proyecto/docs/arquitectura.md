# Documentación de Arquitectura — Big Data Pipeline

## Arquitectura del Cluster

> Despliegue real: **3 instancias EC2 `t3.large`** (2 vCPU, 8 GB) en AWS Academy, Ubuntu 24.04,
> todo **nativo** (sin Docker). Kafka corre en modo **KRaft** (sin Zookeeper).

### Nodo Master (t3.large — 2 vCPU, 8 GB RAM)
**Rol**: Coordinación general y servicios de control. **No hace cómputo pesado.**
- **Kafka (KRaft)** (puerto 9092, controller 9093): bus de mensajes; único broker (node.id=1)
- **Redis** (puerto 6379): almacén in-memory de métricas que consume el dashboard
- **Flink JobManager** (puerto 8081): planifica y reparte las subtareas de los jobs streaming
- **Spark Master** (puerto 7077, UI 8080): coordina y lanza los executors batch en los workers
- **Flask Dashboard** (puerto 5000): sirve el panel web con resultados
- **Productor Kafka**: inyecta el corpus real a alto volumen

**Justificación**: el master centraliza la coordinación y sirve el dashboard; los datos pesados
los procesan los workers. Kafka y Redis viven aquí (centralizados) para que cualquier worker los
alcance por la red privada de la VPC.

### Worker 1 y Worker 2 (t3.large — 2 vCPU, 8 GB RAM cada uno)
**Rol**: cómputo distribuido. **Los dos nodos son idénticos y simétricos** — no hay especialización
por nodo: cada worker corre a la vez los dos motores de procesamiento.
- **Flink TaskManager** (4 task slots): ejecuta subtareas de los 5 jobs streaming
- **Spark Worker**: ejecuta executors de los 5 jobs batch

**Justificación**: con 2 workers hay **8 slots Flink** (4+4) y executors Spark en 2 máquinas, así el
trabajo se divide de verdad entre nodos. Quién procesa qué lo deciden **dinámicamente** Flink (reparte
subtareas, con `evenly-spread-out-slots` para balancear) y Spark (reparte particiones a los executors);
NO se asigna streaming a un nodo y batch al otro. Ambos workers participan en ambas cargas.

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
│                 APACHE KAFKA (KRaft, 1 broker en el master)          │
│   Topic: raw-tweets (3 particiones, RF=1)                           │
│   Topic: raw-comments (3 particiones, RF=1)                         │
│   Topic: classified-hate (3 particiones, RF=1)                      │
│   Topic: alerts (3 particiones, RF=1)                               │
│   Topic: metrics (3 particiones, RF=1)                              │
└──────────────────┬──────────────────────────────────────────────────┘
                   │   (ambos workers consumen y computan ambas cargas)
        ┌──────────┴──────────┐
        ▼                     ▼
┌───────────────┐   ┌─────────────────┐
│ APACHE FLINK  │   │  APACHE SPARK   │
│ (streaming)   │   │  (batch)        │
│ TaskManagers  │   │  Workers        │
│ en W1 y W2    │   │  en W1 y W2     │
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
