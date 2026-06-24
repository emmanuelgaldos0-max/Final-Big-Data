# 🔍 Detección de Discurso Discriminatorio y Polarización Política en Redes Sociales

## Big Data — Trabajo Final | Universidad

---

## 📋 Descripción General

Sistema de análisis en tiempo real que detecta y clasifica publicaciones con contenido discriminatorio, racista o polarizante en el contexto electoral peruano, usando Apache Kafka, Apache Flink y Apache Spark.

---

## 🗂️ Estructura del Repositorio

```
bigdata-proyecto/
├── producers/              # Productores Kafka (datos reales multi-origen)
│   ├── producer_dataset.py    # PRINCIPAL: reproduce el corpus real a alto volumen
│   ├── producer_reddit.py     # En vivo: Reddit Perú (Pullpush)
│   ├── producer_mastodon.py   # En vivo: Mastodon (hashtags peruanos)
│   ├── producer_twitch.py     # En vivo: chat de Twitch
│   ├── producer_firehose.py   # Respaldo OFFLINE sintético (no es la fuente principal)
│   └── requirements.txt
├── flink-jobs/             # 5 Jobs de streaming (Apache Flink via PyFlink)
│   ├── flink_job1_hate_detector.py
│   ├── flink_job2_realtime_counter.py
│   ├── flink_job3_latency_monitor.py
│   ├── flink_job4_alert_system.py
│   ├── flink_job5_topic_classifier.py
│   └── requirements.txt
├── spark-jobs/             # 5 Jobs batch (Apache Spark via PySpark)
│   ├── spark_job1_historical_analysis.py
│   ├── spark_job2_tfidf_keywords.py
│   ├── spark_job3_network_graph.py
│   ├── spark_job4_sentiment_report.py
│   ├── spark_job5_user_profiling.py
│   └── requirements.txt
├── nlp/                    # Módulo NLP compartido
│   ├── nlp_pipeline.py
│   └── dictionaries/
│       ├── hate_words_es.txt
│       └── political_terms_peru.txt
├── dashboard/              # Dashboard web (Flask + Chart.js)
│   ├── app.py
│   ├── templates/
│   │   └── index.html
│   └── requirements.txt
├── data/                   # Datos reales y scripts de obtención
│   ├── fetch_real_datasets.py  # descarga+consolida datos REALES -> corpus_real.jsonl
│   ├── corpus_real.jsonl       # corpus consolidado (~16k textos reales, multi-origen)
│   ├── download_datasets.sh    # (referencia manual de datasets)
│   └── sample_data.jsonl       # muestra pequeña de respaldo
├── infrastructure/         # Scripts de infraestructura AWS
│   ├── setup_cluster.sh
│   ├── start_services.sh
│   └── kafka_topics.sh
└── docs/
    └── arquitectura.md
```

---

## 📦 Fuentes de Datos (REALES, multi-origen)

El proyecto ingiere **datos reales de varios orígenes públicos** (no sintéticos). Se
consolidan en `data/corpus_real.jsonl` (~16k textos únicos, español/Perú) y se reproducen
a alto volumen; además hay productores **en vivo**. Detalle completo y justificación en
[`Documentacion/Fuentes-de-Datos.md`](../Documentacion/Fuentes-de-Datos.md).

| Origen | Tipo | Contenido | Topic |
|--------|------|-----------|-------|
| `pyupeu/social-media-peruvian-sentiment` (HF) | bulk ~8.9k | redes peruanas | raw-tweets |
| `paolorivas/noticias_peru` (HF) | bulk ~2.9k | noticias Perú | raw-comments |
| `Paul/hatecheck-spanish` (HF) | bulk ~3.0k | odio en español | raw-tweets |
| `afcarvallo/spanish-ner-hate-bio-filtered` (HF) | bulk ~0.8k | odio español anotado | raw-comments |
| Reddit Perú (Pullpush, r/peru, r/PeruPolitica) | vivo/bulk ~1.1k+ | política peruana real | raw-comments |
| Mastodon / Twitch | en vivo | hashtags peruanos / chat | raw-tweets / raw-comments |

```bash
# Obtener los datos reales (Hugging Face parquet + Reddit Pullpush):
python data/fetch_real_datasets.py        # -> data/corpus_real.jsonl
```

> El campo `source` (twitter/reddit/news) decide el topic; el campo `origin` conserva el
> dataset de procedencia y se muestra en el dashboard (panel "Fuentes de datos reales").

---

## 🏗️ Infraestructura AWS

### Configuración del Cluster (3 nodos mínimo)

| Nodo | Tipo EC2 | Rol | Servicios |
|------|----------|-----|-----------|
| Master | t3.xlarge (4 vCPU, 16 GB) | Coordinación | Kafka (broker líder), Zookeeper, Flink JobManager, Spark Master, Dashboard |
| Worker 1 | t3.large (2 vCPU, 8 GB) | Procesamiento streaming | Flink TaskManager, Kafka (broker réplica) |
| Worker 2 | t3.large (2 vCPU, 8 GB) | Procesamiento batch | Spark Worker, Kafka (broker réplica), Redis |

**Justificación**: El master centraliza la coordinación sin procesar datos pesados. Los workers se especializan: Worker 1 para baja latencia (Flink), Worker 2 para procesamiento batch (Spark). Redis en Worker 2 almacena resultados intermedios que consume el Dashboard.

---

## 🚀 Instalación desde Cero

### Paso 1: Preparar las instancias EC2

```bash
# En AWS Console:
# 1. Lanzar 3 instancias Ubuntu 22.04 LTS (t3.xlarge para master, t3.large para workers)
# 2. Mismo Security Group con puertos: 22, 2181, 9092, 8081, 8080, 6123, 5000, 6379
# 3. Asignar IPs privadas fijas (o usar DNS interno de AWS)

# Conectarse al master:
ssh -i tu-clave.pem ubuntu@<IP_MASTER>
```

### Paso 2: Instalar dependencias (ejecutar en TODOS los nodos)

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y openjdk-11-jdk python3 python3-pip wget curl git unzip

# Variables de entorno (agregar a ~/.bashrc)
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

### Paso 3: Instalar Apache Kafka (en todos los nodos)

```bash
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-3.7.0.tgz
sudo mv kafka_2.13-3.7.0 /opt/kafka
echo 'export PATH=$PATH:/opt/kafka/bin' >> ~/.bashrc
source ~/.bashrc
```

### Paso 4: Instalar Apache Flink (en master y worker1)

```bash
wget https://downloads.apache.org/flink/flink-1.19.0/flink-1.19.0-bin-scala_2.12.tgz
tar -xzf flink-1.19.0-bin-scala_2.12.tgz
sudo mv flink-1.19.0 /opt/flink
echo 'export FLINK_HOME=/opt/flink' >> ~/.bashrc
echo 'export PATH=$PATH:$FLINK_HOME/bin' >> ~/.bashrc

# Instalar PyFlink
pip3 install apache-flink==1.19.0
```

### Paso 5: Instalar Apache Spark (en master y worker2)

```bash
wget https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3 /opt/spark
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc

pip3 install pyspark==3.5.1
```

### Paso 6: Instalar Redis y dependencias Python

```bash
# En Worker 2 (o master si se prefiere):
sudo apt install -y redis-server
sudo sed -i 's/bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/redis.conf
sudo systemctl enable redis-server

# Dependencias Python por componente (instalar las que corresponden a cada nodo):
pip3 install -r producers/requirements.txt
pip3 install -r flink-jobs/requirements.txt   # incluye apache-flink (PyFlink real)
pip3 install -r spark-jobs/requirements.txt
pip3 install -r dashboard/requirements.txt
```

> El NLP es 100% léxico/regex (ver `nlp/nlp_pipeline.py`): **no** requiere nltk,
> transformers ni torch.

### Conector Kafka para PyFlink (obligatorio)

PyFlink necesita el JAR del conector Kafka (no viene en el wheel). En los nodos con Flink:

```bash
FLINK_KAFKA_JAR=flink-sql-connector-kafka-3.2.0-1.19.jar
wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/$FLINK_KAFKA_JAR
```

Si ejecutas un job en modo local (minicluster, `python3 flink_jobX.py`) en vez de
`flink run`, exporta la ruta del jar para que `flink_common.get_env()` lo cargue:

```bash
export FLINK_CONNECTOR_JARS=/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar
```

### Paso 7: Clonar repositorio

> ⚠️ **IMPORTANTE — la ruta NO debe contener espacios.** PyFlink ejecuta sus UDFs de
> Python mediante un script interno (`pyflink-udf-runner.sh`) que no entrecomilla las
> rutas; si el proyecto vive en una carpeta con espacios (p.ej. `Github Personal/`), los
> jobs Flink fallan con `Failed to create stage bundle factory`. Clona siempre en una
> ruta limpia como `~/bigdata-proyecto`.

```bash
cd ~
git clone https://github.com/TU_USUARIO/bigdata-proyecto.git
cd bigdata-proyecto
```

---

## ▶️ Arranque del Sistema (tras reinicio de sesión)

Usar el script maestro:

```bash
# En el nodo MASTER:
cd bigdata-proyecto/infrastructure
bash start_services.sh
```

O manualmente en orden:

```bash
# 1. Zookeeper (master)
/opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties

# 2. Kafka brokers (todos los nodos, adaptar broker.id)
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties

# 3. Crear topics (master, esperar ~10s a que Kafka inicie)
bash infrastructure/kafka_topics.sh

# 4. Flink cluster (master)
/opt/flink/bin/start-cluster.sh

# 5. Spark cluster (master)
/opt/spark/sbin/start-master.sh
/opt/spark/sbin/start-worker.sh spark://<IP_MASTER>:7077  # en worker2

# 6. Redis (worker2 o master)
sudo systemctl start redis-server

# 7. Dashboard (master)
cd dashboard && python3 app.py &

# 8. Productores Kafka — fuente principal: corpus REAL a alto volumen
#    (si falta data/corpus_real.jsonl, primero:  python3 data/fetch_real_datasets.py)
cd producers && python3 producer_dataset.py --rate 200 &
#    (opcional, fuentes en vivo simultáneas)
#    python3 producer_reddit.py &  python3 producer_mastodon.py &  python3 producer_twitch.py &

# 9. Jobs Flink — se SUBMITEN al cluster Flink (no se corren como script suelto).
#    -pyfs envía los módulos auxiliares (flink_common.py y el paquete nlp/).
cd flink-jobs
flink run -py flink_job1_hate_detector.py -pyfs flink_common.py,../nlp &
flink run -py flink_job2_realtime_counter.py -pyfs flink_common.py &
flink run -py flink_job3_latency_monitor.py -pyfs flink_common.py &
flink run -py flink_job4_alert_system.py   -pyfs flink_common.py &
flink run -py flink_job5_topic_classifier.py -pyfs flink_common.py,../nlp &
#    (Verlos corriendo en la Flink UI: http://<MASTER>:8081)

# 10. Jobs Spark (batch; ejecutar manualmente o vía cron). Leen el JSONL del Job Flink #1.
spark-submit spark-jobs/spark_job1_historical_analysis.py
spark-submit spark-jobs/spark_job2_tfidf_keywords.py
spark-submit spark-jobs/spark_job3_network_graph.py
spark-submit spark-jobs/spark_job4_sentiment_report.py
spark-submit spark-jobs/spark_job5_user_profiling.py
```

> **Nota sobre el cluster:** la sección de infraestructura de abajo describe el cluster
> en AWS (3 nodos EC2). Como alternativa válida del enunciado, este proyecto se desplegará
> sobre **2 máquinas físicas interconectadas** (PC Ubuntu + Mac M4) actuando como cluster
> Flink/Spark; esa topología se documentará en la fase de configuración de máquinas.

---

## 📊 Acceso al Dashboard

Una vez levantado: `http://<IP_MASTER>:5000`

---

## 📝 Métricas del Sistema

- **Throughput**: medido en mensajes/segundo en Kafka (visible en dashboard)
- **Latencia promedio**: tiempo entre producción y procesamiento Flink (< 2s objetivo)
