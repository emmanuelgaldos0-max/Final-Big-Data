# Ejecucion local en laptop

Esta configuracion permite probar el pipeline sin AWS. Kafka y Redis corren con
Docker Desktop, y los productores, jobs y dashboard corren con Python local.

## Requisitos

- Docker Desktop
- Python 3.10 o superior
- PowerShell

## 1. Levantar Kafka y Redis

Desde la raiz del proyecto:

```powershell
docker compose -f docker-compose.local.yml up -d
```

Verifica que ambos contenedores esten arriba:

```powershell
docker ps
```

## 2. Crear los topics de Kafka

```powershell
docker exec bigdata-kafka-local kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic raw-tweets
docker exec bigdata-kafka-local kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic raw-comments
docker exec bigdata-kafka-local kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic classified-hate
docker exec bigdata-kafka-local kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic metrics
docker exec bigdata-kafka-local kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic alerts
```

Listar topics:

```powershell
docker exec bigdata-kafka-local kafka-topics.sh --list --bootstrap-server localhost:9092
```

## 3. Preparar entorno Python

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r producers\requirements.txt
pip install -r dashboard\requirements.txt
pip install -r flink-jobs\requirements.txt
pip install -r spark-jobs\requirements.txt
```

Los jobs de `flink-jobs/` son **PyFlink real** (DataStream API). Al ejecutar
`python flink_jobX.py` directamente, PyFlink levanta un **minicluster local** embebido
(útil para probar en una sola máquina). Para ello necesitas:

1. `apache-flink` instalado (viene en `flink-jobs/requirements.txt`).
2. El JAR del conector Kafka, apuntado por `FLINK_CONNECTOR_JARS`:

```powershell
# Descarga una vez (PowerShell):
Invoke-WebRequest -OutFile flink-sql-connector-kafka-3.2.0-1.19.jar `
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
$env:FLINK_CONNECTOR_JARS="$PWD\flink-sql-connector-kafka-3.2.0-1.19.jar"
```

> En el **cluster real** (PC Ubuntu + Mac M4) los jobs no se corren como script suelto:
> se SUBMITEN con `flink run -py ...` al JobManager. Ver README.

## 4. Abrir terminales y ejecutar

Terminal 1, detector:

```powershell
$env:KAFKA_BOOTSTRAP="localhost:9092"
$env:REDIS_HOST="localhost"
$env:FLINK_CONNECTOR_JARS="$PWD\flink-sql-connector-kafka-3.2.0-1.19.jar"
python flink-jobs\flink_job1_hate_detector.py
```

Terminal 2, contador de ventana:

```powershell
$env:KAFKA_BOOTSTRAP="localhost:9092"
$env:REDIS_HOST="localhost"
$env:FLINK_CONNECTOR_JARS="$PWD\flink-sql-connector-kafka-3.2.0-1.19.jar"
python flink-jobs\flink_job2_realtime_counter.py
```

Terminal 3, productor de datos:

```powershell
$env:KAFKA_BOOTSTRAP="localhost:9092"
python producers\producer_dataset.py
```

Terminal 4, dashboard:

```powershell
$env:REDIS_HOST="localhost"
python dashboard\app.py
```

Abre:

```text
http://localhost:5000
```

## 5. Apagar todo

Deten los procesos Python con `Ctrl+C` y luego:

```powershell
docker compose -f docker-compose.local.yml down
```

Si quieres borrar los datos locales de Kafka y Redis:

```powershell
docker compose -f docker-compose.local.yml down -v
```

## Modo demo rapido

Si solo quieres ver el dashboard, puedes correr:

```powershell
python dashboard\app.py
```

El dashboard muestra datos simulados cuando Redis no esta disponible.
