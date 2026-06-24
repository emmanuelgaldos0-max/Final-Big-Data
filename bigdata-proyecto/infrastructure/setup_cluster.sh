#!/bin/bash
# =============================================================================
# setup_cluster.sh — Configuración inicial del cluster (ejecutar UNA sola vez)
# Ejecutar en el MASTER como ubuntu
# =============================================================================

set -e

MASTER_IP=$(hostname -I | awk '{print $1}')
WORKER1_IP="<IP_WORKER1>"   # ← Reemplazar
WORKER2_IP="<IP_WORKER2>"   # ← Reemplazar

echo "=== [1/5] Configurando Kafka ==="

# Zookeeper — solo master
cat > /opt/kafka/config/zookeeper.properties << EOF
dataDir=/tmp/zookeeper
clientPort=2181
maxClientCnxns=100
EOF

# Kafka broker — master (broker.id=0)
cat > /opt/kafka/config/server.properties << EOF
broker.id=0
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://${MASTER_IP}:9092
log.dirs=/tmp/kafka-logs
num.partitions=3
default.replication.factor=2
zookeeper.connect=${MASTER_IP}:2181
auto.create.topics.enable=false
EOF

echo "=== [2/5] Configurando Flink ==="

cat > /opt/flink/conf/flink-conf.yaml << EOF
jobmanager.rpc.address: ${MASTER_IP}
jobmanager.memory.process.size: 2048m
taskmanager.memory.process.size: 2048m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4
EOF

echo "${WORKER1_IP}" > /opt/flink/conf/workers

echo "=== [3/5] Configurando Spark ==="

cat > /opt/spark/conf/spark-env.sh << EOF
SPARK_MASTER_HOST=${MASTER_IP}
SPARK_WORKER_MEMORY=4g
SPARK_WORKER_CORES=2
EOF

echo "${WORKER2_IP}" > /opt/spark/conf/workers

echo "=== [4/5] Configurando variables de entorno ==="
echo "export MASTER_IP=${MASTER_IP}" >> ~/.bashrc
echo "export KAFKA_BOOTSTRAP=${MASTER_IP}:9092" >> ~/.bashrc
echo "export REDIS_HOST=${MASTER_IP}" >> ~/.bashrc
source ~/.bashrc

echo "=== [5/5] Setup completo ==="
echo "Ahora ejecuta: bash start_services.sh"
