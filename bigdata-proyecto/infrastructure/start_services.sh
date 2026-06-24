#!/bin/bash
# =============================================================================
# start_services.sh — Reinicia todos los servicios tras reinicio de sesión
# Ejecutar en el MASTER
# =============================================================================

MASTER_IP=$(hostname -I | awk '{print $1}')
echo ">>> Master IP: $MASTER_IP"

echo "[1/6] Iniciando Zookeeper..."
/opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
sleep 5

echo "[2/6] Iniciando Kafka broker..."
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
sleep 8

echo "[3/6] Creando topics Kafka..."
bash kafka_topics.sh

echo "[4/6] Iniciando Flink cluster..."
/opt/flink/bin/start-cluster.sh
sleep 5

echo "[5/6] Iniciando Spark master..."
/opt/spark/sbin/start-master.sh

echo "[6/6] Iniciando Redis..."
sudo systemctl start redis-server || redis-server --daemonize yes

echo ""
echo "=== Servicios levantados ==="
echo "Kafka:    ${MASTER_IP}:9092"
echo "Flink UI: http://${MASTER_IP}:8081"
echo "Spark UI: http://${MASTER_IP}:8080"
echo "Redis:    ${MASTER_IP}:6379"
echo ""
echo "En Worker1, ejecutar:"
echo "  /opt/flink/bin/taskmanager.sh start"
echo ""
echo "En Worker2, ejecutar:"
echo "  /opt/spark/sbin/start-worker.sh spark://${MASTER_IP}:7077"
echo "  sudo systemctl start redis-server"
echo ""
echo "Luego, en master:"
echo "  cd dashboard && python3 app.py &"
echo "  cd producers && python3 producer_dataset.py &"
