#!/usr/bin/env bash
# =============================================================================
# arrancar-master.sh  —  NODO MASTER del cluster en AWS EC2
# -----------------------------------------------------------------------------
# Rol del master (NO ejecuta cómputo Flink/Spark, solo coordina y sirve):
#   · Kafka (KRaft) + Redis        -> en Docker, anunciados en su IP privada
#   · Flink JobManager             -> recibe los 5 jobs y los reparte a los workers
#   · Spark Master                 -> coordina los 5 jobs batch
#   · Dashboard Flask (puerto 5000)
#   · (luego) el productor de datos reales
#
# El CÓMPUTO (TaskManagers Flink + Workers Spark) vive en las 2 instancias worker.
# Así "1 master + 2 workers" queda limpio y justificado para el informe.
#
# Uso:   bash ~/Final-Big-Data-AWS/aws/arrancar-master.sh
# =============================================================================
set -uo pipefail
source "$HOME/.bdenv"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- IP privada de esta instancia (la que ven los workers dentro de la VPC) ----
PRIV_IP=$(hostname -I | awk '{print $1}')
echo "### MASTER · IP privada: $PRIV_IP ###"

# ---- IP pública (solo para decirte dónde abrir el dashboard) ----
TOKEN=$(curl -s -m2 -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 120" 2>/dev/null || true)
PUB_IP=$(curl -s -m2 -H "X-aws-ec2-metadata-token: $TOKEN" \
        http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "")

# Publica la IP del master para los workers (archivo local + se puede copiar)
echo "$PRIV_IP" > "$HOME/master-ip.txt"

# ---- Config de Flink: JobManager escuchando en la IP privada, bind 0.0.0.0 ----
cat > "$FLINK_HOME/conf/config.yaml" <<EOF
jobmanager:
  rpc:
    address: $PRIV_IP
    port: 6123
  bind-host: 0.0.0.0
  memory:
    process:
      size: 1600m
rest:
  address: $PRIV_IP
  bind-address: 0.0.0.0
parallelism:
  default: 2
cluster:
  # Reparte las subtareas DE FORMA PAREJA entre todos los TaskManagers (nodos) en vez de
  # llenar uno antes de usar el siguiente. Así el trabajo se divide visiblemente entre las
  # máquinas (cada worker recibe carga) — clave para demostrar el procesamiento distribuido.
  evenly-spread-out-slots: true
EOF
echo "### config.yaml de Flink (JobManager en $PRIV_IP) ###"

echo "### 1/5  Kafka + Redis (Docker, advertised en $PRIV_IP) ###"
cd "$BD_PROJECT"
HOST_LAN_IP="$PRIV_IP" docker compose -f docker-compose.cluster.yml up -d
for i in $(seq 1 25); do
  docker exec bigdata-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break
  sleep 2
done

echo "### 2/5  Topics Kafka (raw-tweets, raw-comments, classified-hate, metrics, alerts) ###"
for t in raw-tweets raw-comments classified-hate metrics alerts; do
  docker exec bigdata-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic "$t" >/dev/null 2>&1
done
echo "    topics OK"

echo "### 3/5  Flink JobManager ###"
"$JAVA_HOME/bin/jps" 2>/dev/null | grep -q StandaloneSessionClusterEntrypoint \
  && echo "    JobManager ya corriendo" \
  || "$FLINK_HOME/bin/jobmanager.sh" start
sleep 3

echo "### 4/5  Spark Master ###"
"$JAVA_HOME/bin/jps" 2>/dev/null | grep -q "org.apache.spark.deploy.master.Master\|Master" \
  && echo "    Spark Master ya corriendo" \
  || SPARK_MASTER_HOST="$PRIV_IP" SPARK_LOCAL_IP="$PRIV_IP" "$SPARK_HOME/sbin/start-master.sh"
sleep 2

echo "### 5/5  Dashboard (puerto 5000) ###"
if curl -s -m3 -o /dev/null http://localhost:5000/ 2>/dev/null; then
  echo "    dashboard ya corriendo"
else
  ( cd "$BD_PROJECT/dashboard" && REDIS_HOST=localhost PYTHONUNBUFFERED=1 \
      nohup "$BD_VENV/bin/python" -u app.py >/tmp/dashboard.log 2>&1 & )
  echo "    dashboard lanzado"
fi

echo ""
echo "============================================================"
echo " MASTER ARRIBA en $PRIV_IP"
echo "   Flink UI : http://${PUB_IP:-<IP-publica>}:8081"
echo "   Spark UI : http://${PUB_IP:-<IP-publica>}:8080"
echo "   Dashboard: http://${PUB_IP:-<IP-publica>}:5000"
echo ""
echo " AHORA, en CADA worker, corre:"
echo "   bash $HERE/arrancar-worker.sh $PRIV_IP"
echo ""
echo " Cuando los 2 workers estén unidos, vuelve aquí y corre:"
echo "   bash $HERE/lanzar-jobs.sh"
echo "============================================================"
