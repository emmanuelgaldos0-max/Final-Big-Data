#!/usr/bin/env bash
# =============================================================================
# verificar.sh  —  Chequeo rápido de salud del cluster (correr en el MASTER)
# =============================================================================
set -uo pipefail
source "$HOME/.bdenv" 2>/dev/null || true
PRIV_IP=$(hostname -I | awk '{print $1}')
PY="$BD_VENV/bin/python"

echo "===== SALUD DEL CLUSTER (master $PRIV_IP) ====="
echo "-- Docker (Kafka + Redis) --"
docker ps --format '  {{.Names}}  {{.Status}}' 2>/dev/null | grep -E 'kafka|redis' || echo "  (sin contenedores)"
echo "-- Procesos Java (jps) --"
"$JAVA_HOME/bin/jps" 2>/dev/null | grep -vE 'Jps$' | sed 's/^/  /' || true
echo "-- Flink: slots y jobs --"
curl -s -m4 "http://localhost:8081/overview" 2>/dev/null | "$PY" -c "
import sys,json
d=json.load(sys.stdin)
print(f\"  TaskManagers: {d.get('taskmanagers',0)}  ·  slots {d.get('slots-total',0)} (libres {d.get('slots-available',0)})  ·  jobs RUNNING {d.get('jobs-running',0)}\")
" 2>/dev/null || echo "  (JobManager no responde)"
echo "-- Spark workers --"
curl -s -m4 "http://localhost:8080/json/" 2>/dev/null | "$PY" -c "
import sys,json
d=json.load(sys.stdin)
print(f\"  workers ALIVE: {sum(1 for w in d.get('workers',[]) if w.get('state')=='ALIVE')}  ·  cores {d.get('cores',0)}\")
" 2>/dev/null || echo "  (Spark Master no responde)"
echo "-- Redis: métricas del pipeline --"
docker exec bigdata-redis redis-cli -h localhost get metrics:total_processed 2>/dev/null | sed 's/^/  total_processed: /' || echo "  (Redis no responde)"
echo "-- Dashboard --"
echo "  HTTP / -> $(curl -s -m4 -o /dev/null -w '%{http_code}' http://localhost:5000/ 2>/dev/null || echo 'sin respuesta')"
echo "==============================================="
