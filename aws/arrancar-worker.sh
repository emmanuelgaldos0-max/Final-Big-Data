#!/usr/bin/env bash
# =============================================================================
# arrancar-worker.sh  —  NODO WORKER del cluster en AWS EC2
# -----------------------------------------------------------------------------
# Aporta el CÓMPUTO del cluster:
#   · Flink TaskManager  -> se une al JobManager del master (4 slots)
#   · Spark Worker       -> se une al Spark Master del master
#
# Se corre en CADA una de las 2 instancias worker.
# Uso:   bash ~/Final-Big-Data-AWS/aws/arrancar-worker.sh <IP_PRIVADA_DEL_MASTER>
# =============================================================================
set -uo pipefail
source "$HOME/.bdenv"

MASTER_IP="${1:-}"
if [ -z "$MASTER_IP" ]; then
  echo "ERROR: falta la IP privada del master."
  echo "Uso: bash arrancar-worker.sh <IP_PRIVADA_DEL_MASTER>"
  echo "(esa IP la imprime arrancar-master.sh; también está en ~/master-ip.txt del master)"
  exit 1
fi

# IP local que realmente alcanza al master (robusto si hay varias interfaces, p.ej. WSL)
PRIV_IP=$(ip -4 route get "$MASTER_IP" 2>/dev/null | grep -oP 'src \K[0-9.]+' | head -1)
[ -z "$PRIV_IP" ] && PRIV_IP=$(hostname -I | awk '{print $1}')
echo "### WORKER · mi IP privada: $PRIV_IP   ·   master: $MASTER_IP ###"

# ---- Config de Flink: apunta al JobManager del master; TM se anuncia con MI IP ----
cat > "$FLINK_HOME/conf/config.yaml" <<EOF
jobmanager:
  rpc:
    address: $MASTER_IP
    port: 6123
taskmanager:
  bind-host: 0.0.0.0
  host: $PRIV_IP
  numberOfTaskSlots: 4
  memory:
    process:
      size: 3072m
parallelism:
  default: 2
EOF

# ---- Esperar a que el JobManager del master responda ----
echo "### Esperando al JobManager del master ($MASTER_IP:8081)... ###"
for i in $(seq 1 18); do
  curl -s -m4 "http://$MASTER_IP:8081/overview" >/dev/null 2>&1 && { echo "    JobManager OK"; break; }
  sleep 5
done

# ---- Flink TaskManager ----
echo "### Flink TaskManager (se une a $MASTER_IP:6123) ###"
"$FLINK_HOME/bin/taskmanager.sh" stop  >/dev/null 2>&1 || true
"$FLINK_HOME/bin/taskmanager.sh" start
sleep 4

# ---- Spark Worker ----
echo "### Spark Worker (se une a spark://$MASTER_IP:7077) ###"
SPARK_LOCAL_IP="$PRIV_IP" "$SPARK_HOME/sbin/start-worker.sh" "spark://$MASTER_IP:7077"
sleep 2

# ---- Verificación ----
echo "### TaskManagers registrados en el cluster: ###"
curl -s -m5 "http://$MASTER_IP:8081/taskmanagers" 2>/dev/null | "$BD_VENV/bin/python" -c "
import sys, json
try:
    n = len(json.load(sys.stdin)['taskmanagers'])
    print(f'    {n} TaskManager(s) — ideal 2 cuando ambos workers estén arriba')
except Exception:
    print('    (no pude leer el JobManager; ¿está arriba el master en '+'$MASTER_IP'+'?)')
"
echo "Listo. Worker $PRIV_IP unido al master $MASTER_IP."
