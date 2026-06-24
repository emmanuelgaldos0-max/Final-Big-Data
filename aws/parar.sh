#!/usr/bin/env bash
# =============================================================================
# parar.sh  —  Detiene los servicios del proyecto en ESTA instancia
# -----------------------------------------------------------------------------
# Corre en cualquier nodo. Detiene Flink/Spark/dashboard/productor y (en el master)
# los contenedores Kafka+Redis. NO apaga ni termina la instancia EC2: eso se hace
# desde la consola de AWS (botón Stop o Terminate) — ver la guía.
#
# OJO COSTOS: detener servicios NO deja de cobrar la instancia. Para no gastar
# créditos de AWS Academy debes hacer STOP (o TERMINATE) la instancia en la consola.
# =============================================================================
set -uo pipefail
source "$HOME/.bdenv" 2>/dev/null || true

echo "### Deteniendo servicios en esta instancia... ###"
pkill -f "producer_dataset.py" 2>/dev/null && echo "  productor detenido" || true
pkill -f "dashboard/app.py"    2>/dev/null && echo "  dashboard detenido" || true
"$FLINK_HOME/bin/taskmanager.sh" stop  >/dev/null 2>&1 && echo "  Flink TaskManager detenido" || true
"$FLINK_HOME/bin/jobmanager.sh"  stop  >/dev/null 2>&1 && echo "  Flink JobManager detenido" || true
"$SPARK_HOME/sbin/stop-worker.sh"      >/dev/null 2>&1 && echo "  Spark Worker detenido" || true
"$SPARK_HOME/sbin/stop-master.sh"      >/dev/null 2>&1 && echo "  Spark Master detenido" || true
if [ -f "$BD_PROJECT/docker-compose.cluster.yml" ]; then
  ( cd "$BD_PROJECT" && docker compose -f docker-compose.cluster.yml down >/dev/null 2>&1 ) \
    && echo "  Kafka + Redis detenidos" || true
fi
echo "### Listo. Recuerda hacer STOP/TERMINATE de la instancia en la consola de AWS. ###"
