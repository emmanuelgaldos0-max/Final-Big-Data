#!/usr/bin/env bash
# =============================================================================
# lanzar-batch.sh  —  Ejecuta UNA pasada de los 5 jobs batch de Spark (en el MASTER)
# -----------------------------------------------------------------------------
# Corre los 5 jobs Spark de forma DISTRIBUIDA (executors en los 2 workers, vía el
# Spark Master) leyendo el corpus clasificado del topic Kafka 'classified-hate'.
# Cada job escribe su reporte JSON en bigdata-proyecto/data/reports/, y este script
# escribe data/reports/_batch_status.json con el estado (lo lee el dashboard).
#
# Uso:   bash ~/Final-Big-Data-AWS/aws/lanzar-batch.sh
#        BATCH_INTERVAL_MIN=10 bash ... (solo para informar el intervalo al dashboard)
#
# Para ejecutarlo periódicamente usa  aws/batch-loop.sh  (lo llama en bucle).
# =============================================================================
set -uo pipefail
source "$HOME/.bdenv"

PRIV_IP=$(hostname -I | awk '{print $1}')
PY="$BD_VENV/bin/python"
JOBS_DIR="$BD_PROJECT/spark-jobs"
REPORTS="$BD_PROJECT/data/reports"
STATUS="$REPORTS/_batch_status.json"
INTERVAL_MIN="${BATCH_INTERVAL_MIN:-0}"
mkdir -p "$REPORTS"

# Entorno común para que Spark corra DISTRIBUIDO y lea de Kafka:
export SPARK_MASTER="spark://$PRIV_IP:7077"
export CLASSIFIED_SOURCE="${CLASSIFIED_SOURCE:-kafka}"
export KAFKA_BOOTSTRAP="$PRIV_IP:9092"
export FS_DEFAULT=local
export PYSPARK_PYTHON="$PY"
export SPARK_LOCAL_IP="$PRIV_IP"

now(){ date -u +%Y-%m-%dT%H:%M:%SZ; }

# Escribe el JSON de estado (running, tiempos, resultado por job). Recibe los datos
# por variables de entorno para no pelear con el quoting de bash.
write_status(){
  RUNNING="$1" STARTED="$2" FINISHED="$3" DURATION="$4" INTERVAL="$INTERVAL_MIN" \
  JOBS_JSON="$5" SOURCE_MODE="$CLASSIFIED_SOURCE" "$PY" - "$STATUS" <<'PYEOF'
import json, os, sys
out = sys.argv[1]
data = {
    "running": os.environ["RUNNING"] == "1",
    "last_run_started": os.environ.get("STARTED") or None,
    "last_run_finished": os.environ.get("FINISHED") or None,
    "duration_s": int(os.environ["DURATION"]) if os.environ.get("DURATION") else None,
    "interval_min": int(os.environ["INTERVAL"]) if os.environ.get("INTERVAL", "0") != "0" else None,
    "source_mode": os.environ.get("SOURCE_MODE"),
    "jobs": json.loads(os.environ.get("JOBS_JSON") or "[]"),
}
with open(out, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
PYEOF
}

# Lista de jobs: clave | archivo | reporte que produce
JOBS=(
  "historical|spark_job1_historical_analysis.py|historical_hate_report.json"
  "tfidf|spark_job2_tfidf_keywords.py|tfidf_keywords.json"
  "graph|spark_job3_network_graph.py|cooccurrence_graph.json"
  "sentiment|spark_job4_sentiment_report.py|sentiment_by_party.json"
  "users|spark_job5_user_profiling.py|user_profiles.json"
)

STARTED="$(now)"
echo "########## BATCH SPARK — master $PRIV_IP — $STARTED ##########"
echo ">>> SPARK_MASTER=$SPARK_MASTER · fuente=$CLASSIFIED_SOURCE"
# Marca 'running' al inicio (el dashboard muestra el pulso verde)
write_status 1 "$STARTED" "" "" "[]"

t0=$(date +%s)
results=()       # fragmentos JSON por job
for entry in "${JOBS[@]}"; do
  IFS='|' read -r key file report <<< "$entry"
  echo ">>> [$key] $file ..."
  js=$(date +%s)
  if "$PY" "$JOBS_DIR/$file" >"/tmp/batch_$key.log" 2>&1; then
    ok=true;  echo "    OK ($key) en $(( $(date +%s) - js ))s"
  else
    ok=false; echo "    FALLO ($key) — ver /tmp/batch_$key.log"; tail -3 "/tmp/batch_$key.log" | sed 's/^/      /'
  fi
  dur=$(( $(date +%s) - js ))
  results+=("{\"key\":\"$key\",\"report\":\"$report\",\"ok\":$ok,\"seconds\":$dur}")
done

FINISHED="$(now)"
DURATION=$(( $(date +%s) - t0 ))
JOBS_JSON="[$(IFS=,; echo "${results[*]}")]"
write_status 0 "$STARTED" "$FINISHED" "$DURATION" "$JOBS_JSON"

echo "########## BATCH LISTO en ${DURATION}s — reportes en $REPORTS ##########"
ls -1 "$REPORTS"/*.json 2>/dev/null | sed 's/^/  /'
