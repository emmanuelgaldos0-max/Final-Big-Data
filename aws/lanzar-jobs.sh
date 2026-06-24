#!/usr/bin/env bash
# =============================================================================
# lanzar-jobs.sh  —  Somete los 5 jobs Flink y arranca el productor (en el MASTER)
# -----------------------------------------------------------------------------
# Correr DESPUÉS de que el master y los 2 workers estén arriba (8 slots totales).
# Reparte los 5 jobs streaming sobre los TaskManagers de los workers y empieza a
# inyectar el corpus REAL (data/corpus_real.jsonl, 16.6k textos multi-origen).
# Uso:   bash ~/Final-Big-Data-AWS/aws/lanzar-jobs.sh
# =============================================================================
set -uo pipefail
source "$HOME/.bdenv"

PRIV_IP=$(hostname -I | awk '{print $1}')
F="$FLINK_HOME/bin/flink"
PY="$BD_VENV/bin/python"
D="$BD_PROJECT/flink-jobs"
N="$BD_PROJECT/nlp/nlp_pipeline.py"
export KAFKA_BOOTSTRAP="$PRIV_IP:9092" REDIS_HOST="$PRIV_IP"

echo "########## LANZAR JOBS — master $PRIV_IP ##########"

# ---- 1) Esperar a tener los 8 slots (2 workers x 4) ----
echo ">>> Esperando 8 slots (los 2 workers)..."
slots=0
for i in $(seq 1 24); do
  slots=$(curl -s -m4 "http://localhost:8081/overview" 2>/dev/null | "$PY" -c "import sys,json;print(json.load(sys.stdin).get('slots-total',0))" 2>/dev/null || echo 0)
  [ "${slots:-0}" -ge 8 ] && break
  sleep 5
done
echo ">>> Slots totales: ${slots:-0}"
if [ "${slots:-0}" -lt 8 ]; then
  echo "!!! Aún no hay 8 slots. ¿Corriste arrancar-worker.sh en LOS DOS workers?"
  echo "    Puedes seguir igual (los jobs esperarán recursos), pero lo ideal es 8."
fi

# ---- 2) Cancelar jobs previos (re-lanzar limpio) ----
echo ">>> Cancelando jobs previos..."
for j in $(curl -s "http://localhost:8081/jobs" 2>/dev/null | "$PY" -c "import sys,json;[print(x['id']) for x in json.load(sys.stdin).get('jobs',[]) if x['status'] in ('RUNNING','RESTARTING')]" 2>/dev/null); do
  "$F" cancel "$j" >/dev/null 2>&1
done
sleep 3

# ---- 3) Someter los 5 jobs (dimensionados a 8 slots: 2+2+2+1+1) ----
sub(){ "$F" run -d -p "$2" -pyexec "$PY" -pyclientexec "$PY" -py "$D/$1" -pyfs "$D/flink_common.py$3" 2>&1 | grep -iE "JobID|Error" | head -1; }
echo ">>> Sometiendo 5 jobs streaming..."
sub flink_job1_hate_detector.py    2 ",$N";  sleep 6
sub flink_job3_latency_monitor.py  2 "";      sleep 6
sub flink_job2_realtime_counter.py 2 "";      sleep 6
sub flink_job5_topic_classifier.py 1 ",$N";  sleep 6
sub flink_job4_alert_system.py     1 ""

# ---- 4) Productor de datos REALES ----
if pgrep -f "producer_dataset.py" >/dev/null 2>&1; then
  echo ">>> Productor ya corriendo."
else
  echo ">>> Arrancando productor (corpus real, caudal VARIABLE ~200 msg/s)..."
  ( cd "$BD_PROJECT/producers" && KAFKA_BOOTSTRAP="$PRIV_IP:9092" PYTHONUNBUFFERED=1 \
      nohup "$PY" -u producer_dataset.py --rate 200 --vary >/tmp/producer.log 2>&1 & )
fi

echo ""
echo "########## LISTO ##########"
echo " 5 jobs Flink corriendo + productor inyectando datos reales."
echo " Mira el dashboard (IP pública del master, puerto 5000)."
echo " Estado de jobs: $F list   ·   Flink UI: http://<IP-publica>:8081"
echo ""
echo " (Opcional) jobs batch Spark DISTRIBUIDOS (executors en los workers, NO local):"
echo "   SPARK_MASTER=spark://$PRIV_IP:7077 CLASSIFIED_SOURCE=kafka \\"
echo "     KAFKA_BOOTSTRAP=$PRIV_IP:9092 FS_DEFAULT=local \\"
echo "     $BD_VENV/bin/python $BD_PROJECT/spark-jobs/spark_job1_historical_analysis.py"
echo "   (SPARK_MASTER=spark://... reparte el trabajo entre los workers; local[*] NO lo haría)"
