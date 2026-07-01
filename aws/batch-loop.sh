#!/usr/bin/env bash
# =============================================================================
# batch-loop.sh  —  Planificador: re-ejecuta los 5 jobs Spark cada N minutos
# -----------------------------------------------------------------------------
# Corre en el MASTER, en segundo plano. Cada N minutos llama a lanzar-batch.sh,
# que regenera los reportes JSON que muestra el panel "Análisis batch" del dashboard.
#
# Uso (desacoplado, sobrevive al cierre del SSH):
#   nohup bash ~/Final-Big-Data-AWS/aws/batch-loop.sh 10 > ~/batch-loop.log 2>&1 &
#
#   (el argumento es el intervalo en minutos; por defecto 10)
#
# Para detenerlo:   pkill -f batch-loop.sh
# =============================================================================
set -uo pipefail
source "$HOME/.bdenv"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

INTERVAL_MIN="${1:-10}"
export BATCH_INTERVAL_MIN="$INTERVAL_MIN"

echo "### batch-loop iniciado — cada ${INTERVAL_MIN} min — $(date -u +%H:%M:%SZ) ###"
echo "### (PID $$ · detener con: pkill -f batch-loop.sh) ###"

while true; do
  echo "===== corrida batch $(date -u +%H:%M:%SZ) ====="
  bash "$HERE/lanzar-batch.sh" || echo "!!! lanzar-batch.sh devolvió error (sigo en bucle)"
  echo "===== fin corrida; durmiendo ${INTERVAL_MIN} min ====="
  sleep "$(( INTERVAL_MIN * 60 ))"
done
