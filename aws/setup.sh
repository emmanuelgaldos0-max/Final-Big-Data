#!/usr/bin/env bash
# =============================================================================
# setup.sh  —  Instalador universal del cluster en AWS EC2 (Ubuntu 22.04 o 24.04)
# -----------------------------------------------------------------------------
# Se corre UNA vez en CADA instancia (master y los 2 workers). Es idéntico en las
# tres: instala Java 11, Python 3.10 (nativo en Ubuntu 22.04), Docker, y descarga
# las distribuciones de Flink 1.19.1 y Spark 3.5.1 + el conector Kafka. Luego crea
# el entorno Python (venv) con las dependencias probadas del proyecto.
#
# Uso:   bash ~/Final-Big-Data-AWS/aws/setup.sh
#
# No necesita argumentos: el rol (master/worker) se decide al ARRANCAR, no al
# instalar. Idempotente: puedes volver a correrlo sin romper nada.
# =============================================================================
set -euo pipefail

FLINK_VER="1.19.1"
SPARK_VER="3.5.1"
CONN_VER="3.2.0-1.19"          # flink-sql-connector-kafka
# Rutas DERIVADAS de la ubicación del script -> funciona se clone como se clone
# (Final-Big-Data, Final-Big-Data-AWS, etc.). El script vive en <repo>/aws/.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$HERE/.." && pwd)"
PROJECT="$REPO_DIR/bigdata-proyecto"
VENV="$PROJECT/.venv"
FLINK="$HOME/flink"
SPARK="$HOME/spark"

echo "############################################################"
echo "#  SETUP cluster Big Data en EC2 (Ubuntu 22.04)            #"
echo "#  Flink $FLINK_VER · Spark $SPARK_VER · Python 3.10        #"
echo "############################################################"

if [ ! -d "$PROJECT" ]; then
  echo "ERROR: no encuentro el proyecto en $PROJECT"
  echo "Primero trae el código (ver GUIA-AWS-ACADEMY.md, paso 'clonar repo')."
  exit 1
fi

# ---- 1. Paquetes del sistema ----
echo ""; echo "### 1/5  Paquetes del sistema (Java 11, Python 3.10, Docker) ###"
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    software-properties-common ca-certificates unzip curl wget >/dev/null
# Python 3.10: nativo en Ubuntu 22.04; en 24.04 (Python 3.12) lo traemos del PPA
# deadsnakes, porque PyFlink 1.19 NO soporta 3.12. Así el setup sirve en ambas.
if ! apt-cache show python3.10 >/dev/null 2>&1; then
  echo "    python3.10 no está en los repos -> agrego PPA deadsnakes"
  sudo add-apt-repository -y ppa:deadsnakes/ppa >/dev/null 2>&1
  sudo apt-get update -qq
fi
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    openjdk-11-jdk python3.10 python3.10-venv python3.10-dev python3-pip \
    docker.io >/dev/null
sudo systemctl enable --now docker >/dev/null 2>&1 || true
sudo usermod -aG docker "$USER" || true     # para usar docker sin sudo (tras re-login)

JAVA_HOME_DIR="/usr/lib/jvm/java-11-openjdk-amd64"
echo "    Java: $($JAVA_HOME_DIR/bin/java -version 2>&1 | head -1)"
echo "    Python: $(python3.10 --version)"

# ---- 2. Distribución de Flink ----
echo ""; echo "### 2/5  Apache Flink $FLINK_VER ###"
if [ ! -d "$FLINK" ]; then
  cd "$HOME"
  wget -q "https://archive.apache.org/dist/flink/flink-${FLINK_VER}/flink-${FLINK_VER}-bin-scala_2.12.tgz"
  tar xzf "flink-${FLINK_VER}-bin-scala_2.12.tgz"
  ln -sfn "$HOME/flink-${FLINK_VER}" "$FLINK"
  rm -f "flink-${FLINK_VER}-bin-scala_2.12.tgz"
  echo "    Flink instalado en $FLINK"
else
  echo "    Flink ya estaba en $FLINK"
fi

# Conector Kafka como JAR dentro de flink/lib (lo necesita PyFlink)
JAR="$FLINK/lib/flink-sql-connector-kafka-${CONN_VER}.jar"
if [ ! -f "$JAR" ]; then
  # Preferir el JAR que viaja en el repo; si no, descargarlo de Maven
  if [ -f "$PROJECT/libs/flink-sql-connector-kafka-${CONN_VER}.jar" ]; then
    cp "$PROJECT/libs/flink-sql-connector-kafka-${CONN_VER}.jar" "$JAR"
    echo "    Conector Kafka copiado desde el repo"
  else
    wget -q -O "$JAR" \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${CONN_VER}/flink-sql-connector-kafka-${CONN_VER}.jar"
    echo "    Conector Kafka descargado de Maven"
  fi
fi

# ---- 3. Distribución de Spark ----
echo ""; echo "### 3/5  Apache Spark $SPARK_VER ###"
if [ ! -d "$SPARK" ]; then
  cd "$HOME"
  wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VER}/spark-${SPARK_VER}-bin-hadoop3.tgz"
  tar xzf "spark-${SPARK_VER}-bin-hadoop3.tgz"
  ln -sfn "$HOME/spark-${SPARK_VER}-bin-hadoop3" "$SPARK"
  rm -f "spark-${SPARK_VER}-bin-hadoop3.tgz"
  echo "    Spark instalado en $SPARK"
else
  echo "    Spark ya estaba en $SPARK"
fi

# ---- 4. Entorno Python (venv) ----
echo ""; echo "### 4/5  Entorno Python (venv) + dependencias ###"
if [ ! -d "$VENV" ]; then
  python3.10 -m venv "$VENV"
fi
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q -r "$HERE/requirements-cluster.txt"
"$VENV/bin/python" -c "import pyflink, pyspark, redis, flask, kafka; print('    deps OK')"

# ---- 5. Variables de entorno persistentes ----
echo ""; echo "### 5/5  Variables de entorno (~/.bdenv) ###"
cat > "$HOME/.bdenv" <<EOF
# Entorno del cluster Big Data (cargar con: source ~/.bdenv)
export JAVA_HOME="$JAVA_HOME_DIR"
export FLINK_HOME="$FLINK"
export SPARK_HOME="$SPARK"
export BD_PROJECT="$PROJECT"
export BD_VENV="$VENV"
export PATH="\$FLINK_HOME/bin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$PATH"
EOF
grep -q 'source ~/.bdenv' "$HOME/.bashrc" 2>/dev/null || echo 'source ~/.bdenv' >> "$HOME/.bashrc"

echo ""
echo "============================================================"
echo " LISTO. Instalación completa en esta instancia."
echo " Importante: cierra y reabre el SSH (o corre 'newgrp docker')"
echo " para que 'docker' funcione sin sudo."
echo ""
echo " Siguiente paso:"
echo "   - En el MASTER:  bash $HERE/arrancar-master.sh"
echo "   - En cada WORKER: bash $HERE/arrancar-worker.sh <IP_PRIVADA_MASTER>"
echo "============================================================"
