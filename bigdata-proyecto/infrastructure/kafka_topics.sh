#!/bin/bash
# =============================================================================
# kafka_topics.sh — Crea los topics necesarios
# =============================================================================

KAFKA_BIN=/opt/kafka/bin
BOOTSTRAP=localhost:9092

echo "Creando topic: raw-tweets"
$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --replication-factor 2 \
  --partitions 3 \
  --topic raw-tweets

echo "Creando topic: raw-comments"
$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --replication-factor 2 \
  --partitions 3 \
  --topic raw-comments

echo "Creando topic: classified-hate"
$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --replication-factor 2 \
  --partitions 3 \
  --topic classified-hate

echo "Creando topic: metrics"
$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --replication-factor 1 \
  --partitions 1 \
  --topic metrics

echo "Creando topic: alerts"
$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --replication-factor 1 \
  --partitions 1 \
  --topic alerts

echo ""
echo "Topics creados:"
$KAFKA_BIN/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP
