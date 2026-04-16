#!/usr/bin/env sh
set -eu

BROKER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
MAIN_TOPIC="${KAFKA_TOPIC:-events}"
RETRY_TOPIC="${KAFKA_RETRY_TOPIC:-events.retry}"

echo "[topic-init] waiting for broker ${BROKER}"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BROKER}" --list >/dev/null 2>&1

echo "[topic-init] ensuring topic ${MAIN_TOPIC}"
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BROKER}" \
  --create --if-not-exists \
  --topic "${MAIN_TOPIC}" \
  --partitions 3 \
  --replication-factor 1

echo "[topic-init] ensuring topic ${RETRY_TOPIC}"
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BROKER}" \
  --create --if-not-exists \
  --topic "${RETRY_TOPIC}" \
  --partitions 3 \
  --replication-factor 1

echo "[topic-init] topics ready"
