#!/usr/bin/env bash
set -euo pipefail
BROKER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
KAFKA_CLI="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"
for topic in transcripciones_raw transcripciones_procesadas alertas; do
  "$KAFKA_CLI" --bootstrap-server "$BROKER" --create --if-not-exists \
    --topic "$topic" --partitions "${KAFKA_PARTITIONS:-3}" --replication-factor 1
done
