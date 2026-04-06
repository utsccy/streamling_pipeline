#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-host.docker.internal:9092}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
KAFKA_IMAGE="${KAFKA_IMAGE:-confluentinc/cp-kafka:7.7.0}"
DOCKER_NETWORK="${DOCKER_NETWORK:-}"

docker_args=(run --rm)

if [[ -n "$DOCKER_NETWORK" ]]; then
  docker_args+=(--network "$DOCKER_NETWORK")
fi

run_kafka_topics() {
  local topic="$1"
  local partitions="$2"
  local cleanup_policy="$3"
  shift 3

  docker "${docker_args[@]}" "$KAFKA_IMAGE" kafka-topics \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config "cleanup.policy=$cleanup_policy" \
    --config retention.ms=604800000 \
    "$@"
}

run_kafka_topics "input-events" 3 "delete"
run_kafka_topics "processed-events" 3 "delete"
run_kafka_topics "event-counts" 1 "compact,delete" \
  --config min.cleanable.dirty.ratio=0.1
