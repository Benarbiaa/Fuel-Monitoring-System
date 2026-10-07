#!/usr/bin/env bash
# Starts a single-node Kafka broker (KRaft mode, no ZooKeeper) in Docker for
# local development. Safe to re-run: if a "kafka" container already exists
# (running or stopped), it's removed first so this always ends in a fresh,
# correctly-configured container.
#
# Usage:
#   ./scripts/start-kafka.sh          # start with existing data
#   ./scripts/start-kafka.sh --reset  # wipe .kafka-data and start clean

set -euo pipefail

cd "$(dirname "$0")/.."  # run from project root regardless of caller's cwd

CONTAINER_NAME="kafka"
DATA_DIR="$(pwd)/.kafka-data"
IMAGE="apache/kafka:3.9.0"

if [[ "${1:-}" == "--reset" ]]; then
  echo "Resetting: removing existing container and $DATA_DIR ..."
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  rm -rf "$DATA_DIR"
elif docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo "Removing existing '$CONTAINER_NAME' container..."
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1
fi

mkdir -p "$DATA_DIR"

echo "Starting Kafka (KRaft, single broker) on localhost:9092 ..."
docker run -d \
  --name "$CONTAINER_NAME" \
  -p 9092:9092 \
  -v "$DATA_DIR:/var/lib/kafka/data" \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e CLUSTER_ID=fuel-monitor-dev-cluster \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  "$IMAGE"

echo "Waiting for broker to come up..."
sleep 5
docker logs "$CONTAINER_NAME" --tail 10

echo ""
echo "Kafka is up. Creating topics (safe to ignore 'already exists' errors)..."

BIN=/opt/kafka/bin/kafka-topics.sh

docker exec "$CONTAINER_NAME" "$BIN" \
  --create --if-not-exists \
  --topic fuel.readings.raw \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker exec "$CONTAINER_NAME" "$BIN" \
  --create --if-not-exists \
  --topic fuel.readings.dlq \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

echo ""
echo "Topics:"
docker exec "$CONTAINER_NAME" "$BIN" --list --bootstrap-server localhost:9092

echo ""
echo "Done. Broker reachable at localhost:9092."