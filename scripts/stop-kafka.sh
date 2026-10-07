#!/usr/bin/env bash
# Stops and removes the local Kafka dev container. Data in .kafka-data is
# preserved unless you also delete that folder (or run start-kafka.sh --reset).

set -euo pipefail

docker rm -f kafka >/dev/null 2>&1 && echo "Kafka container stopped and removed." \
  || echo "No running 'kafka' container found."