#!/usr/bin/env bash
# Usage: scripts/start-broker.sh <brokerId> [port] [zkPort] [dataDir]
cd "$(dirname "$0")/.."
ID=${1:?broker id required}
exec java -cp target/tributary-1.0-SNAPSHOT.jar com.tributary.broker.TributaryBroker \
  "$ID" localhost "${2:-$((9090 + ID))}" "${3:-2181}" "${4:-data}"

