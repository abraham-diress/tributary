#!/usr/bin/env bash
# Starts a standalone ZooKeeper on port 2181 (no separate install needed).
cd "$(dirname "$0")/.."
exec java -cp target/tributary-1.0-SNAPSHOT.jar com.tributary.tools.EmbeddedZooKeeper "${1:-2181}" "${2:-data/zookeeper}"
