#!/usr/bin/env bash
# Fault-tolerance demo with real processes:
#   1. start ZooKeeper and 3 brokers
#   2. create a topic (3 partitions, each copied to all 3 brokers) and send 10 messages
#   3. kill -9 broker 1
#   4. send 10 more messages; the client finds the new leaders on its own
#   5. read every partition back and check that all 20 messages are there
set -euo pipefail
cd "$(dirname "$0")/.."
JAR=target/tributary-1.0-SNAPSHOT.jar
[ -f "$JAR" ] || mvn -q package -DskipTests

WORK=$(mktemp -d -t tributary-demo)
PIDS=()
cleanup() {
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT
step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

step "Starting ZooKeeper and 3 brokers (logs in $WORK)"
java -cp "$JAR" com.tributary.tools.EmbeddedZooKeeper 2181 "$WORK/zk" >"$WORK/zookeeper.log" 2>&1 &
PIDS+=($!)
sleep 2
declare -a BROKER
for id in 1 2 3; do
  java -cp "$JAR" com.tributary.broker.TributaryBroker "$id" localhost $((9090 + id)) 2181 "$WORK/data" >"$WORK/broker-$id.log" 2>&1 &
  BROKER[$id]=$!
  PIDS+=($!)
done
sleep 4

step "Creating topic 'demo' and sending 10 messages"
java -cp "$JAR" com.tributary.client.TributaryProducer localhost 9092 demo 10

step "Crashing broker 1 with kill -9"
kill -9 "${BROKER[1]}"
echo "Waiting for ZooKeeper to notice and the controller to pick new leaders..."
sleep 10

step "Sending 10 more messages"
java -cp "$JAR" com.tributary.client.TributaryProducer localhost 9092 demo 10

step "Reading every partition back"
OUT="$WORK/consumed.txt"
: >"$OUT"
for p in 0 1 2; do
  java -cp "$JAR" com.tributary.client.TributaryConsumer localhost 9092 demo "$p" >>"$OUT" 2>/dev/null &
  CPID=$!
  sleep 4
  kill "$CPID" 2>/dev/null || true
done
cat "$OUT"
COUNT=$(grep -c "^Offset" "$OUT" || true)

if [ "$COUNT" = 20 ]; then
  printf '\n\033[32mPASS: all 20 messages survived the crash.\033[0m\n'
else
  printf '\n\033[31mFAIL: expected 20 messages, found %s.\033[0m\n' "$COUNT"
  exit 1
fi

