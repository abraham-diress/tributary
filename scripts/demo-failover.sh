#!/usr/bin/env bash
# End-to-end fault-tolerance demo using real processes:
#   1. start ZooKeeper + 3 brokers
#   2. create a topic with replication factor 3 and write 500 messages with acks=all
#   3. kill -9 the partition leader (a crash, not a clean shutdown)
#   4. write 500 more messages; the client finds the newly elected leader on its own
#   5. read everything back and verify all 1000 acknowledged messages survived, in order
#   6. restart the crashed broker and watch it catch up and rejoin the ISR
set -euo pipefail
cd "$(dirname "$0")/.."
JAR=target/tributary-1.0-SNAPSHOT.jar
[ -f "$JAR" ] || mvn -q package -DskipTests

WORK=$(mktemp -d -t tributary-demo)
LOGS=$WORK/logs
mkdir -p "$LOGS"
ALL_PIDS=()
declare -a BROKER_PID
cleanup() {
  for p in "${ALL_PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

CLI="java -cp $JAR com.tributary.tools.Cli"
BOOT=localhost:9091,localhost:9092,localhost:9093
step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
start_broker() {
  java -cp "$JAR" com.tributary.broker.TributaryBroker "$1" localhost $((9090 + $1)) 2181 "$WORK/data" >>"$LOGS/broker-$1.log" 2>&1 &
  BROKER_PID[$1]=$!
  ALL_PIDS+=($!)
}
leader_of_demo() { $CLI describe --bootstrap "$BOOT" --topic demo | awk '/partition 0/ {print $4}'; }

step "Starting ZooKeeper and 3 brokers (logs in $LOGS)"
java -cp "$JAR" com.tributary.tools.EmbeddedZooKeeper 2181 "$WORK/zk" >"$LOGS/zookeeper.log" 2>&1 &
ALL_PIDS+=($!)
sleep 2
for id in 1 2 3; do start_broker "$id"; done
sleep 3

step "Creating topic 'demo' (1 partition, replication factor 3)"
$CLI create-topic --bootstrap "$BOOT" --topic demo --partitions 1 --replication-factor 3
$CLI describe --bootstrap "$BOOT"

step "Producing msg-1..msg-500 with acks=all"
seq 1 500 | sed 's/^/msg-/' | $CLI produce --bootstrap "$BOOT" --topic demo --acks all

LEADER=$(leader_of_demo)
step "Crashing the leader: kill -9 broker $LEADER"
kill -9 "${BROKER_PID[$LEADER]}"
echo "Waiting for ZooKeeper to expire broker $LEADER's session and the controller to elect a new leader..."
for _ in $(seq 1 30); do
  NEW=$(leader_of_demo 2>/dev/null || true)
  if [ -n "$NEW" ] && [ "$NEW" != "$LEADER" ] && [ "$NEW" != "none" ]; then break; fi
  sleep 1
done
$CLI describe --bootstrap "$BOOT" --topic demo

step "Producing msg-501..msg-1000 with acks=all to the new leader"
seq 501 1000 | sed 's/^/msg-/' | $CLI produce --bootstrap "$BOOT" --topic demo --acks all

step "Consuming everything back"
OUT=$WORK/consumed.txt
$CLI consume --bootstrap "$BOOT" --topic demo --idle-timeout-ms 3000 >"$OUT"
TOTAL=$(wc -l <"$OUT" | tr -d ' ')
UNIQUE=$(sed 's/.*] //' "$OUT" | sort -u | wc -l | tr -d ' ')
EXPECTED=$(seq 1 1000 | sed 's/^/msg-/')
if [ "$(sed 's/.*] //' "$OUT")" = "$EXPECTED" ]; then ORDER=yes; else ORDER=no; fi
echo "consumed=$TOTAL unique=$UNIQUE in_order=$ORDER"
head -2 "$OUT"; echo "..."; tail -2 "$OUT"

step "Restarting broker $LEADER; it should truncate to its high watermark, catch up, and rejoin the ISR"
start_broker "$LEADER"
for _ in $(seq 1 30); do
  if $CLI describe --bootstrap "$BOOT" --topic demo | grep -q "isr \[[0-9], [0-9], [0-9]\]"; then break; fi
  sleep 1
done
$CLI describe --bootstrap "$BOOT" --topic demo

if [ "$UNIQUE" = 1000 ] && [ "$ORDER" = yes ]; then
  printf '\n\033[32mPASS: all 1000 acknowledged messages survived the leader crash, in order.\033[0m\n'
else
  printf '\n\033[31mFAIL: expected 1000 unique in-order messages.\033[0m\n'
  exit 1
fi
