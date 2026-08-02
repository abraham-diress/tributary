# Tributary

*A distributed, replicated message log written from scratch in Java.*

Producers write messages to topics. Each topic is split into partitions, and each partition is stored on disk and copied to several brokers. If a broker crashes, another broker takes over its partitions and no acknowledged messages are lost. ZooKeeper tracks which brokers are alive and which broker leads each partition.

```
 Producer ──send──▶ Leader broker ──replicate──▶ Follower brokers
                        │
 Consumer ◀──fetch──────┘

 ZooKeeper: live brokers, controller election, partition leaders
```

## How it works

**Storage.** Each partition is an append-only log on disk, split into 1 MB segments. A segment is a `.log` file holding messages as `[length][bytes]`, plus an `.index` file that maps each offset to its position in the log. Finding the right segment is a binary search, and finding a message inside it is one index lookup. On restart, a broker rebuilds its state from the files and cuts off any message that was only partly written during a crash.

**Wire protocol.** Clients and brokers talk over TCP using a compact binary format. Every message is a length-prefixed frame whose first byte is the request type: produce, fetch, metadata, create topic, replicate, or topic notification.

**Cluster coordination.** Each broker registers an ephemeral node in ZooKeeper, which disappears automatically if the broker dies. The first broker to create the `/controller` node becomes the controller. The controller creates topics, spreads partition replicas across brokers round-robin, and picks a new leader when a broker goes down. If the controller itself dies, another broker takes over.

**Replication.** Every partition has one leader and some followers. The leader appends each message to its log and sends it to all live followers in parallel, and only answers the producer once they have acknowledged it. Followers accept only the next offset they expect. If a follower missed messages, for example because it was restarted, the leader sends it the missing ones first.

**Clients.** The client asks any broker for cluster metadata and then sends each request straight to the partition's leader. If a request fails, it refreshes the metadata and retries, which is how it follows a leader that moved. A broker that receives a request for a partition it doesn't lead forwards it to the leader.

## Running it

Requires Java 17+ and Maven. ZooKeeper is embedded, so nothing else needs installing.

```bash
mvn package                     # builds the jar and runs the tests

scripts/start-zookeeper.sh      # terminal 1
scripts/start-broker.sh 1       # terminal 2 (port 9091)
scripts/start-broker.sh 2       # terminal 3 (port 9092)
scripts/start-broker.sh 3       # terminal 4 (port 9093)
```

Send 10 messages (this creates the topic if it doesn't exist):

```bash
java -cp target/tributary-1.0-SNAPSHOT.jar com.tributary.client.TributaryProducer localhost 9091 test-topic
```

Read partition 0:

```bash
java -cp target/tributary-1.0-SNAPSHOT.jar com.tributary.client.TributaryConsumer localhost 9091 test-topic 0
```

Data is stored under `data/<brokerId>/<topic>/<partition>/`.

### Crash demo

`scripts/demo-failover.sh` starts ZooKeeper and 3 brokers as separate processes, sends 10 messages, kills broker 1 with `kill -9`, sends 10 more, then reads every partition back:

```
==> Crashing broker 1 with kill -9
==> Sending 10 more messages
Sent 'Message 0' to partition 0 at offset 4
...
PASS: all 20 messages survived the crash.
```

## Tests

```bash
mvn test
```

- `PartitionTest` covers appending and reading, segment rollover, reading across segments, restart recovery, and cutting off a partly written message.
- `ProtocolTest` checks request and response encoding.
- `ClusterTest` runs ZooKeeper and 3 brokers in one JVM. It checks controller election, producing and consuming across partitions, replication to every follower, electing a new leader without losing messages, a restarted broker catching up, and controller failover.

## Project layout

```
src/main/java/com/tributary/
  broker/   TributaryBroker, Partition, Protocol, ZookeeperClient, BrokerInfo
  client/   TributaryClient, TributaryProducer, TributaryConsumer
  tools/    EmbeddedZooKeeper
scripts/    start-zookeeper.sh, start-broker.sh, demo-failover.sh
```

## Limitations

- Every write is flushed to disk and replicated before the producer gets an answer, and each request opens a new connection. That keeps it simple and safe but limits throughput.
- When a leader fails, the controller promotes the first live replica. Replicas aren't tracked as in sync or not, so if a follower that had fallen behind is promoted, messages it was missing can be lost.
- Consumers track their own offsets; there are no consumer groups or committed offsets.
- There is no retention, so logs grow until deleted by hand.

