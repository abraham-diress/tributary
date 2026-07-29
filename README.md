# Tributary

*A distributed, fault-tolerant event streaming log, inspired by Apache Kafka.*

A distributed, replicated commit log built from scratch in Java, modeled on Apache Kafka's architecture. Brokers store partitioned topics as segmented, checksummed logs on disk. Leaders replicate to followers using an in-sync replica (ISR) set and a high watermark, and a ZooKeeper-elected controller fails partitions over to a new leader when a broker dies.

**Results:** up to ~420K msgs/s produce throughput on a 3-broker cluster with 3x replication. Acknowledged writes survive a `kill -9` of the partition leader with no data loss. 30 unit and integration tests.

```
  Producer ──produce──▶ Leader (broker 2) ◀──fetch── Follower (broker 3)
                           │   ▲                     Follower (broker 1)
  Consumer ◀──fetch────────┘   │ high watermark = min(log end offset of ISR)
                               │
            ZooKeeper: broker liveness, controller election, partition state
```

## Features

| Area | What's implemented |
|---|---|
| **Storage** | Append-only log split into 1 MB segments (`00000000000000000000.log` + `.index`). A sparse offset index with binary search gives O(log n) seeks. Every record has a CRC32. On restart, recovery truncates torn writes. Retention is by time or size. |
| **Wire protocol** | Length-prefixed binary frames over TCP: produce, fetch, metadata, create-topic, offset commit/fetch. Typed error codes, with each one marked retriable or not. |
| **Replication** | Pull-based: followers long-poll the leader with fetch requests. The leader tracks each follower's log end offset, maintains the ISR (adding followers that catch up, dropping laggers after `replica.lag.max.ms`), and advances the **high watermark**. Consumers only ever see committed data. |
| **Durability modes** | `acks=0/1/all`, plus `min.insync.replicas`. `acks=all` waits until every in-sync replica has the write. |
| **Fault tolerance** | Brokers register ephemeral ZooKeeper nodes. A controller (elected through an ephemeral `/controller` node) watches for broker failures and elects a new leader from the ISR. **Leader epochs** fence stale followers. There is no unclean election, so an out-of-sync replica is never promoted. All ZooKeeper writes are compare-and-set. |
| **Controller failover** | If the controller dies, another broker takes over and reconciles all partition state. |
| **Producer** | Async and batching, with `linger.ms` and `batch.size`. Keyed records are partitioned with murmur2 (the same hash Kafka uses), so per-key order is preserved. Unkeyed records go round-robin. Back-pressure via a bounded buffer. Ordering is preserved because each partition has at most one batch in flight. |
| **Consumer** | Long-poll fetches, consumer-group offset commits, `seek`, and auto-reset to earliest/latest on out-of-range offsets. |
| **Client** | Bootstraps from any broker, caches metadata, and routes requests to partition leaders. On `NOT_LEADER` or a dead broker it refreshes metadata and retries. |
| **Tooling** | CLI (`create-topic`, `describe`, `produce`, `consume`), throughput benchmark, embedded ZooKeeper, one-JVM local cluster, and a scripted crash/failover demo. |

## Quick start

Requires Java 17+ and Maven. ZooKeeper is embedded, so there is nothing else to install.

```bash
mvn package                       # builds target/tributary-1.0-SNAPSHOT.jar and runs the tests

scripts/start-zookeeper.sh        # terminal 1
scripts/start-broker.sh 1         # terminal 2  (port 9091)
scripts/start-broker.sh 2         # terminal 3  (port 9092)
scripts/start-broker.sh 3         # terminal 4  (port 9093)

scripts/tributary-cli.sh create-topic --topic orders --partitions 3 --replication-factor 3
scripts/tributary-cli.sh describe
echo -e "alice:order-1\nbob:order-2" | scripts/tributary-cli.sh produce --topic orders --key-separator : --acks all
scripts/tributary-cli.sh consume --topic orders --group billing
```

As in the original tutorial, you can also run `java -cp target/tributary-1.0-SNAPSHOT.jar com.tributary.client.TributaryProducer localhost 9091 test-topic` and `... TributaryConsumer localhost 9091 test-topic 0`.

### Crash-recovery demo

`scripts/demo-failover.sh` starts ZooKeeper and 3 broker **processes**, writes 500 messages with `acks=all`, **`kill -9`s the partition leader**, writes 500 more, then reads everything back:

```
==> Crashing the leader: kill -9 broker 2
  partition 0   leader 3   epoch 1   replicas [2, 3, 1]    isr [3, 1]
==> Consuming everything back
consumed=1000 unique=1000 in_order=yes
==> Restarting broker 2; it should truncate to its high watermark, catch up, and rejoin the ISR
  partition 0   leader 3   epoch 1   replicas [2, 3, 1]    isr [2, 3, 1]
PASS: all 1000 acknowledged messages survived the leader crash, in order.
```

## Benchmarks

Setup: 3 brokers on one Apple M3 Pro laptop, a topic with 6 partitions and replication factor 3, `linger.ms=5`, 64 KB batches. Run with `java -cp target/tributary-1.0-SNAPSHOT.jar com.tributary.tools.Benchmark`.

| Record size | acks | Produce | Consume |
|---|---|---|---|
| 100 B (1M records) | 1   | **~420K msgs/s** (40 MB/s) | ~5M msgs/s |
| 100 B (1M records) | all | **~320K msgs/s** (31 MB/s) | ~5M msgs/s |
| 1 KB (200K records) | 1   | ~48K msgs/s (47 MB/s) | ~1.2 GB/s |
| 1 KB (200K records) | all | ~39K msgs/s (38 MB/s) | ~1.3 GB/s |

Producer latency in these runs mostly reflects queueing, because the benchmark sends as fast as it can. Consumer numbers are loopback reads served from the OS page cache. Everything runs on one machine, so treat these as relative numbers rather than production figures.

## Design notes

**Why pull-based replication?** A follower that falls behind (or restarts) catches up at its own pace using the same fetch path consumers use. The offset a follower fetches at doubles as its acknowledgment, which is how the leader learns follower progress without extra messages.

**High watermark.** `HW = min(log end offset over the ISR)`. Records below the HW exist on every in-sync replica, so they survive any single failure the ISR can tolerate. Consumers read only up to the HW, so they never see a record that a leader failover could later erase. When a broker becomes a follower, it truncates its log to its HW (checkpointed to disk every second) and re-fetches anything newer from the new leader.

**Leader epochs and CAS.** Every leadership change bumps the partition's epoch. Brokers ignore stale ZooKeeper notifications, and a follower fetching with the wrong epoch is fenced. Both the controller (on failover) and partition leaders (ISR changes) write the same ZooKeeper node, so every write is compare-and-set on the node version.

**Single metadata thread.** ZooKeeper callbacks are queued onto one thread per broker, so leader/follower transitions and log truncation for a partition never race.

**Crash safety.** Records are self-describing (`offset | size | crc | timestamp | key | value`). On startup the active segment is scanned and every record's CRC is verified. Anything after the first bad record, such as a torn write, is truncated and the index rebuilt. Fetch threads are never interrupted, because interrupting Java NIO `FileChannel` I/O closes the channel.

### Compared with the tutorial

This project follows the 8-stage [*Build Your Own Kafka*](https://buildthingsuseful.medium.com/building-your-own-kafka-like-system-from-scratch-a-step-by-step-guide-d3c5f0a303c0) series: the same broker/client split, ZooKeeper layout, and core classes (the tutorial's `SimpleKafkaBroker`, `SimpleKafkaClient`, `SimpleKafkaProducer` and `SimpleKafkaConsumer` became `TributaryBroker`, `TributaryClient`, `TributaryProducer` and `TributaryConsumer`, alongside `Partition`, `ZookeeperClient` and `Protocol`). It goes further in several places:

- **Framing.** The tutorial reads requests into a fixed 1 KB buffer, which breaks on larger messages. Here every message is length-prefixed.
- **Sparse index.** The tutorial's index has one entry per message. Here it's a true sparse index with binary search.
- **Replication.** The tutorial pushes fire-and-forget copies to followers. Here followers pull, with ISR tracking, a high watermark, leader epochs, `acks=all`, and follower truncation.
- **Durability.** Added CRC32 checksums, torn-write recovery, and retention.
- **Producer.** Added async batching and key-based partitioning.
- **Consumer.** Added consumer-group offsets and long polling.
- **Tests.** Added integration tests that run a real 3-broker cluster through leader failover, controller failover, and broker rejoin.

## Project layout

```
src/main/java/com/tributary/
  storage/   Log, LogSegment, OffsetIndex, Records (on-disk format), LogConfig
  common/    Protocol (wire format), Errors, BrokerInfo, TopicPartition
  broker/    TributaryBroker, Partition (ISR/HW), ReplicaFetcher, Controller, ZookeeperClient, PartitionState
  client/    TributaryClient, TributaryProducer, TributaryConsumer
  tools/     Cli, Benchmark, EmbeddedZooKeeper, LocalCluster
src/test/java/...  LogTest, ProtocolTest, ControllerTest, ClusterIntegrationTest
scripts/           start-zookeeper.sh, start-broker.sh, tributary-cli.sh, demo-failover.sh
```

## Testing

```bash
mvn test
```

- **Unit tests** cover segment rolling, index lookups, torn-write and CRC-corruption recovery, truncation, retention, protocol round-trips, and the leader-election rules.
- **Integration tests** start embedded ZooKeeper plus 3 brokers in-process. They verify replicated produce/consume with per-key ordering, leader failover with zero loss of acknowledged writes, a restarted broker rejoining the ISR, controller failover, consumer-group resume, and long-poll latency.

## Limitations / future work

- **Consumer groups.** Partition assignment is static (one partition or all of them). There is no group membership or rebalancing protocol yet.
- **Delivery guarantees.** Delivery is at-least-once. A produce retried after a network error can duplicate a record, since there is no idempotent producer.
- **Truncation edge case.** Followers truncate to their high watermark rather than using leader-epoch-based truncation (Kafka's KIP-101), which can re-fetch more data than strictly necessary.
- **Networking.** The broker uses one thread per connection rather than an NIO selector loop, and there is no zero-copy `sendfile` path.
- **Metadata store.** Metadata lives in ZooKeeper. Modern Kafka has replaced ZooKeeper with KRaft, a built-in Raft quorum.
