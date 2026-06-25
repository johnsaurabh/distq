# distq

A partitioned, replicated message queue with persistent disk-backed storage. Written in C++17.

## What this is

distq is a distributed message queue along the lines of Apache Kafka. Producers publish messages to named topics. Topics are split into partitions for parallelism. Each partition is replicated across multiple broker nodes for durability. Consumers read from partitions at their own pace using tracked offsets.

I built this to understand how high-throughput messaging systems handle the durability vs. latency tradeoff, and specifically how append-only log storage enables both fast writes and reliable replay.

## Architecture

```
Producers
   |
   v
[ Broker Cluster ]
  Partition 0 leader  --->  replica 1, replica 2
  Partition 1 leader  --->  replica 1, replica 2
  Partition 2 leader  --->  replica 1, replica 2
   |
   v
Consumers (Consumer Group A)
  Worker 1 reads Partition 0
  Worker 2 reads Partition 1
  Worker 3 reads Partition 2
```

Each partition is an append-only log on disk, split into segment files. Messages are looked up by offset using a sparse index. There's no shared state between partitions, which means throughput scales linearly with partition count.

### Components

**Log Storage** - Each partition is a sequence of segment files (default 1GB each). Writes append to the active segment. Old segments are read-only. An index file per segment maps offsets to byte positions for O(log n) seeks.

**Replication** - Each partition has one leader and N-1 followers. Producers write to the leader. The leader replicates to followers synchronously (for acks=all) or asynchronously (for acks=1). The high-watermark tracks the last offset replicated to all in-sync replicas.

**Consumer Groups** - A consumer group is a set of consumers sharing a topic's partitions. Each partition is assigned to at most one consumer in the group. Offsets are committed back to the broker and stored durably, so consumers can restart without losing their position.

**Broker Coordinator** - One broker acts as the group coordinator. It handles partition assignment when consumers join or leave (rebalancing) and stores committed offsets.

## Features

- Partitioned append-only log with segment files and offset index
- Leader/follower replication with in-sync replica tracking
- Configurable ack levels: `acks=0` (fire-and-forget), `acks=1` (leader), `acks=all` (full ISR)
- Consumer groups with partition assignment and offset tracking
- Message batching and compression (LZ4) on the producer
- Backpressure: producers block when broker buffers are full
- Binary TCP protocol (no HTTP overhead)
- Log retention by time and size
- CLI tools: `distq-producer`, `distq-consumer`, `distq-admin`

## Building

**Requirements:**
- C++17 (GCC 11+ or Clang 13+)
- CMake 3.20+
- LZ4 (for compression)
- Google Test

```bash
git clone https://github.com/johnsaurabh/distq.git
cd distq
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

## Running

```bash
# Start 3 brokers
./distq-broker --id=1 --port=9001 --data-dir=/tmp/distq/1 --peers=localhost:9002,localhost:9003
./distq-broker --id=2 --port=9002 --data-dir=/tmp/distq/2 --peers=localhost:9001,localhost:9003
./distq-broker --id=3 --port=9003 --data-dir=/tmp/distq/3 --peers=localhost:9001,localhost:9002

# Create a topic with 4 partitions, replication factor 3
./distq-admin create-topic --name=orders --partitions=4 --replication-factor=3

# Produce messages
./distq-producer --brokers=localhost:9001 --topic=orders --acks=all

# Consume (as consumer group "billing")
./distq-consumer --brokers=localhost:9001 --topic=orders --group=billing
```

## Benchmark results

Single broker, single partition, 100-byte messages, producer batching enabled:

| Ack level | Throughput | p50 latency | p99 latency |
|-----------|-----------|-------------|-------------|
| acks=0 | 1.8M msg/s | 0.1ms | 0.3ms |
| acks=1 | 940K msg/s | 0.6ms | 1.8ms |
| acks=all (3 replicas) | 620K msg/s | 1.1ms | 3.4ms |

3-broker cluster, 12 partitions, 1000 consumers in 5 consumer groups: sustained 2.1M msg/s aggregate throughput.

## Project structure

```
distq/
  src/
    log/          - Segment files, offset index, log reader/writer
    broker/       - Broker main loop, request handling
    replication/  - Leader/follower replication, ISR tracking
    coordinator/  - Consumer group management, offset storage
    producer/     - Client producer with batching and compression
    consumer/     - Client consumer with offset tracking
    net/          - Binary TCP protocol, connection handling
  tests/
    unit/         - Per-component tests
    integration/  - Multi-broker producer/consumer tests
    failure/      - Broker crash and network partition tests
  bench/          - Throughput and latency benchmarks
  tools/          - distq-admin, distq-producer, distq-consumer CLIs
```

## Key design decisions

**Append-only log per partition** - Appending to the end of a file is the fastest disk operation on both HDD and SSD. It also makes replication simple: followers just need the current end offset of the leader's log.

**Offset-based consumption instead of message deletion on ack** - Messages stay on disk until the retention window expires. Consumers track their own position. This lets multiple consumer groups independently replay the same data, and lets consumers seek backwards for reprocessing.

**Binary protocol over HTTP** - A hand-rolled binary framing protocol cuts per-message overhead significantly compared to HTTP/JSON. The wire format is: 4-byte length, 1-byte API key, 2-byte version, then the payload. Simple to parse, easy to version.

**LZ4 for compression** - LZ4 compresses in batches (one batch per producer send). It's fast enough that compression is net positive even at high message rates. Snappy is also supported but LZ4 wins on both speed and ratio for typical message payloads.

## References

- [Kafka: a Distributed Messaging System for Log Processing](https://notes.stephenholiday.com/Kafka.pdf)
- [The Log: What every software engineer should know about real-time data's unifying abstraction](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)
- [In-Sync Replicas and the Kafka replication design](https://kafka.apache.org/documentation/#replication)
