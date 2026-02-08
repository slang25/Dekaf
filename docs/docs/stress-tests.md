---
sidebar_position: 14
---

# Stress Test Results

Long-running stress tests comparing sustained performance between Dekaf and Confluent.Kafka under real-world load.

**Last Updated:** 2026-02-08 03:22 UTC

:::info
These tests run weekly (Sunday 2 AM UTC) and can be manually triggered. 
They measure sustained performance over 15+ minutes with real Kafka instances.
:::

## Producer Performance (15 min, 1000B)

| Client | Messages/sec | MB/sec | Total | Errors |
|--------|--------------|--------|-------|--------|
| Confluent | 448,426 | 427.65 | 403,669,940 | 39309088 |
| Dekaf | 96,886 | 92.40 | 87,195,773 | 0 |

:::note
Confluent.Kafka is 4.63x faster for producer throughput.
:::

## Consumer Performance (15 min, 1000B)

| Client | Messages/sec | MB/sec | Total | Errors |
|--------|--------------|--------|-------|--------|
| Confluent | 392,537 | 374.35 | 353,283,578 | 0 |
| Dekaf | 2 | 0.00 | 2,042 | 0 |

## Memory & GC Statistics

| Client | Scenario | Gen0 | Gen1 | Gen2 | Allocated |
|--------|----------|------|------|------|-----------|
| Confluent | consumer | 85963 | 526 | 8 | 802.83 GB |
| Confluent | producer | 23887 | 89 | 9 | 4.40 GB |
| Dekaf | consumer | 9260 | 2204 | 299 | 22.32 GB |
| Dekaf | producer | 11955 | 2974 | 264 | 35.12 GB |

---

## About These Tests

Stress tests measure sustained performance over extended periods:

- **Real Kafka**: Tests run against actual Apache Kafka instances
- **Parallel Execution**: Each test runs in its own isolated environment
- **Both Clients**: Direct comparison between Dekaf and Confluent.Kafka
- **Memory Monitoring**: Tracks GC behavior and memory usage over time
- **Error Rates**: Ensures stability under load
