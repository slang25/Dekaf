---
sidebar_position: 11
---

# Testing

`Dekaf.Testing` provides an in-memory Kafka cluster so you can test code that uses Dekaf
producers and consumers without Docker, Testcontainers, or any running broker.

```bash
dotnet add package Dekaf.Testing
```

## How it works

Dekaf clients talk to brokers through a typed protocol connection layer. The in-memory
cluster plugs in at that seam — below your producers and consumers, above wire
serialization — much like an `HttpClient` `DelegatingHandler` intercepts typed requests
before any bytes hit a socket. No ports are opened and no I/O occurs; requests are handled
as plain objects against an in-memory log.

Everything above the seam is the real client: batching, partitioning, consumer groups,
rebalancing, offset management, and long-poll fetches all behave as they do in production.

## Producing and consuming

Create one cluster per test and attach clients with `UseInMemoryCluster`:

```csharp
using Dekaf;
using Dekaf.Testing;

var cluster = new InMemoryKafkaCluster();

await using var producer = Kafka.CreateProducer<string, string>()
    .UseInMemoryCluster(cluster)
    .Build();

await producer.ProduceAsync("orders", "order-1", "created");

await using var consumer = Kafka.CreateConsumer<string, string>()
    .UseInMemoryCluster(cluster)
    .WithGroupId("order-processor")
    .WithAutoOffsetReset(AutoOffsetReset.Earliest)
    .Build();

consumer.Subscribe("orders");

var result = await consumer.ConsumeOneAsync(TimeSpan.FromSeconds(5));
Console.WriteLine(result?.Value); // "created"
```

All clients attached to the same `InMemoryKafkaCluster` instance share its topics,
records, and consumer groups. Consumer groups fully work — joins, rebalances,
heartbeats, offset commits — so multi-consumer tests behave like the real thing.

## Inspecting state

Assert on what was produced without spinning up a consumer:

```csharp
var records = cluster.GetRecords("orders");           // all records, in offset order
var highWatermark = cluster.GetHighWatermark("orders");
var committed = cluster.GetCommittedOffset("order-processor", "orders");
```

Topics are auto-created on first use (like a broker with auto-creation enabled).
To control partitioning, create topics up front:

```csharp
var cluster = new InMemoryKafkaCluster(new InMemoryKafkaClusterOptions
{
    AutoCreateTopics = false,
    DefaultPartitionCount = 1
});

cluster.CreateTopic("orders", partitionCount: 3);
```

The `AdminClient` also works against the cluster (`CreateTopicsAsync`, `DeleteTopicsAsync`).

## Fault injection

`RequestInterceptor` sees every typed protocol request before the cluster handles it —
return a response to short-circuit, or null to pass through. This enables failure testing
that is impossible with a container:

```csharp
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

cluster.RequestInterceptor = (request, cancellationToken) =>
{
    if (request is ProduceRequest produce)
    {
        // Fail all produces with a retriable error
        var response = new ProduceResponse
        {
            Responses = produce.TopicData.Select(t => new ProduceResponseTopicData
            {
                Name = t.Name,
                PartitionResponses = t.PartitionData.Select(p => new ProduceResponsePartitionData
                {
                    Index = p.Index,
                    ErrorCode = ErrorCode.NotEnoughReplicas,
                    BaseOffset = -1
                }).ToList()
            }).ToList()
        };
        return ValueTask.FromResult<object?>(response);
    }

    return ValueTask.FromResult<object?>(null);
};
```

You can also delay inside the interceptor to simulate slow brokers, or inject errors
for only the first N requests to test retry behavior.

## Scope

The in-memory cluster models a single broker that leads every partition. It supports
the full produce/fetch data path, consumer group coordination (classic protocol,
client-side assignment), offset commit/fetch, `ListOffsets`, and topic administration.

It does not simulate replication, transactions/exactly-once, quotas, or ACLs. For
verifying behavior against a real broker, keep using integration tests with
Testcontainers — the in-memory cluster is for fast, deterministic unit tests of your
application logic.
