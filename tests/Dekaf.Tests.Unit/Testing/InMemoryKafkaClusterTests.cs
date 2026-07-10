using System.Text;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Errors;
using Dekaf.Producer;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;
using Dekaf.Serialization;
using Dekaf.Testing;

namespace Dekaf.Tests.Unit.Testing;

public class InMemoryKafkaClusterTests
{
    [Test]
    public async Task ProduceAndConsume_WithManualAssign_RoundTrips()
    {
        var cluster = new InMemoryKafkaCluster();

        await using var producer = Kafka.CreateProducer<string, string>()
            .UseInMemoryCluster(cluster)
            .Build();

        await producer.ProduceAsync("orders", "k1", "v1");
        await producer.ProduceAsync("orders", "k2", "v2");
        await producer.ProduceAsync("orders", "k3", "v3");

        await using var consumer = Kafka.CreateConsumer<string, string>()
            .UseInMemoryCluster(cluster)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .Build();

        consumer.Assign(new TopicPartition("orders", 0));

        for (var i = 1; i <= 3; i++)
        {
            var result = await consumer.ConsumeOneAsync(TimeSpan.FromSeconds(10));
            await Assert.That(result).IsNotNull();
            await Assert.That(result!.Value.Key).IsEqualTo($"k{i}");
            await Assert.That(result.Value.Value).IsEqualTo($"v{i}");
            await Assert.That(result.Value.Offset).IsEqualTo(i - 1);
        }
    }

    [Test]
    public async Task Produce_NullKeyAndHeaders_VisibleViaClusterInspection()
    {
        var cluster = new InMemoryKafkaCluster();

        await using var producer = Kafka.CreateProducer<string, string>()
            .UseInMemoryCluster(cluster)
            .Build();

        var message = new ProducerMessage<string, string>
        {
            Topic = "audit",
            Key = null,
            Value = "payload",
            Headers = Headers.Create("trace-id", "abc-123")
        };
        await producer.ProduceAsync(message);

        await Assert.That(cluster.GetHighWatermark("audit")).IsEqualTo(1);

        var records = cluster.GetRecords("audit");
        await Assert.That(records).Count().IsEqualTo(1);
        await Assert.That(records[0].Key).IsNull();
        await Assert.That(Encoding.UTF8.GetString(records[0].Value!)).IsEqualTo("payload");
        await Assert.That(records[0].Headers).Count().IsEqualTo(1);
        await Assert.That(records[0].Headers[0].Key).IsEqualTo("trace-id");
        await Assert.That(Encoding.UTF8.GetString(records[0].Headers[0].Value!)).IsEqualTo("abc-123");
    }

    [Test]
    public async Task FireAndForgetSend_WithAcksNone_Delivers()
    {
        var cluster = new InMemoryKafkaCluster();

        await using var producer = Kafka.CreateProducer<string, string>()
            .UseInMemoryCluster(cluster)
            .WithAcks(Acks.None)
            .Build();

        producer.Send("events", "key", "value");
        await producer.FlushAsync();

        // FlushAsync only drains the accumulator; a Send may still be in the worker
        // channel when it returns, so allow a bounded wait for delivery.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline &&
               (!cluster.Topics.Contains("events") || cluster.GetHighWatermark("events") < 1))
        {
            await Task.Delay(20);
        }

        await Assert.That(cluster.GetHighWatermark("events")).IsEqualTo(1);
    }

    [Test]
    public async Task ConsumerGroup_SubscribeConsumeCommit_PersistsOffsets()
    {
        var cluster = new InMemoryKafkaCluster();

        await using var producer = Kafka.CreateProducer<string, string>()
            .UseInMemoryCluster(cluster)
            .Build();

        for (var i = 0; i < 5; i++)
            await producer.ProduceAsync("payments", $"k{i}", $"v{i}");

        await using (var consumer = Kafka.CreateConsumer<string, string>()
            .UseInMemoryCluster(cluster)
            .WithGroupId("payment-processor")
            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithOffsetCommitMode(OffsetCommitMode.Manual)
            .Build())
        {
            consumer.Subscribe("payments");

            for (var i = 0; i < 5; i++)
            {
                var result = await consumer.ConsumeOneAsync(TimeSpan.FromSeconds(10));
                await Assert.That(result).IsNotNull();
                await Assert.That(result!.Value.Value).IsEqualTo($"v{i}");
            }

            await consumer.CommitAsync();
        }

        await Assert.That(cluster.GetCommittedOffset("payment-processor", "payments")).IsEqualTo(5);
    }

    [Test]
    public async Task ConsumerGroup_SecondConsumer_ResumesFromCommittedOffset()
    {
        var cluster = new InMemoryKafkaCluster();

        await using var producer = Kafka.CreateProducer<string, string>()
            .UseInMemoryCluster(cluster)
            .Build();

        await producer.ProduceAsync("inventory", "k1", "old-1");
        await producer.ProduceAsync("inventory", "k2", "old-2");

        await using (var first = Kafka.CreateConsumer<string, string>()
            .UseInMemoryCluster(cluster)
            .WithGroupId("inventory-workers")
            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithOffsetCommitMode(OffsetCommitMode.Manual)
            .Build())
        {
            first.Subscribe("inventory");
            await first.ConsumeOneAsync(TimeSpan.FromSeconds(10));
            await first.ConsumeOneAsync(TimeSpan.FromSeconds(10));
            await first.CommitAsync();
        }

        await producer.ProduceAsync("inventory", "k3", "new-1");

        await using var second = Kafka.CreateConsumer<string, string>()
            .UseInMemoryCluster(cluster)
            .WithGroupId("inventory-workers")
            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithOffsetCommitMode(OffsetCommitMode.Manual)
            .Build();

        second.Subscribe("inventory");
        var result = await second.ConsumeOneAsync(TimeSpan.FromSeconds(10));

        await Assert.That(result).IsNotNull();
        await Assert.That(result!.Value.Value).IsEqualTo("new-1");
        await Assert.That(result.Value.Offset).IsEqualTo(2);
    }

    [Test]
    public async Task Consumer_LongPoll_WakesWhenMessageProducedAfterFetchStarts()
    {
        var cluster = new InMemoryKafkaCluster();

        await using var consumer = Kafka.CreateConsumer<string, string>()
            .UseInMemoryCluster(cluster)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .Build();

        consumer.Assign(new TopicPartition("alerts", 0));

        var consumeTask = consumer.ConsumeOneAsync(TimeSpan.FromSeconds(10)).AsTask();

        await using var producer = Kafka.CreateProducer<string, string>()
            .UseInMemoryCluster(cluster)
            .Build();

        await Task.Delay(300);
        await producer.ProduceAsync("alerts", "k", "wake-up");

        var result = await consumeTask;
        await Assert.That(result).IsNotNull();
        await Assert.That(result!.Value.Value).IsEqualTo("wake-up");
    }

    [Test]
    public async Task MultiplePartitions_KeyedMessages_AllConsumable()
    {
        var cluster = new InMemoryKafkaCluster();
        cluster.CreateTopic("sharded", partitionCount: 3);

        await using var producer = Kafka.CreateProducer<string, string>()
            .UseInMemoryCluster(cluster)
            .Build();

        for (var i = 0; i < 9; i++)
            await producer.ProduceAsync("sharded", $"key-{i}", $"value-{i}");

        var totalStored = 0L;
        for (var partition = 0; partition < 3; partition++)
            totalStored += cluster.GetHighWatermark("sharded", partition);
        await Assert.That(totalStored).IsEqualTo(9);

        await using var consumer = Kafka.CreateConsumer<string, string>()
            .UseInMemoryCluster(cluster)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .Build();

        consumer.Assign(
            new TopicPartition("sharded", 0),
            new TopicPartition("sharded", 1),
            new TopicPartition("sharded", 2));

        var seen = new HashSet<string>();
        while (seen.Count < 9)
        {
            var result = await consumer.ConsumeOneAsync(TimeSpan.FromSeconds(10));
            await Assert.That(result).IsNotNull();
            seen.Add(result!.Value.Value!);
        }

        await Assert.That(seen.Count).IsEqualTo(9);
    }

    [Test]
    public async Task AdminClient_CreateAndDeleteTopics_UpdatesCluster()
    {
        var cluster = new InMemoryKafkaCluster(new InMemoryKafkaClusterOptions { AutoCreateTopics = false });

        await using var admin = new AdminClientBuilder()
            .UseInMemoryCluster(cluster)
            .Build();

        await admin.CreateTopicsAsync([new NewTopic { Name = "managed", NumPartitions = 3 }]);

        await Assert.That(cluster.Topics).Contains("managed");
        await Assert.That(cluster.GetPartitionCount("managed")).IsEqualTo(3);

        await admin.DeleteTopicsAsync(["managed"]);
        await Assert.That(cluster.Topics).DoesNotContain("managed");
    }

    [Test]
    public async Task RequestInterceptor_CanInjectProduceErrors()
    {
        var cluster = new InMemoryKafkaCluster();
        cluster.RequestInterceptor = (request, _) =>
        {
            if (request is not ProduceRequest produce)
                return ValueTask.FromResult<object?>(null);

            // Fail every partition in the request, DelegatingHandler-style.
            var response = new ProduceResponse
            {
                Responses = produce.TopicData
                    .Select(topic => new ProduceResponseTopicData
                    {
                        Name = topic.Name,
                        PartitionResponses = topic.PartitionData
                            .Select(partition => new ProduceResponsePartitionData
                            {
                                Index = partition.Index,
                                ErrorCode = ErrorCode.NotEnoughReplicas,
                                BaseOffset = -1
                            })
                            .ToList()
                    })
                    .ToList()
            };
            return ValueTask.FromResult<object?>(response);
        };

        await using var producer = Kafka.CreateProducer<string, string>()
            .UseInMemoryCluster(cluster)
            .Build();

        KafkaException? caught = null;
        try
        {
            await producer.ProduceAsync("doomed", "k", "v");
        }
        catch (KafkaException ex)
        {
            caught = ex;
        }

        await Assert.That(caught).IsNotNull();
        await Assert.That(cluster.GetHighWatermark("doomed")).IsEqualTo(0);
    }

    [Test]
    public async Task ConsumerGroup_TwoConsumers_RebalanceSplitsPartitions()
    {
        var cluster = new InMemoryKafkaCluster();
        cluster.CreateTopic("workload", partitionCount: 2);

        ConsumerOptions BuildOptions() => new()
        {
            BootstrapServers = [cluster.BootstrapServers],
            GroupId = "workload-team",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            OffsetCommitMode = OffsetCommitMode.Manual,
            HeartbeatIntervalMs = 100,
            FetchMaxWaitMs = 100,
            ConnectionPoolFactory = cluster.CreateConnectionPool
        };

        await using var first = new KafkaConsumer<string, string>(BuildOptions(), Serializers.String, Serializers.String);
        await using var second = new KafkaConsumer<string, string>(BuildOptions(), Serializers.String, Serializers.String);
        first.Subscribe("workload");
        second.Subscribe("workload");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var consumed = new System.Collections.Concurrent.ConcurrentBag<string>();

        async Task PollLoopAsync(IKafkaConsumer<string, string> consumer)
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = await consumer.ConsumeOneAsync(TimeSpan.FromMilliseconds(250), cts.Token);
                    if (result is { IsPartitionEof: false, Value: not null })
                        consumed.Add(result.Value.Value);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        var firstLoop = Task.Run(() => PollLoopAsync(first));
        var secondLoop = Task.Run(() => PollLoopAsync(second));

        // Wait for the cooperative rebalance to settle: one partition each.
        while (!cts.Token.IsCancellationRequested &&
               (first.Assignment.Count != 1 || second.Assignment.Count != 1))
        {
            await Task.Delay(50);
        }

        await Assert.That(first.Assignment.Count).IsEqualTo(1);
        await Assert.That(second.Assignment.Count).IsEqualTo(1);
        await Assert.That(first.Assignment.Single()).IsNotEqualTo(second.Assignment.Single());

        await using var producer = Kafka.CreateProducer<string, string>()
            .UseInMemoryCluster(cluster)
            .Build();

        for (var i = 0; i < 10; i++)
            await producer.ProduceAsync("workload", $"key-{i}", $"value-{i}");

        while (!cts.Token.IsCancellationRequested && consumed.Count < 10)
            await Task.Delay(50);

        cts.Cancel();
        await Task.WhenAll(firstLoop, secondLoop);

        await Assert.That(consumed.Count).IsEqualTo(10);
        await Assert.That(consumed.Distinct().Count()).IsEqualTo(10);
    }

    [Test]
    public async Task AutoOffsetResetLatest_SkipsExistingRecords()
    {
        var cluster = new InMemoryKafkaCluster();

        await using var producer = Kafka.CreateProducer<string, string>()
            .UseInMemoryCluster(cluster)
            .Build();

        await producer.ProduceAsync("stream", "k1", "old");

        await using var consumer = Kafka.CreateConsumer<string, string>()
            .UseInMemoryCluster(cluster)
            .WithAutoOffsetReset(AutoOffsetReset.Latest)
            .Build();

        consumer.Assign(new TopicPartition("stream", 0));

        var consumeTask = consumer.ConsumeOneAsync(TimeSpan.FromSeconds(10)).AsTask();
        await Task.Delay(300);
        await producer.ProduceAsync("stream", "k2", "new");

        var result = await consumeTask;
        await Assert.That(result).IsNotNull();
        await Assert.That(result!.Value.Value).IsEqualTo("new");
        await Assert.That(result.Value.Offset).IsEqualTo(1);
    }
}
