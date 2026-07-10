using System.Collections.Concurrent;
using Dekaf.Networking;
using Dekaf.Protocol.Messages;

namespace Dekaf.Testing;

/// <summary>
/// An in-memory Kafka cluster for testing Dekaf clients without a broker.
/// </summary>
/// <remarks>
/// <para>The cluster intercepts requests at the typed protocol layer (<see cref="IKafkaConnection"/>),
/// below producers/consumers but above wire serialization — no sockets, ports, or containers are involved.</para>
/// <para>Create one cluster per test and attach clients to it with
/// <c>UseInMemoryCluster(cluster)</c> on the producer/consumer/admin builders.
/// All clients attached to the same cluster instance share its topics, records, and consumer groups.</para>
/// <para>The cluster models a single broker (node 1) that leads every partition and
/// coordinates every consumer group.</para>
/// </remarks>
public sealed partial class InMemoryKafkaCluster
{
    internal const int BrokerId = 1;

    internal string HostName { get; } = "in-memory";

    internal int PortNumber { get; } = 9092;

    private readonly ConcurrentDictionary<string, InMemoryTopic> _topics = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, InMemoryConsumerGroup> _groups = new(StringComparer.Ordinal);

    /// <summary>
    /// Creates an in-memory cluster.
    /// </summary>
    /// <param name="options">Optional cluster behavior settings.</param>
    public InMemoryKafkaCluster(InMemoryKafkaClusterOptions? options = null)
    {
        Options = options ?? new InMemoryKafkaClusterOptions();
    }

    /// <summary>
    /// The options this cluster was created with.
    /// </summary>
    public InMemoryKafkaClusterOptions Options { get; }

    /// <summary>
    /// The bootstrap servers string to configure clients with. The builder extension
    /// <c>UseInMemoryCluster</c> sets this automatically.
    /// </summary>
    public string BootstrapServers => $"{HostName}:{PortNumber}";

    /// <summary>
    /// Intercepts requests before the cluster handles them, in the style of an
    /// <c>HttpClient</c> <c>DelegatingHandler</c>. The argument is the typed request
    /// (e.g. <see cref="ProduceRequest"/>, <see cref="FetchRequest"/>). Return a typed
    /// response to short-circuit the cluster, or null to let the cluster handle the
    /// request normally. Delays and injected errors compose naturally:
    /// await before returning null to delay, or return a response carrying an error code.
    /// </summary>
    public Func<object, CancellationToken, ValueTask<object?>>? RequestInterceptor { get; set; }

    /// <summary>
    /// The names of all topics that currently exist.
    /// </summary>
    public IReadOnlyCollection<string> Topics => [.. _topics.Keys];

    /// <summary>
    /// Creates a connection pool bound to this cluster. Pass this method as the
    /// connection pool factory on a client builder; each client gets its own pool,
    /// all sharing this cluster's state.
    /// </summary>
    public IConnectionPool CreateConnectionPool() => new InMemoryConnectionPool(this);

    /// <summary>
    /// Creates a topic with the given number of partitions.
    /// </summary>
    /// <exception cref="InvalidOperationException">The topic already exists.</exception>
    public void CreateTopic(string topic, int partitionCount = 1)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);

        if (!_topics.TryAdd(topic, new InMemoryTopic(topic, partitionCount)))
            throw new InvalidOperationException($"Topic '{topic}' already exists.");
    }

    /// <summary>
    /// Deletes a topic and all its records. Returns false if the topic did not exist.
    /// </summary>
    public bool DeleteTopic(string topic) => _topics.TryRemove(topic, out _);

    /// <summary>
    /// Returns a snapshot of all records in a partition, in offset order.
    /// </summary>
    public IReadOnlyList<InMemoryRecord> GetRecords(string topic, int partition = 0)
        => GetExistingPartition(topic, partition).Snapshot();

    /// <summary>
    /// Returns the high watermark (next offset to be written) of a partition.
    /// </summary>
    public long GetHighWatermark(string topic, int partition = 0)
        => GetExistingPartition(topic, partition).HighWatermark;

    /// <summary>
    /// Returns the number of partitions of a topic.
    /// </summary>
    public int GetPartitionCount(string topic)
        => GetExistingTopic(topic).Partitions.Count;

    /// <summary>
    /// Returns the committed offset for a consumer group on a partition,
    /// or null if the group has not committed one.
    /// </summary>
    public long? GetCommittedOffset(string groupId, string topic, int partition = 0)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        return _groups.TryGetValue(groupId, out var group)
            ? group.GetCommittedOffset(topic, partition)
            : null;
    }

    internal InMemoryTopic? GetTopic(string name)
        => _topics.TryGetValue(name, out var topic) ? topic : null;

    internal InMemoryTopic GetOrCreateTopic(string name)
        => _topics.GetOrAdd(name, static (n, o) => new InMemoryTopic(n, o.DefaultPartitionCount), Options);

    internal IReadOnlyList<InMemoryTopic> SnapshotTopics() => [.. _topics.Values];

    internal InMemoryConsumerGroup GetGroup(string groupId)
        => _groups.GetOrAdd(groupId, static (id, cluster) => new InMemoryConsumerGroup(id, cluster.Options), this);

    private InMemoryTopic GetExistingTopic(string topic)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        return GetTopic(topic) ?? throw new InvalidOperationException($"Topic '{topic}' does not exist.");
    }

    private InMemoryPartition GetExistingPartition(string topic, int partition)
        => GetExistingTopic(topic).GetPartition(partition)
            ?? throw new ArgumentOutOfRangeException(nameof(partition), $"Topic '{topic}' has no partition {partition}.");

    /// <summary>
    /// Routes a typed protocol request to its handler. This is the single entry point
    /// used by every in-memory connection.
    /// </summary>
    internal async ValueTask<object> HandleRequestAsync(object request, CancellationToken cancellationToken)
    {
        if (RequestInterceptor is { } interceptor)
        {
            var overridden = await interceptor(request, cancellationToken).ConfigureAwait(false);
            if (overridden is not null)
                return overridden;
        }

        return request switch
        {
            ApiVersionsRequest => HandleApiVersions(),
            MetadataRequest r => HandleMetadata(r),
            ProduceRequest r => HandleProduce(r),
            FetchRequest r => await HandleFetchAsync(r, cancellationToken).ConfigureAwait(false),
            ListOffsetsRequest r => HandleListOffsets(r),
            CreateTopicsRequest r => HandleCreateTopics(r),
            DeleteTopicsRequest r => HandleDeleteTopics(r),
            FindCoordinatorRequest r => HandleFindCoordinator(r),
            JoinGroupRequest r => await GetGroup(r.GroupId).JoinAsync(r, cancellationToken).ConfigureAwait(false),
            SyncGroupRequest r => await GetGroup(r.GroupId).SyncAsync(r, cancellationToken).ConfigureAwait(false),
            HeartbeatRequest r => GetGroup(r.GroupId).Heartbeat(r),
            LeaveGroupRequest r => GetGroup(r.GroupId).Leave(r),
            OffsetCommitRequest r => GetGroup(r.GroupId).CommitOffsets(r),
            OffsetFetchRequest r => HandleOffsetFetch(r),
            _ => throw new NotSupportedException(
                $"The in-memory Kafka cluster does not support {request.GetType().Name}. " +
                "Use InMemoryKafkaCluster.RequestInterceptor to handle it, or run this test against a real broker.")
        };
    }
}

/// <summary>
/// Behavior settings for <see cref="InMemoryKafkaCluster"/>.
/// </summary>
public sealed class InMemoryKafkaClusterOptions
{
    /// <summary>
    /// Whether topics are created automatically when first referenced by a client
    /// (via metadata requests, mirroring broker-side auto-creation). Default is true.
    /// </summary>
    public bool AutoCreateTopics { get; init; } = true;

    /// <summary>
    /// The number of partitions for auto-created topics. Default is 1.
    /// </summary>
    public int DefaultPartitionCount { get; init; } = 1;

    /// <summary>
    /// How long the group coordinator waits for all known members to rejoin during a
    /// rebalance before evicting the stragglers. Default is 10 seconds.
    /// </summary>
    public TimeSpan RebalanceTimeout { get; init; } = TimeSpan.FromSeconds(10);
}
