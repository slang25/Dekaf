using Dekaf.Admin;
using Dekaf.Testing;

namespace Dekaf;

/// <summary>
/// Builder extensions that attach Dekaf clients to an <see cref="InMemoryKafkaCluster"/>.
/// </summary>
public static class InMemoryKafkaClusterExtensions
{
    /// <summary>
    /// Connects the producer to an in-memory cluster instead of real brokers.
    /// Sets the bootstrap servers and connection pool; no network I/O occurs.
    /// </summary>
    public static ProducerBuilder<TKey, TValue> UseInMemoryCluster<TKey, TValue>(
        this ProducerBuilder<TKey, TValue> builder,
        InMemoryKafkaCluster cluster)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(cluster);
        return builder
            .WithBootstrapServers(cluster.BootstrapServers)
            .WithConnectionPoolFactory(cluster.CreateConnectionPool);
    }

    /// <summary>
    /// Connects the consumer to an in-memory cluster instead of real brokers.
    /// Sets the bootstrap servers and connection pool; no network I/O occurs.
    /// </summary>
    public static ConsumerBuilder<TKey, TValue> UseInMemoryCluster<TKey, TValue>(
        this ConsumerBuilder<TKey, TValue> builder,
        InMemoryKafkaCluster cluster)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(cluster);
        return builder
            .WithBootstrapServers(cluster.BootstrapServers)
            .WithConnectionPoolFactory(cluster.CreateConnectionPool);
    }

    /// <summary>
    /// Connects the admin client to an in-memory cluster instead of real brokers.
    /// Sets the bootstrap servers and connection pool; no network I/O occurs.
    /// </summary>
    public static AdminClientBuilder UseInMemoryCluster(
        this AdminClientBuilder builder,
        InMemoryKafkaCluster cluster)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(cluster);
        return builder
            .WithBootstrapServers(cluster.BootstrapServers)
            .WithConnectionPoolFactory(cluster.CreateConnectionPool);
    }
}
