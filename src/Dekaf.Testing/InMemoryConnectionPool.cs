using Dekaf.Networking;
using Dekaf.Protocol;

namespace Dekaf.Testing;

/// <summary>
/// A connection pool whose connections dispatch typed requests to an
/// <see cref="InMemoryKafkaCluster"/> instead of a TCP socket.
/// </summary>
internal sealed class InMemoryConnectionPool : IConnectionPool
{
    private readonly InMemoryKafkaConnection _connection;

    public InMemoryConnectionPool(InMemoryKafkaCluster cluster)
    {
        _connection = new InMemoryKafkaConnection(cluster);
    }

    public ValueTask<IKafkaConnection> GetConnectionAsync(int brokerId, CancellationToken cancellationToken = default)
        => ValueTask.FromResult<IKafkaConnection>(_connection);

    public ValueTask<IKafkaConnection> GetConnectionAsync(string host, int port, CancellationToken cancellationToken = default)
        => ValueTask.FromResult<IKafkaConnection>(_connection);

    public void RegisterBroker(int brokerId, string host, int port)
    {
        // Single in-memory broker; nothing to track.
    }

    public ValueTask RemoveConnectionAsync(int brokerId) => ValueTask.CompletedTask;

    public ValueTask CloseAllAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// A connection that routes typed protocol requests to the in-memory cluster.
/// No bytes are serialized and no I/O occurs.
/// </summary>
internal sealed class InMemoryKafkaConnection : IKafkaConnection
{
    private static int s_instanceCounter;

    private readonly InMemoryKafkaCluster _cluster;

    public InMemoryKafkaConnection(InMemoryKafkaCluster cluster)
    {
        _cluster = cluster;
        ConnectionInstanceId = Interlocked.Increment(ref s_instanceCounter);
    }

    public int BrokerId => InMemoryKafkaCluster.BrokerId;

    public string Host => _cluster.HostName;

    public int Port => _cluster.PortNumber;

    public bool IsConnected => true;

    public int ConnectionInstanceId { get; }

    public ValueTask ConnectAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

    public async ValueTask<TResponse> SendAsync<TRequest, TResponse>(
        TRequest request,
        short apiVersion,
        CancellationToken cancellationToken = default)
        where TRequest : IKafkaRequest<TResponse>
        where TResponse : IKafkaResponse
    {
        var response = await _cluster.HandleRequestAsync(request, cancellationToken).ConfigureAwait(false);
        return (TResponse)response;
    }

    public async ValueTask SendFireAndForgetAsync<TRequest, TResponse>(
        TRequest request,
        short apiVersion,
        CancellationToken cancellationToken = default)
        where TRequest : IKafkaRequest<TResponse>
        where TResponse : IKafkaResponse
    {
        // Fire-and-forget (acks=0): handle the request for its side effects, drop the response.
        _ = await _cluster.HandleRequestAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
