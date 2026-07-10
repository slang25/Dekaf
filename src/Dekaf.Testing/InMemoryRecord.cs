namespace Dekaf.Testing;

/// <summary>
/// A record stored in an <see cref="InMemoryKafkaCluster"/> partition.
/// Key, value, and header bytes are copied out of producer buffers at append time,
/// so instances remain valid for the lifetime of the cluster.
/// </summary>
public sealed record InMemoryRecord
{
    /// <summary>
    /// The offset of the record within its partition.
    /// </summary>
    public required long Offset { get; init; }

    /// <summary>
    /// The record timestamp.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// The record key, or null for keyless records.
    /// </summary>
    public byte[]? Key { get; init; }

    /// <summary>
    /// The record value, or null for tombstones.
    /// </summary>
    public byte[]? Value { get; init; }

    /// <summary>
    /// The record headers.
    /// </summary>
    public IReadOnlyList<InMemoryRecordHeader> Headers { get; init; } = [];

    internal int ApproximateSize
    {
        get
        {
            var size = 32 + (Key?.Length ?? 0) + (Value?.Length ?? 0);
            foreach (var header in Headers)
                size += header.Key.Length + (header.Value?.Length ?? 0) + 8;
            return size;
        }
    }
}

/// <summary>
/// A header on an <see cref="InMemoryRecord"/>.
/// </summary>
/// <param name="Key">The header key.</param>
/// <param name="Value">The header value, or null.</param>
public sealed record InMemoryRecordHeader(string Key, byte[]? Value);
