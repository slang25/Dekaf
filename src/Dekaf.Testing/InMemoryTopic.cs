using Dekaf.Protocol.Records;

namespace Dekaf.Testing;

/// <summary>
/// A topic in the in-memory cluster: a fixed set of partitions.
/// </summary>
internal sealed class InMemoryTopic
{
    public InMemoryTopic(string name, int partitionCount)
    {
        Name = name;
        var partitions = new InMemoryPartition[partitionCount];
        for (var i = 0; i < partitionCount; i++)
            partitions[i] = new InMemoryPartition();
        Partitions = partitions;
    }

    public string Name { get; }

    public IReadOnlyList<InMemoryPartition> Partitions { get; }

    public InMemoryPartition? GetPartition(int index)
        => index >= 0 && index < Partitions.Count ? Partitions[index] : null;
}

/// <summary>
/// A single partition log. Appends copy record data out of producer-owned buffers;
/// reads return stored records that are safe to retain indefinitely.
/// </summary>
internal sealed class InMemoryPartition
{
    private readonly object _lock = new();
    private readonly List<InMemoryRecord> _records = [];
    private TaskCompletionSource _dataSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public long HighWatermark
    {
        get
        {
            lock (_lock)
                return _records.Count;
        }
    }

    /// <summary>
    /// Appends all records in the batch, copying key/value/header bytes out of the
    /// producer's (potentially pooled) memory. Returns the base offset assigned to the batch.
    /// </summary>
    public long Append(RecordBatch batch)
    {
        // Copy outside the lock: the batch memory is only guaranteed valid for the
        // duration of this call, and copying is the slow part.
        var copied = new List<InMemoryRecord>();
        foreach (var record in batch.Records)
        {
            IReadOnlyList<InMemoryRecordHeader> headers = [];
            if (record.Headers is { Count: > 0 } sourceHeaders)
            {
                var copiedHeaders = new List<InMemoryRecordHeader>(sourceHeaders.Count);
                foreach (var header in sourceHeaders)
                    copiedHeaders.Add(new InMemoryRecordHeader(header.Key, header.IsValueNull ? null : header.Value.ToArray()));
                headers = copiedHeaders;
            }

            copied.Add(new InMemoryRecord
            {
                Offset = 0, // assigned under the lock below
                Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(batch.BaseTimestamp + record.TimestampDelta),
                Key = record.IsKeyNull ? null : record.Key.ToArray(),
                Value = record.IsValueNull ? null : record.Value.ToArray(),
                Headers = headers
            });
        }

        lock (_lock)
        {
            var baseOffset = (long)_records.Count;
            for (var i = 0; i < copied.Count; i++)
                _records.Add(copied[i] with { Offset = baseOffset + i });

            var signal = _dataSignal;
            _dataSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            signal.TrySetResult();
            return baseOffset;
        }
    }

    /// <summary>
    /// Reads records starting at <paramref name="fromOffset"/>, stopping once
    /// <paramref name="maxBytes"/> is reached (always returns at least one record when available).
    /// </summary>
    public (IReadOnlyList<InMemoryRecord> Records, long HighWatermark) Read(long fromOffset, int maxBytes)
    {
        lock (_lock)
        {
            var highWatermark = (long)_records.Count;
            var start = Math.Max(0, fromOffset);
            if (start >= highWatermark)
                return ([], highWatermark);

            var slice = new List<InMemoryRecord>();
            long bytes = 0;
            for (var i = (int)start; i < highWatermark; i++)
            {
                var record = _records[i];
                slice.Add(record);
                bytes += record.ApproximateSize;
                if (bytes >= maxBytes)
                    break;
            }

            return (slice, highWatermark);
        }
    }

    /// <summary>
    /// Returns a task that completes when data becomes available at or beyond
    /// <paramref name="fromOffset"/>. Completed immediately if data already exists.
    /// </summary>
    public Task WhenDataAvailableAsync(long fromOffset)
    {
        lock (_lock)
        {
            if (_records.Count > fromOffset)
                return Task.CompletedTask;
            return _dataSignal.Task;
        }
    }

    /// <summary>
    /// Finds the earliest offset whose record timestamp is at or after the given timestamp,
    /// or the high watermark if none match.
    /// </summary>
    public (long Offset, long TimestampMs) FindOffsetForTimestamp(long timestampMs)
    {
        lock (_lock)
        {
            foreach (var record in _records)
            {
                var recordTs = record.Timestamp.ToUnixTimeMilliseconds();
                if (recordTs >= timestampMs)
                    return (record.Offset, recordTs);
            }

            return (_records.Count, -1);
        }
    }

    public IReadOnlyList<InMemoryRecord> Snapshot()
    {
        lock (_lock)
            return [.. _records];
    }
}
