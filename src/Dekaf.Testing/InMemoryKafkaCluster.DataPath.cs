using System.Diagnostics;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;
using Dekaf.Protocol.Records;

namespace Dekaf.Testing;

public sealed partial class InMemoryKafkaCluster
{
    private static readonly IReadOnlyList<ApiVersion> s_supportedApis =
    [
        new(ApiVersionsRequest.ApiKey, ApiVersionsRequest.LowestSupportedVersion, ApiVersionsRequest.HighestSupportedVersion),
        new(MetadataRequest.ApiKey, MetadataRequest.LowestSupportedVersion, MetadataRequest.HighestSupportedVersion),
        new(ProduceRequest.ApiKey, ProduceRequest.LowestSupportedVersion, ProduceRequest.HighestSupportedVersion),
        new(FetchRequest.ApiKey, FetchRequest.LowestSupportedVersion, FetchRequest.HighestSupportedVersion),
        new(ListOffsetsRequest.ApiKey, ListOffsetsRequest.LowestSupportedVersion, ListOffsetsRequest.HighestSupportedVersion),
        new(CreateTopicsRequest.ApiKey, CreateTopicsRequest.LowestSupportedVersion, CreateTopicsRequest.HighestSupportedVersion),
        new(DeleteTopicsRequest.ApiKey, DeleteTopicsRequest.LowestSupportedVersion, DeleteTopicsRequest.HighestSupportedVersion),
        new(FindCoordinatorRequest.ApiKey, FindCoordinatorRequest.LowestSupportedVersion, FindCoordinatorRequest.HighestSupportedVersion),
        new(JoinGroupRequest.ApiKey, JoinGroupRequest.LowestSupportedVersion, JoinGroupRequest.HighestSupportedVersion),
        new(SyncGroupRequest.ApiKey, SyncGroupRequest.LowestSupportedVersion, SyncGroupRequest.HighestSupportedVersion),
        new(HeartbeatRequest.ApiKey, HeartbeatRequest.LowestSupportedVersion, HeartbeatRequest.HighestSupportedVersion),
        new(LeaveGroupRequest.ApiKey, LeaveGroupRequest.LowestSupportedVersion, LeaveGroupRequest.HighestSupportedVersion),
        new(OffsetCommitRequest.ApiKey, OffsetCommitRequest.LowestSupportedVersion, OffsetCommitRequest.HighestSupportedVersion),
        new(OffsetFetchRequest.ApiKey, OffsetFetchRequest.LowestSupportedVersion, OffsetFetchRequest.HighestSupportedVersion)
    ];

    private static ApiVersionsResponse HandleApiVersions() => new()
    {
        ErrorCode = ErrorCode.None,
        ApiKeys = s_supportedApis
    };

    private MetadataResponse HandleMetadata(MetadataRequest request)
    {
        List<TopicMetadata> topics = [];

        if (request.Topics is null)
        {
            foreach (var topic in SnapshotTopics())
                topics.Add(BuildTopicMetadata(topic));
        }
        else
        {
            foreach (var requested in request.Topics)
            {
                if (requested.Name is not { } name)
                    continue;

                var topic = GetTopic(name);
                if (topic is null && request.AllowAutoTopicCreation && Options.AutoCreateTopics)
                    topic = GetOrCreateTopic(name);

                topics.Add(topic is not null
                    ? BuildTopicMetadata(topic)
                    : new TopicMetadata
                    {
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        Name = name,
                        Partitions = []
                    });
            }
        }

        return new MetadataResponse
        {
            Brokers = [new BrokerMetadata { NodeId = BrokerId, Host = HostName, Port = PortNumber }],
            ClusterId = "in-memory-cluster",
            ControllerId = BrokerId,
            Topics = topics
        };
    }

    private static TopicMetadata BuildTopicMetadata(InMemoryTopic topic)
    {
        var partitions = new List<PartitionMetadata>(topic.Partitions.Count);
        for (var i = 0; i < topic.Partitions.Count; i++)
        {
            partitions.Add(new PartitionMetadata
            {
                ErrorCode = ErrorCode.None,
                PartitionIndex = i,
                LeaderId = BrokerId,
                LeaderEpoch = 0,
                ReplicaNodes = [BrokerId],
                IsrNodes = [BrokerId]
            });
        }

        return new TopicMetadata
        {
            ErrorCode = ErrorCode.None,
            Name = topic.Name,
            Partitions = partitions
        };
    }

    private ProduceResponse HandleProduce(ProduceRequest request)
    {
        var topicResponses = new List<ProduceResponseTopicData>(request.TopicData.Count);

        foreach (var topicData in request.TopicData)
        {
            var topic = GetTopic(topicData.Name);
            if (topic is null && Options.AutoCreateTopics)
                topic = GetOrCreateTopic(topicData.Name);

            var partitionResponses = new List<ProduceResponsePartitionData>(topicData.PartitionData.Count);
            foreach (var partitionData in topicData.PartitionData)
            {
                var partition = topic?.GetPartition(partitionData.Index);
                if (partition is null)
                {
                    partitionResponses.Add(new ProduceResponsePartitionData
                    {
                        Index = partitionData.Index,
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        BaseOffset = -1
                    });
                    continue;
                }

                long baseOffset = -1;
                foreach (var batch in partitionData.Records)
                {
                    var batchBaseOffset = partition.Append(batch);
                    if (baseOffset < 0)
                        baseOffset = batchBaseOffset;
                }

                partitionResponses.Add(new ProduceResponsePartitionData
                {
                    Index = partitionData.Index,
                    ErrorCode = ErrorCode.None,
                    BaseOffset = baseOffset < 0 ? partition.HighWatermark : baseOffset,
                    LogStartOffset = 0
                });
            }

            topicResponses.Add(new ProduceResponseTopicData
            {
                Name = topicData.Name,
                PartitionResponses = partitionResponses
            });
        }

        return new ProduceResponse { Responses = topicResponses };
    }

    private async ValueTask<FetchResponse> HandleFetchAsync(FetchRequest request, CancellationToken cancellationToken)
    {
        var maxWait = TimeSpan.FromMilliseconds(Math.Max(0, request.MaxWaitMs));
        var start = Stopwatch.GetTimestamp();

        while (true)
        {
            var (response, hasData, signals) = BuildFetchResponse(request);
            if (hasData)
                return response;

            var remaining = maxWait - Stopwatch.GetElapsedTime(start);
            if (remaining <= TimeSpan.Zero || signals.Count == 0)
                return response;

            // Long-poll: wake as soon as any requested partition receives data,
            // or when the client's MaxWaitMs budget is spent.
            var delay = Task.Delay(remaining, cancellationToken);
            signals.Add(delay);
            var completed = await Task.WhenAny(signals).ConfigureAwait(false);
            if (completed == delay)
            {
                await delay.ConfigureAwait(false); // propagate cancellation
                return BuildFetchResponse(request).Response;
            }
        }
    }

    private (FetchResponse Response, bool HasData, List<Task> Signals) BuildFetchResponse(FetchRequest request)
    {
        var hasData = false;
        List<Task> signals = [];
        var topicResponses = new List<FetchResponseTopic>(request.Topics.Count);

        foreach (var requestTopic in request.Topics)
        {
            var topic = requestTopic.Topic is { } name ? GetTopic(name) : null;
            var partitionResponses = new List<FetchResponsePartition>(requestTopic.Partitions.Count);

            foreach (var requestPartition in requestTopic.Partitions)
            {
                var partition = topic?.GetPartition(requestPartition.Partition);
                if (partition is null)
                {
                    partitionResponses.Add(new FetchResponsePartition
                    {
                        PartitionIndex = requestPartition.Partition,
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        HighWatermark = -1
                    });
                    continue;
                }

                var fetchOffset = Math.Max(0, requestPartition.FetchOffset);
                var (records, highWatermark) = partition.Read(fetchOffset, Math.Max(1, requestPartition.PartitionMaxBytes));

                if (records.Count > 0)
                    hasData = true;
                else
                    signals.Add(partition.WhenDataAvailableAsync(fetchOffset));

                partitionResponses.Add(new FetchResponsePartition
                {
                    PartitionIndex = requestPartition.Partition,
                    ErrorCode = ErrorCode.None,
                    HighWatermark = highWatermark,
                    LastStableOffset = highWatermark,
                    LogStartOffset = 0,
                    Records = records.Count > 0 ? [BuildRecordBatch(records)] : null
                });
            }

            topicResponses.Add(new FetchResponseTopic
            {
                Topic = requestTopic.Topic,
                Partitions = partitionResponses
            });
        }

        var response = new FetchResponse
        {
            ErrorCode = ErrorCode.None,
            SessionId = 0,
            Responses = topicResponses
        };

        return (response, hasData, signals);
    }

    private static RecordBatch BuildRecordBatch(IReadOnlyList<InMemoryRecord> records)
    {
        var baseOffset = records[0].Offset;
        var baseTimestamp = records[0].Timestamp.ToUnixTimeMilliseconds();
        var maxTimestamp = baseTimestamp;
        var batchSize = 61; // record batch header overhead

        var protocolRecords = new List<Record>(records.Count);
        foreach (var record in records)
        {
            var timestamp = record.Timestamp.ToUnixTimeMilliseconds();
            maxTimestamp = Math.Max(maxTimestamp, timestamp);
            batchSize += record.ApproximateSize;

            List<RecordHeader>? headers = null;
            if (record.Headers.Count > 0)
            {
                headers = new List<RecordHeader>(record.Headers.Count);
                foreach (var header in record.Headers)
                {
                    headers.Add(new RecordHeader
                    {
                        Key = header.Key,
                        Value = header.Value ?? ReadOnlyMemory<byte>.Empty,
                        IsValueNull = header.Value is null
                    });
                }
            }

            protocolRecords.Add(new Record
            {
                Length = record.ApproximateSize,
                TimestampDelta = (int)(timestamp - baseTimestamp),
                OffsetDelta = (int)(record.Offset - baseOffset),
                Key = record.Key ?? ReadOnlyMemory<byte>.Empty,
                Value = record.Value ?? ReadOnlyMemory<byte>.Empty,
                Headers = headers,
                IsKeyNull = record.Key is null,
                IsValueNull = record.Value is null
            });
        }

        return new RecordBatch
        {
            BaseOffset = baseOffset,
            BatchLength = batchSize,
            Attributes = RecordBatchAttributes.None,
            LastOffsetDelta = (int)(records[^1].Offset - baseOffset),
            BaseTimestamp = baseTimestamp,
            MaxTimestamp = maxTimestamp,
            Records = protocolRecords
        };
    }

    private ListOffsetsResponse HandleListOffsets(ListOffsetsRequest request)
    {
        var topicResponses = new List<ListOffsetsResponseTopic>(request.Topics.Count);

        foreach (var requestTopic in request.Topics)
        {
            var topic = GetTopic(requestTopic.Name);
            var partitionResponses = new List<ListOffsetsResponsePartition>(requestTopic.Partitions.Count);

            foreach (var requestPartition in requestTopic.Partitions)
            {
                var partition = topic?.GetPartition(requestPartition.PartitionIndex);
                if (partition is null)
                {
                    partitionResponses.Add(new ListOffsetsResponsePartition
                    {
                        PartitionIndex = requestPartition.PartitionIndex,
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        Offset = -1,
                        Timestamp = -1
                    });
                    continue;
                }

                var (offset, timestamp) = requestPartition.Timestamp switch
                {
                    -2 => (0L, -1L), // earliest
                    -1 => (partition.HighWatermark, -1L), // latest
                    var ts => partition.FindOffsetForTimestamp(ts)
                };

                partitionResponses.Add(new ListOffsetsResponsePartition
                {
                    PartitionIndex = requestPartition.PartitionIndex,
                    ErrorCode = ErrorCode.None,
                    Offset = offset,
                    Timestamp = timestamp
                });
            }

            topicResponses.Add(new ListOffsetsResponseTopic
            {
                Name = requestTopic.Name,
                Partitions = partitionResponses
            });
        }

        return new ListOffsetsResponse { Topics = topicResponses };
    }

    private CreateTopicsResponse HandleCreateTopics(CreateTopicsRequest request)
    {
        var topicResponses = new List<CreateTopicsResponseTopic>(request.Topics.Count);

        foreach (var topic in request.Topics)
        {
            var partitionCount = topic.NumPartitions > 0 ? topic.NumPartitions : Options.DefaultPartitionCount;
            ErrorCode errorCode;

            if (topic.NumPartitions is <= 0 and not -1)
            {
                errorCode = ErrorCode.InvalidPartitions;
            }
            else if (GetTopic(topic.Name) is not null)
            {
                errorCode = ErrorCode.TopicAlreadyExists;
            }
            else
            {
                if (!request.ValidateOnly)
                    _topics.TryAdd(topic.Name, new InMemoryTopic(topic.Name, partitionCount));
                errorCode = ErrorCode.None;
            }

            topicResponses.Add(new CreateTopicsResponseTopic
            {
                Name = topic.Name,
                ErrorCode = errorCode,
                NumPartitions = errorCode == ErrorCode.None ? partitionCount : -1
            });
        }

        return new CreateTopicsResponse { Topics = topicResponses };
    }

    private DeleteTopicsResponse HandleDeleteTopics(DeleteTopicsRequest request)
    {
        List<string> names = [];
        if (request.TopicNames is { } topicNames)
            names.AddRange(topicNames);
        if (request.Topics is { } topicStates)
            names.AddRange(topicStates.Select(t => t.Name).OfType<string>());

        var responses = new List<DeleteTopicsResponseTopic>(names.Count);
        foreach (var name in names)
        {
            responses.Add(new DeleteTopicsResponseTopic
            {
                Name = name,
                ErrorCode = DeleteTopic(name) ? ErrorCode.None : ErrorCode.UnknownTopicOrPartition
            });
        }

        return new DeleteTopicsResponse { Responses = responses };
    }
}
