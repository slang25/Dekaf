using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Testing;

public sealed partial class InMemoryKafkaCluster
{
    private FindCoordinatorResponse HandleFindCoordinator(FindCoordinatorRequest request)
    {
        // Populate both the v0-3 scalar fields and the v4+ coordinator list so the
        // response is valid regardless of the negotiated version.
        var keys = request.CoordinatorKeys is { Count: > 0 } coordinatorKeys
            ? coordinatorKeys
            : [request.Key];

        return new FindCoordinatorResponse
        {
            ErrorCode = ErrorCode.None,
            NodeId = BrokerId,
            Host = HostName,
            Port = PortNumber,
            Coordinators = keys.Select(key => new Coordinator
            {
                Key = key,
                NodeId = BrokerId,
                Host = HostName,
                Port = PortNumber,
                ErrorCode = ErrorCode.None
            }).ToList()
        };
    }

    private OffsetFetchResponse HandleOffsetFetch(OffsetFetchRequest request)
    {
        // The consumer reads both the v0-7 Topics shape and the v8+ Groups shape,
        // so populate both with identical data.
        if (request.Groups is { Count: > 0 } requestGroups)
        {
            var groups = requestGroups
                .Select(g => new OffsetFetchResponseGroup
                {
                    GroupId = g.GroupId,
                    ErrorCode = ErrorCode.None,
                    Topics = BuildOffsetFetchTopics(g.GroupId, g.Topics ?? request.Topics)
                })
                .ToList();

            return new OffsetFetchResponse
            {
                ErrorCode = ErrorCode.None,
                Topics = groups[0].Topics,
                Groups = groups
            };
        }

        var topics = BuildOffsetFetchTopics(request.GroupId, request.Topics);
        return new OffsetFetchResponse
        {
            ErrorCode = ErrorCode.None,
            Topics = topics,
            Groups =
            [
                new OffsetFetchResponseGroup
                {
                    GroupId = request.GroupId,
                    ErrorCode = ErrorCode.None,
                    Topics = topics
                }
            ]
        };
    }

    private List<OffsetFetchResponseTopic> BuildOffsetFetchTopics(
        string groupId,
        IReadOnlyList<OffsetFetchRequestTopic>? requestTopics)
    {
        var group = GetGroup(groupId);
        List<OffsetFetchResponseTopic> topics = [];

        if (requestTopics is null)
        {
            foreach (var (topicName, partitions) in group.SnapshotCommittedOffsets())
            {
                topics.Add(new OffsetFetchResponseTopic
                {
                    Name = topicName,
                    Partitions = partitions
                        .Select(p => new OffsetFetchResponsePartition
                        {
                            PartitionIndex = p.Partition,
                            CommittedOffset = p.Offset,
                            ErrorCode = ErrorCode.None
                        })
                        .ToList()
                });
            }

            return topics;
        }

        foreach (var requestTopic in requestTopics)
        {
            topics.Add(new OffsetFetchResponseTopic
            {
                Name = requestTopic.Name,
                Partitions = requestTopic.PartitionIndexes
                    .Select(partition => new OffsetFetchResponsePartition
                    {
                        PartitionIndex = partition,
                        CommittedOffset = group.GetCommittedOffset(requestTopic.Name, partition) ?? -1,
                        ErrorCode = ErrorCode.None
                    })
                    .ToList()
            });
        }

        return topics;
    }
}
