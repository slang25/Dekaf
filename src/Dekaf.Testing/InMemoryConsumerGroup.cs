using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Testing;

/// <summary>
/// A minimal classic-protocol group coordinator for one consumer group.
/// </summary>
/// <remarks>
/// <para>Rebalances follow the real coordinator's barrier model: any join starts a round,
/// existing members are told to rejoin via <see cref="ErrorCode.RebalanceInProgress"/> on
/// their next heartbeat, and the round completes when every known member has rejoined
/// (stragglers are evicted after <see cref="InMemoryKafkaClusterOptions.RebalanceTimeout"/>).</para>
/// <para>Assignment is computed client-side by the leader, exactly as with a real broker:
/// the coordinator only redistributes the leader's opaque assignment bytes.</para>
/// </remarks>
internal sealed class InMemoryConsumerGroup
{
    // Serialized ConsumerProtocolAssignment v0 with no topics and empty user data,
    // handed to members the leader did not include in its assignments.
    private static readonly byte[] s_emptyAssignment = new byte[10];

    private readonly object _lock = new();
    private readonly string _groupId;
    private readonly InMemoryKafkaClusterOptions _options;
    private readonly Dictionary<string, MemberState> _members = [];
    private readonly Dictionary<(string Topic, int Partition), long> _committedOffsets = [];

    private int _generation;
    private int _memberCounter;
    private long _joinCounter;
    private string? _protocolName;
    private RebalanceRound? _currentRound;
    private RebalanceRound? _syncRound;

    public InMemoryConsumerGroup(string groupId, InMemoryKafkaClusterOptions options)
    {
        _groupId = groupId;
        _options = options;
    }

    public async Task<JoinGroupResponse> JoinAsync(JoinGroupRequest request, CancellationToken cancellationToken)
    {
        string memberId;
        RebalanceRound round;

        lock (_lock)
        {
            memberId = string.IsNullOrEmpty(request.MemberId)
                ? $"{_groupId}-member-{++_memberCounter}"
                : request.MemberId;

            if (!_members.TryGetValue(memberId, out var member))
            {
                member = new MemberState { MemberId = memberId, JoinOrder = ++_joinCounter };
                _members[memberId] = member;
            }

            var protocol = request.Protocols.Count > 0 ? request.Protocols[0] : null;
            member.Metadata = protocol?.Metadata ?? [];
            _protocolName = protocol?.Name ?? _protocolName;

            round = _currentRound ??= StartRound();
            round.Joined.Add(memberId);
            TryCompleteRound();
        }

        var result = await round.JoinBarrier.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        var isLeader = result.LeaderId == memberId;

        return new JoinGroupResponse
        {
            ErrorCode = ErrorCode.None,
            GenerationId = result.Generation,
            ProtocolName = _protocolName,
            Leader = result.LeaderId,
            MemberId = memberId,
            Members = isLeader ? result.Members : []
        };
    }

    public async Task<SyncGroupResponse> SyncAsync(SyncGroupRequest request, CancellationToken cancellationToken)
    {
        RebalanceRound? round;

        lock (_lock)
        {
            if (!_members.ContainsKey(request.MemberId))
                return new SyncGroupResponse { ErrorCode = ErrorCode.UnknownMemberId, Assignment = [] };

            if (request.GenerationId != _generation)
                return new SyncGroupResponse { ErrorCode = ErrorCode.IllegalGeneration, Assignment = [] };

            round = _syncRound;
            if (round is null || round.Generation != _generation)
                return new SyncGroupResponse { ErrorCode = ErrorCode.RebalanceInProgress, Assignment = [] };

            if (request.Assignments.Count > 0)
            {
                var assignments = request.Assignments.ToDictionary(a => a.MemberId, a => a.Assignment);
                round.AssignmentsReady.TrySetResult(assignments);
            }
        }

        Dictionary<string, byte[]> memberAssignments;
        try
        {
            memberAssignments = await round.AssignmentsReady.Task
                .WaitAsync(_options.RebalanceTimeout, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // The leader never delivered assignments (e.g. it was evicted mid-round):
            // tell the member to rejoin.
            return new SyncGroupResponse { ErrorCode = ErrorCode.RebalanceInProgress, Assignment = [] };
        }

        return new SyncGroupResponse
        {
            ErrorCode = ErrorCode.None,
            ProtocolType = "consumer",
            ProtocolName = _protocolName,
            Assignment = memberAssignments.TryGetValue(request.MemberId, out var assignment)
                ? assignment
                : s_emptyAssignment
        };
    }

    public HeartbeatResponse Heartbeat(HeartbeatRequest request)
    {
        lock (_lock)
        {
            if (!_members.ContainsKey(request.MemberId))
                return new HeartbeatResponse { ErrorCode = ErrorCode.UnknownMemberId };

            if (_currentRound is { } round && !round.Joined.Contains(request.MemberId))
                return new HeartbeatResponse { ErrorCode = ErrorCode.RebalanceInProgress };

            if (request.GenerationId != _generation)
                return new HeartbeatResponse { ErrorCode = ErrorCode.IllegalGeneration };

            return new HeartbeatResponse { ErrorCode = ErrorCode.None };
        }
    }

    public LeaveGroupResponse Leave(LeaveGroupRequest request)
    {
        List<LeaveGroupResponseMember> members = [];

        lock (_lock)
        {
            IEnumerable<string> leavingIds = request.Members is { Count: > 0 } leaving
                ? leaving.Select(m => m.MemberId)
                : request.MemberId is { } single ? [single] : [];

            foreach (var memberId in leavingIds)
            {
                var known = _members.Remove(memberId);
                _currentRound?.Joined.Remove(memberId);
                members.Add(new LeaveGroupResponseMember
                {
                    MemberId = memberId,
                    ErrorCode = known ? ErrorCode.None : ErrorCode.UnknownMemberId
                });
            }

            TryCompleteRound();
        }

        return new LeaveGroupResponse { ErrorCode = ErrorCode.None, Members = members };
    }

    public OffsetCommitResponse CommitOffsets(OffsetCommitRequest request)
    {
        var topics = new List<OffsetCommitResponseTopic>(request.Topics.Count);

        lock (_lock)
        {
            foreach (var topic in request.Topics)
            {
                var partitions = new List<OffsetCommitResponsePartition>(topic.Partitions.Count);
                foreach (var partition in topic.Partitions)
                {
                    _committedOffsets[(topic.Name, partition.PartitionIndex)] = partition.CommittedOffset;
                    partitions.Add(new OffsetCommitResponsePartition
                    {
                        PartitionIndex = partition.PartitionIndex,
                        ErrorCode = ErrorCode.None
                    });
                }

                topics.Add(new OffsetCommitResponseTopic { Name = topic.Name, Partitions = partitions });
            }
        }

        return new OffsetCommitResponse { Topics = topics };
    }

    public long? GetCommittedOffset(string topic, int partition)
    {
        lock (_lock)
            return _committedOffsets.TryGetValue((topic, partition), out var offset) ? offset : null;
    }

    public IReadOnlyList<(string Topic, IReadOnlyList<(int Partition, long Offset)> Partitions)> SnapshotCommittedOffsets()
    {
        lock (_lock)
        {
            return _committedOffsets
                .GroupBy(kvp => kvp.Key.Topic, StringComparer.Ordinal)
                .Select(g => (
                    g.Key,
                    (IReadOnlyList<(int, long)>)g.Select(kvp => (kvp.Key.Partition, kvp.Value)).ToList()))
                .ToList();
        }
    }

    private RebalanceRound StartRound()
    {
        var round = new RebalanceRound();
        _ = EvictStragglersAfterTimeoutAsync(round);
        return round;
    }

    // Called under _lock. Completes the active round once every known member has rejoined.
    private void TryCompleteRound()
    {
        if (_currentRound is not { } round)
            return;

        if (_members.Count > 0 && !_members.Keys.All(round.Joined.Contains))
            return;

        _generation++;
        round.Generation = _generation;
        round.TimeoutCts.Cancel();
        _currentRound = null;
        _syncRound = round;

        var ordered = _members.Values.OrderBy(m => m.JoinOrder).ToList();
        var leaderId = ordered.Count > 0 ? ordered[0].MemberId : string.Empty;
        var members = ordered
            .Select(m => new JoinGroupResponseMember { MemberId = m.MemberId, Metadata = m.Metadata })
            .ToList();

        round.JoinBarrier.TrySetResult(new JoinResult(_generation, leaderId, members));
    }

    private async Task EvictStragglersAfterTimeoutAsync(RebalanceRound round)
    {
        try
        {
            await Task.Delay(_options.RebalanceTimeout, round.TimeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return; // round completed normally
        }

        lock (_lock)
        {
            if (_currentRound != round)
                return;

            foreach (var memberId in _members.Keys.Where(id => !round.Joined.Contains(id)).ToList())
                _members.Remove(memberId);

            TryCompleteRound();
        }
    }

    private sealed class MemberState
    {
        public required string MemberId { get; init; }
        public required long JoinOrder { get; init; }
        public byte[] Metadata { get; set; } = [];
    }

    private sealed record JoinResult(int Generation, string LeaderId, IReadOnlyList<JoinGroupResponseMember> Members);

    private sealed class RebalanceRound
    {
        public TaskCompletionSource<JoinResult> JoinBarrier { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<Dictionary<string, byte[]>> AssignmentsReady { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public HashSet<string> Joined { get; } = [];

        public CancellationTokenSource TimeoutCts { get; } = new();

        public int Generation { get; set; }
    }
}
