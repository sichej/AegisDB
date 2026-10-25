
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Core.Consensus;

/// <summary>
/// Coordinates log replication from one Raft leader to all followers.
///
/// Each follower maintains independent replication progress.
/// A manager instance belongs to one leadership term.
/// </summary>
public sealed class RaftReplicationManager
{
    private readonly RaftNode _leader;

    private readonly IReadOnlyDictionary<
        string, RaftFollowerProgress> _progress;

    private readonly IReadOnlyDictionary<
        string, RaftLogReplicator> _replicators;

    private readonly long _leaderTerm;

    public RaftReplicationManager(
        RaftNode leader,
        IRaftTransport transport,
        IEnumerable<string> peers)
    {
        _leader = leader ?? throw new ArgumentNullException(nameof(leader));
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(peers);

        var peerIds = peers.ToArray();

        if (peerIds.Any(id => string.IsNullOrWhiteSpace(id)) ||
            peerIds.Distinct(StringComparer.Ordinal).Count() != peerIds.Length ||
            peerIds.Any(id =>
                id == leader.NodeId || !leader.Cluster.Contains(id)) ||
            peerIds.Length != leader.Cluster.ClusterSize - 1)
        {
            throw new ArgumentException(
                "Peers must contain every other cluster member exactly once.",
                nameof(peers));
        }

        // The manager must be created after leader election.
        // Its ownership term is captured when constructed.
        _leaderTerm = leader.CurrentTerm;

        var progress = new Dictionary<string, RaftFollowerProgress>(
            StringComparer.Ordinal);

        var replicators = new Dictionary<string, RaftLogReplicator>(
            StringComparer.Ordinal);

        foreach (string peerId in peerIds)
        {
            var followerProgress = new RaftFollowerProgress(
                peerId,
                leader.LastLogIndex);

            progress.Add(peerId, followerProgress);

            replicators.Add(
                peerId,
                new RaftLogReplicator(
                    leader,
                    transport,
                    followerProgress));
        }

        _progress = progress;
        _replicators = replicators;
    }

    /// <summary>
    /// Performs one replication attempt for each follower.
    ///
    /// One unavailable follower does not prevent attempts to
    /// replicate to other followers.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, bool>> ReplicateAllOnceAsync(
        CancellationToken cancellationToken = default)
    {
        RaftNodeStateSnapshot leaderState =
            await _leader.GetStateSnapshotAsync(cancellationToken);

        if (leaderState.State != RaftNodeState.Leader ||
            leaderState.CurrentTerm != _leaderTerm)
        {
            throw new InvalidOperationException(
                "Replication manager no longer owns the current leadership term.");
        }


        async Task<(string PeerId, bool Success)> ReplicatePeerAsync(
            KeyValuePair<string, RaftLogReplicator> pair)
        {
            try
            {
                bool success = await pair.Value.ReplicateOnceAsync(
                    cancellationToken);

                return (pair.Key, success);
            }
            catch (RaftTransportException)
            {
                // The follower is temporarily unreachable.
                // Other peers may still provide a quorum.
                return (pair.Key, false);
            }
        }


        var results = await Task.WhenAll(
            _replicators.Select(ReplicatePeerAsync));

        return results.ToDictionary(
            item => item.PeerId,
            item => item.Success,
            StringComparer.Ordinal);
    }

    /// <summary>
    /// Returns a snapshot of the replication progress for one follower.
    /// </summary>
    public (long NextIndex, long MatchIndex) GetFollowerProgress(
        string followerId)
    {
        if (!_progress.TryGetValue(followerId, out var progress))
        {
            throw new ArgumentException(
                $"Unknown follower '{followerId}'.",
                nameof(followerId));
        }

        return progress.GetSnapshot();
    }


    /// <summary>
    /// Attempts to commit the highest current-term entry
    /// acknowledged by a majority of cluster members.
    /// </summary>
    public async Task<bool> TryAdvanceCommitAsync(
        CancellationToken cancellationToken = default)
    {
        long[] followerMatches = _progress.Values
            .Select(progress => progress.GetSnapshot().MatchIndex)
            .ToArray();

        return await _leader.TryAdvanceLeaderCommitAsync(
            expectedTerm: _leaderTerm,
            followerMatchIndexes: followerMatches,
            cancellationToken: cancellationToken);
    }


    /// <summary>
    /// Advances the leader commit index after quorum replication
    /// and applies all committed entries to the state machine.
    ///
    /// Application is attempted even when the commit index did
    /// not advance, allowing retry after a previous apply failure.
    /// </summary>
    public async Task<bool> CommitAndApplyAsync(
        CancellationToken cancellationToken = default)
    {
        RaftNodeStateSnapshot snapshot =
            await _leader.GetStateSnapshotAsync(cancellationToken);

        if (snapshot.State != RaftNodeState.Leader ||
            snapshot.CurrentTerm != _leaderTerm)
        {
            return false;
        }

        bool committed = await TryAdvanceCommitAsync(
            cancellationToken);

        // The state machine applies entries in order.
        // LastApplied only advances after a successful application.
        await _leader.ApplyCommittedEntriesAsync(
            cancellationToken);

        return committed;
    }


}
