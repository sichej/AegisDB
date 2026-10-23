
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
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
                when (ex is not OperationCanceledException)
            {
                // A failed peer does not prevent replication
                // attempts to the remaining peers.
                // Structured logging will be added in the server.
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
}
