
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Core.Consensus;

/// <summary>
/// Replicates the leader's log to a single follower.
///
/// Replication progress belongs to the current leader term.
/// Calls are serialized to prevent overlapping requests from
/// applying stale success or failure updates.
/// </summary>
public sealed class RaftLogReplicator
{
    private readonly RaftNode _leader;
    private readonly IRaftTransport _transport;
    private readonly RaftFollowerProgress _progress;
    private readonly SemaphoreSlim _replicationLock = new(1, 1);

    public RaftLogReplicator(
        RaftNode leader,
        IRaftTransport transport,
        RaftFollowerProgress progress)
    {
        _leader = leader ?? throw new ArgumentNullException(nameof(leader));
        _transport = transport
            ?? throw new ArgumentNullException(nameof(transport));
        _progress = progress
            ?? throw new ArgumentNullException(nameof(progress));

        if (!_leader.Cluster.Contains(_progress.FollowerId) ||
            _progress.FollowerId == _leader.NodeId)
        {
            throw new ArgumentException(
                "Replication target must be another cluster member.",
                nameof(progress));
        }
    }

    /// <summary>
    /// Performs one AppendEntries attempt.
    ///
    /// Returns true if the follower accepted the request.
    /// Returns false if replication must be retried or
    /// the node is no longer the current leader.
    ///
    /// The caller controls retry timing.
    /// </summary>
    public async Task<bool> ReplicateOnceAsync(
        CancellationToken cancellationToken = default)
    {
        await _replicationLock.WaitAsync(cancellationToken);

        try
        {
            RaftNodeStateSnapshot leaderState =
                await _leader.GetStateSnapshotAsync(cancellationToken);

            if (leaderState.State != RaftNodeState.Leader)
                return false;

            // Capture a consistent copy of the leader log.
            IReadOnlyList<RaftLogEntry> entries =
                _leader.Log.CreateSnapshot();

            var progress = _progress.GetSnapshot();

            long nextIndex = progress.NextIndex;
            long lastIndex = entries.Count;

            if (nextIndex < 1 || nextIndex > lastIndex + 1)
            {
                throw new InvalidOperationException(
                    "Follower replication progress is outside the leader log.");
            }

            long prevLogIndex = nextIndex - 1;

            long prevLogTerm = prevLogIndex == 0
                ? 0
                : entries[checked((int)(prevLogIndex - 1))].Term;

            RaftLogEntry[] entriesToSend = entries
                .Where(entry => entry.Index >= nextIndex)
                .ToArray();

            var request = new AppendEntriesRequest(
                Term: leaderState.CurrentTerm,
                LeaderId: leaderState.NodeId,
                PrevLogIndex: prevLogIndex,
                PrevLogTerm: prevLogTerm,
                Entries: entriesToSend,
                LeaderCommit: leaderState.CommitIndex);

            AppendEntriesResponse response =
                await _transport.AppendEntriesAsync(
                    _progress.FollowerId,
                    request,
                    cancellationToken);

            // A higher term always takes precedence over replication.
            if (response.Term > leaderState.CurrentTerm)
            {
                await _leader.StepDownIfHigherTermAsync(
                    response.Term,
                    cancellationToken);

                return false;
            }

            // Ignore replies belonging to an obsolete leadership term.
            RaftNodeStateSnapshot currentState =
                await _leader.GetStateSnapshotAsync(cancellationToken);

            if (currentState.State != RaftNodeState.Leader ||
                currentState.CurrentTerm != leaderState.CurrentTerm ||
                response.Term != leaderState.CurrentTerm)
            {
                return false;
            }

            if (!response.Success)
            {
                _progress.RecordFailure();
                return false;
            }

            long replicatedThroughIndex =
                request.PrevLogIndex + request.Entries.Count;

            _progress.RecordSuccess(replicatedThroughIndex);

            return true;
        }
        finally
        {
            _replicationLock.Release();
        }
    }
}
