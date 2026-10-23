
using AegisDB.Core.Persistence;

namespace AegisDB.Core.Consensus;

/// <summary>
/// Submits client commands through the Raft replication pipeline.
///
/// A successful result means that the command has been committed
/// by a quorum and applied to the local state machine.
/// </summary>
public sealed class RaftCommandSubmissionService
{
    private readonly RaftNode _leader;
    private readonly RaftReplicationManager _replicationManager;
    private readonly TimeSpan _retryInterval;

    // Serialize client submissions so each command has an
    // unambiguous local append and commitment order.
    private readonly SemaphoreSlim _submissionLock = new(1, 1);

    public RaftCommandSubmissionService(
        RaftNode leader,
        RaftReplicationManager replicationManager,
        TimeSpan? retryInterval = null)
    {
        _leader = leader ?? throw new ArgumentNullException(nameof(leader));
        _replicationManager = replicationManager
            ?? throw new ArgumentNullException(nameof(replicationManager));

        _retryInterval =
            retryInterval ?? TimeSpan.FromMilliseconds(50);

        if (_retryInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(retryInterval));
    }

    /// <summary>
    /// Persists a command locally, replicates it, and waits until
    /// it is committed and applied.
    ///
    /// Cancellation stops waiting; it does not roll back the
    /// command already appended to the Raft log.
    /// </summary>
    public async Task<long> SubmitAsync(
        WalRecord command,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);

        await _submissionLock.WaitAsync(cancellationToken);

        try
        {
            RaftNodeStateSnapshot initialState =
                await _leader.GetStateSnapshotAsync(cancellationToken);

            if (initialState.State != RaftNodeState.Leader)
            {
                throw new InvalidOperationException(
                    "Client commands must be submitted to the leader.");
            }

            long submissionTerm = initialState.CurrentTerm;

            // AppendLeaderCommandAsync persists the command before
            // exposing the entry in the in-memory Raft log.
            RaftLogEntry entry =
                await _leader.AppendLeaderCommandAsync(
                    command,
                    cancellationToken);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                RaftNodeStateSnapshot currentState =
                    await _leader.GetStateSnapshotAsync(
                        cancellationToken);

                if (currentState.State != RaftNodeState.Leader ||
                    currentState.CurrentTerm != submissionTerm)
                {
                    throw new InvalidOperationException(
                        "Leadership changed before command confirmation.");
                }

                // Replicate to all followers. Failures of individual
                // peers are handled by the replication manager.
                await _replicationManager.ReplicateAllOnceAsync(
                    cancellationToken);

                // Try to advance the commit index and apply
                // committed entries to the local state machine.
                await _replicationManager.CommitAndApplyAsync(
                    cancellationToken);

                RaftNodeStateSnapshot afterCommit =
                    await _leader.GetStateSnapshotAsync(
                        cancellationToken);

                if (afterCommit.State != RaftNodeState.Leader ||
                    afterCommit.CurrentTerm != submissionTerm)
                {
                    throw new InvalidOperationException(
                        "Leadership changed during command confirmation.");
                }

                if (afterCommit.CommitIndex >= entry.Index &&
                    afterCommit.LastApplied >= entry.Index)
                {
                    return entry.Index;
                }

                await Task.Delay(_retryInterval, cancellationToken);
            }
        }
        finally
        {
            _submissionLock.Release();
        }
    }
}
