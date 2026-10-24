
namespace AegisDB.Core.Consensus;

/// <summary>
/// Periodically replicates log entries and propagates commit
/// progress while the node remains leader in the owning term.
/// </summary>
public sealed class RaftReplicationService
{
    private readonly RaftNode _node;
    private readonly RaftReplicationManager _manager;
    private readonly TimeSpan _interval;

    public RaftReplicationService(
        RaftNode node,
        RaftReplicationManager manager,
        TimeSpan interval)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
        _manager = manager ?? throw new ArgumentNullException(nameof(manager));

        if (interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(interval));

        _interval = interval;
    }

    /// <summary>
    /// Runs replication rounds until shutdown or leadership loss.
    /// </summary>
    public async Task RunAsync(
        CancellationToken cancellationToken = default)
    {
        RaftNodeStateSnapshot initial =
            await _node.GetStateSnapshotAsync(cancellationToken);

        if (initial.State != RaftNodeState.Leader)
            return;

        long owningTerm = initial.CurrentTerm;

        using var timer = new PeriodicTimer(_interval);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            RaftNodeStateSnapshot current =
                await _node.GetStateSnapshotAsync(cancellationToken);

            if (current.State != RaftNodeState.Leader ||
                current.CurrentTerm != owningTerm)
            {
                return;
            }

            // Replicate missing entries and send updated LeaderCommit.
            await _manager.ReplicateAllOnceAsync(cancellationToken);

            // Advance commitment when replication reaches a quorum,
            // and retry any pending local state-machine application.
            await _manager.CommitAndApplyAsync(cancellationToken);

            if (!await timer.WaitForNextTickAsync(cancellationToken))
                return;
        }
    }
}
