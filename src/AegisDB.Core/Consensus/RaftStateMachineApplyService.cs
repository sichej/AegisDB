
namespace AegisDB.Core.Consensus;

/// <summary>
/// Applies committed Raft log entries to the local state machine.
///
/// Runs independently of the node role, so the same service can
/// be used by leaders, followers, and candidates.
/// </summary>
public sealed class RaftStateMachineApplyService
{
    private readonly RaftNode _node;
    private readonly TimeSpan _pollInterval;

    public RaftStateMachineApplyService(
        RaftNode node,
        TimeSpan? pollInterval = null)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));

        _pollInterval =
            pollInterval ?? TimeSpan.FromMilliseconds(25);

        if (_pollInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pollInterval));
        }
    }

    /// <summary>
    /// Continuously observes committed progress and applies
    /// pending entries until the service is cancelled.
    /// </summary>
    public async Task RunAsync(
        CancellationToken cancellationToken = default)
    {
        using var timer = new PeriodicTimer(_pollInterval);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            RaftNodeStateSnapshot snapshot =
                await _node.GetStateSnapshotAsync(cancellationToken);

            if (snapshot.LastApplied < snapshot.CommitIndex)
            {
                // RaftNode serializes application and only advances
                // LastApplied after each successful Apply call.
                await _node.ApplyCommittedEntriesAsync(
                    cancellationToken);
            }

            if (!await timer.WaitForNextTickAsync(cancellationToken))
                return;
        }
    }
}
