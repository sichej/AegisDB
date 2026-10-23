
namespace AegisDB.Core.Consensus;

/// <summary>
/// Runs the election lifecycle of a Raft node.
///
/// The service waits for election timeouts and starts elections
/// when the node is not already the leader.
/// </summary>
public sealed class RaftElectionService
{
    private readonly RaftNode _node;
    private readonly RaftElectionTimer _timer;
    private readonly RaftElectionCoordinator _coordinator;

    public RaftElectionService(
        RaftNode node,
        RaftElectionTimer timer,
        RaftElectionCoordinator coordinator)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
        _timer = timer ?? throw new ArgumentNullException(nameof(timer));
        _coordinator = coordinator
            ?? throw new ArgumentNullException(nameof(coordinator));
    }

    /// <summary>
    /// Runs the election loop until cancellation is requested.
    /// </summary>

    public async Task RunAsync(
        CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            RaftNodeStateSnapshot snapshot =
                await _node.GetStateSnapshotAsync(cancellationToken);

            if (snapshot.State == RaftNodeState.Leader)
                return;

            // Capture the timer generation before waiting.
            // Any valid heartbeat or granted vote can invalidate it.
            long generation = _timer.Generation;

            bool timedOut = await _timer.WaitForTimeoutAsync(
                cancellationToken);

            if (!timedOut)
                continue;

            // The coordinator checks the timer generation and starts
            // the election atomically with the node state transition.
            await _coordinator.RunElectionAsync(
                cancellationToken,
                expectedTimerGeneration: generation);

            // Regardless of the election outcome, wait for a new
            // randomized timeout before attempting another election.
        }
    }

}
