
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
            // A leader does not need to start new elections.
            if (_node.State == RaftNodeState.Leader)
                return;

            long generation = _timer.Generation;

            bool timedOut = await _timer.WaitForTimeoutAsync(
                cancellationToken);

            if (!timedOut)
                continue;

            if (_node.State == RaftNodeState.Leader)
                return;

            await _coordinator.RunElectionAsync(
                cancellationToken,
                expectedTimerGeneration: generation);

            if (!timedOut)
                continue;

            // The node may have become leader while waiting.
            if (_node.State == RaftNodeState.Leader)
                return;

            try
            {
                await _coordinator.RunElectionAsync(cancellationToken);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
        }
    }
}
