
namespace AegisDB.Core.Consensus;

/// <summary>
/// Supervises the election and heartbeat lifecycle of one Raft node.
///
/// The runtime switches between election and heartbeat services
/// according to the node's current role.
/// </summary>
public sealed class RaftNodeRuntime
{
    private readonly RaftNode _node;
    private readonly RaftElectionService _electionService;
    private readonly RaftHeartbeatService _heartbeatService;

    private readonly TimeSpan _pollInterval;

    public RaftNodeRuntime(
        RaftNode node,
        RaftElectionService electionService,
        RaftHeartbeatService heartbeatService,
        TimeSpan? pollInterval = null)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
        _electionService = electionService
            ?? throw new ArgumentNullException(nameof(electionService));
        _heartbeatService = heartbeatService
            ?? throw new ArgumentNullException(nameof(heartbeatService));

        _pollInterval = pollInterval ??
            TimeSpan.FromMilliseconds(25);

        if (_pollInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(pollInterval));
        }
    }

    /// <summary>
    /// Runs the node until shutdown is requested.
    /// </summary>

    public async Task RunAsync(
        CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            RaftNodeStateSnapshot snapshot =
                await _node.GetStateSnapshotAsync(cancellationToken);

            if (snapshot.State == RaftNodeState.Leader)
            {
                await RunLeaderModeAsync(cancellationToken);
            }
            else
            {
                await RunElectionModeAsync(cancellationToken);
            }
        }
    }


    private async Task RunElectionModeAsync(
        CancellationToken cancellationToken)
    {
        // The election service returns after leadership is acquired.
        await _electionService.RunAsync(cancellationToken);
    }

    private async Task RunLeaderModeAsync(
        CancellationToken cancellationToken)
    {
        // Heartbeats run while the node remains leader.
        await _heartbeatService.RunAsync(cancellationToken);

        // Avoid a tight loop during rapid role transitions.
        await Task.Delay(_pollInterval, cancellationToken);
    }
}
