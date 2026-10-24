
using AegisDB.Core.Consensus.Transport;

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
    private Task? _replicationTask;
    private CancellationTokenSource? _replicationCancellation;
    private long? _replicationTerm;
    private readonly IRaftTransport _transport;
    private readonly IReadOnlyList<string> _peerIds;


    public RaftNodeRuntime(
        RaftNode node,
        RaftElectionService electionService,
        RaftHeartbeatService heartbeatService,
        IRaftTransport transport,
        IEnumerable<string> peerIds,
        TimeSpan? pollInterval = null)
    {
        _node = node 
            ?? throw new ArgumentNullException(nameof(node));

        _electionService = electionService
            ?? throw new ArgumentNullException(nameof(electionService));

        _heartbeatService = heartbeatService
            ?? throw new ArgumentNullException(nameof(heartbeatService));

        _transport = transport
            ?? throw new ArgumentNullException(nameof(transport));

        _peerIds = peerIds?.ToArray()
            ?? throw new ArgumentNullException(nameof(peerIds));

        _pollInterval = pollInterval 
            ?? TimeSpan.FromMilliseconds(25);

        if (_pollInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(pollInterval));
        }
    }

    /// <summary>
    /// Runs the Raft node until shutdown is requested.
    ///
    /// Followers and candidates run the election workflow.
    /// Leaders run the replication workflow for their current term.
    ///
    /// Replication is stopped when leadership changes or the
    /// runtime shuts down.
    /// </summary>
    public async Task RunAsync(
        CancellationToken cancellationToken = default)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                RaftNodeStateSnapshot snapshot =
                    await _node.GetStateSnapshotAsync(cancellationToken);

                if (snapshot.State == RaftNodeState.Leader)
                {
                    // Stop replication from a previous leader term.
                    if (_replicationTerm.HasValue &&
                        _replicationTerm.Value != snapshot.CurrentTerm)
                    {
                        await StopLeaderReplicationAsync();
                    }

                    // Start replication only once per leader term.
                    if (_replicationTask is null)
                    {
                        RaftReplicationManager manager =
                            CreateReplicationManager();

                        StartLeaderReplication(
                            manager,
                            snapshot.CurrentTerm,
                            cancellationToken);
                    }

                    // Observe service failures instead of silently
                    // restarting a faulted replication task.
                    if (_replicationTask is { IsCompleted: true })
                    {
                        await _replicationTask;

                        // Replication should not terminate normally
                        // while the node remains leader.
                        throw new InvalidOperationException(
                            "Raft replication stopped while the node is leader.");
                    }

                    // Preserve the runtime polling interval (25 ms).
                    await Task.Delay(_pollInterval, cancellationToken);
                }
                else
                {
                    // A follower or candidate must not keep an
                    // old leader replication service running.
                    await StopLeaderReplicationAsync();

                    // Preserve the existing election workflow.
                    await RunElectionModeAsync(cancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Expected during graceful shutdown.
        }
        finally
        {
            await StopLeaderReplicationAsync();
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


    /// <summary>
    /// Starts one replication service per leader term.
    /// A new term always receives a new replication manager.
    /// </summary>
    private void StartLeaderReplication(
        RaftReplicationManager manager,
        long term,
        CancellationToken runtimeToken)
    {
        if (_replicationTask is not null)
        {
            throw new InvalidOperationException(
                "A previous replication service is still registered.");
        }

        _replicationCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(runtimeToken);

        _replicationTerm = term;

        var service = new RaftReplicationService(
            _node,
            manager,
            TimeSpan.FromMilliseconds(100));

        _replicationTask =
            service.RunAsync(_replicationCancellation.Token);
    }


    /// <summary>
    /// Stops the current replication service and observes its result.
    /// </summary>
    private async Task StopLeaderReplicationAsync()
    {
        Task? task = _replicationTask;
        CancellationTokenSource? cancellation = _replicationCancellation;

        if (task is null)
            return;

        try
        {
            cancellation?.Cancel();

            try
            {
                await task;
            }
            catch (OperationCanceledException)
                when (cancellation?.IsCancellationRequested == true)
            {
                // Expected during controlled shutdown.
            }
        }
        finally
        {
            _replicationTask = null;
            _replicationCancellation = null;
            _replicationTerm = null;

            cancellation?.Dispose();
        }
    }

    /// <summary>
    /// Creates a replication manager for the current leader term.
    ///
    /// Each new leadership term requires fresh follower progress
    /// tracking (NextIndex and MatchIndex).
    /// </summary>
    private RaftReplicationManager CreateReplicationManager()
    {
        return new RaftReplicationManager(
            _node,
            _transport,
            _peerIds);
    }


}
