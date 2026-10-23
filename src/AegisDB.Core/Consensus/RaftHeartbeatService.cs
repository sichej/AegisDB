
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Core.Consensus;

/// <summary>
/// Periodically sends AppendEntries heartbeats while the local
/// node is the Raft leader.
///
/// A higher term in any response causes the leader to step down.
/// </summary>
public sealed class RaftHeartbeatService
{
    private readonly RaftNode _node;
    private readonly IRaftTransport _transport;
    private readonly IReadOnlyList<string> _peers;
    private readonly TimeSpan _heartbeatInterval;

    public RaftHeartbeatService(
        RaftNode node,
        IRaftTransport transport,
        IEnumerable<string> peers,
        TimeSpan heartbeatInterval)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));

        ArgumentNullException.ThrowIfNull(peers);

        if (heartbeatInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(heartbeatInterval));

        _peers = peers.ToArray();

        if (_peers.Distinct(StringComparer.Ordinal).Count() != _peers.Count ||
            _peers.Any(id =>
                string.IsNullOrWhiteSpace(id) ||
                id == node.NodeId ||
                !node.Cluster.Contains(id)) ||
            _peers.Count != node.Cluster.ClusterSize - 1)
        {
            throw new ArgumentException(
                "Peers must contain every other cluster member exactly once.",
                nameof(peers));
        }

        _heartbeatInterval = heartbeatInterval;
    }

    /// <summary>
    /// Sends heartbeats until the node is no longer leader
    /// or the operation is cancelled.
    /// </summary>
    public async Task RunAsync(
        CancellationToken cancellationToken = default)
    {
        using var timer = new PeriodicTimer(_heartbeatInterval);

        while (!cancellationToken.IsCancellationRequested)
        {
            if (_node.State != RaftNodeState.Leader)
                return;

            await SendHeartbeatsAsync(cancellationToken);

            if (!await timer.WaitForNextTickAsync(cancellationToken))
                break;
        }
    }

    private async Task SendHeartbeatsAsync(
        CancellationToken cancellationToken)
    {
        long term = _node.CurrentTerm;

        var request = new AppendEntriesRequest(
            Term: term,
            LeaderId: _node.NodeId,
            PrevLogIndex: _node.LastLogIndex,
            PrevLogTerm: _node.LastLogTerm,
            Entries: [],
            LeaderCommit: _node.CommitIndex);

        var requests = _peers.Select(async peerId =>
        {
            try
            {
                var response = await _transport.AppendEntriesAsync(
                    peerId,
                    request,
                    cancellationToken);

                // Even a rejected heartbeat can reveal a newer term.
                if (response.Term > term)
                {
                    await _node.BecomeFollowerAsync(
                        response.Term,
                        cancellationToken);
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                // An unreachable follower must not prevent
                // communication with the remaining peers.
                // Production code should log this failure.
            }
        });

        await Task.WhenAll(requests);
    }
}
