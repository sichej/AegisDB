
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
    private readonly TimeSpan _rpcTimeout;

    public RaftHeartbeatService(
        RaftNode node,
        IRaftTransport transport,
        IEnumerable<string> peers,
        TimeSpan heartbeatInterval,
        TimeSpan? rpcTimeout = null)
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

        _rpcTimeout = rpcTimeout ?? TimeSpan.FromMilliseconds(250);

        if (_rpcTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(rpcTimeout));
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
        RaftNodeStateSnapshot snapshot =
            await _node.GetStateSnapshotAsync(cancellationToken);

        if (snapshot.State != RaftNodeState.Leader)
            return;

        var request = new AppendEntriesRequest(
            Term: snapshot.CurrentTerm,
            LeaderId: snapshot.NodeId,
            PrevLogIndex: snapshot.LastLogIndex,
            PrevLogTerm: snapshot.LastLogTerm,
            Entries: [],
            LeaderCommit: snapshot.CommitIndex);


        async Task SendToPeerAsync(string peerId)
        {
            using var rpcCts =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);

            rpcCts.CancelAfter(_rpcTimeout);

            AppendEntriesResponse response;

            try
            {
                response = await _transport.AppendEntriesAsync(
                    peerId,
                    request,
                    rpcCts.Token);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
                when (rpcCts.IsCancellationRequested)
            {
                // The individual RPC timed out.
                return;
            }
            catch (Exception ex)
                when (ex is not OperationCanceledException)
            {
                // Transport failure: the peer is unavailable.
                // Add structured logging when integrating the server.
                return;
            }

            // A persistence failure here must propagate to the runtime.
            await _node.StepDownIfHigherTermAsync(
                response.Term,
                cancellationToken);
        }


        await Task.WhenAll(_peers.Select(SendToPeerAsync));
    }

}
