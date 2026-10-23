
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Core.Consensus;

/// <summary>
/// Coordinates a Raft election by requesting votes from cluster peers.
///
/// RaftNode owns election state and quorum decisions.
/// The coordinator only orchestrates outbound RPCs.
/// </summary>
public sealed class RaftElectionCoordinator
{
    private readonly RaftNode _node;
    private readonly IRaftTransport _transport;
    private readonly IReadOnlyList<string> _peers;
    private readonly TimeSpan _rpcTimeout = TimeSpan.FromSeconds(2);

    public RaftElectionCoordinator(
        RaftNode node,
        IRaftTransport transport,
        IEnumerable<string> peers)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));

        ArgumentNullException.ThrowIfNull(peers);

        _peers = peers
            .Where(id => id != node.NodeId)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        if (_peers.Any(id => string.IsNullOrWhiteSpace(id) ||
                             !node.Cluster.Contains(id)))
        {
            throw new ArgumentException(
                "Peers must belong to the Raft cluster.",
                nameof(peers));
        }

        if (_peers.Count != node.Cluster.ClusterSize - 1)
        {
            throw new ArgumentException(
                "All other voting members must be provided.",
                nameof(peers));
        }
    }

    /// <summary>
    /// Starts an election and sends RequestVote RPCs to peers.
    /// A failed RPC is treated as an unavailable peer, not a vote.
    /// </summary>

    public async Task<bool> RunElectionAsync(
        CancellationToken cancellationToken = default,
        long? expectedTimerGeneration = null)
    {
        if (expectedTimerGeneration is long generation)
        {
            bool started = await _node.TryStartElectionAsync(
                generation,
                cancellationToken);

            if (!started)
                return false;
        }
        else
        {
            await _node.StartElectionAsync(cancellationToken);
        }

        long electionTerm = _node.CurrentTerm;

        var request = new RequestVoteRequest(
            Term: electionTerm,
            CandidateId: _node.NodeId,
            LastLogIndex: _node.LastLogIndex,
            LastLogTerm: _node.LastLogTerm);

        if (_node.Cluster.QuorumSize == 1)
        {
            await _node.ReceiveSingleNodeMajorityAsync(cancellationToken);
            return _node.State == RaftNodeState.Leader;
        }

        using var electionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // Every RPC receives its own timeout.
        async Task<(string PeerId, RequestVoteResponse? Response)> RequestPeerAsync(
            string peerId)
        {
            using var rpcCts = CancellationTokenSource.CreateLinkedTokenSource(electionCts.Token);

            rpcCts.CancelAfter(_rpcTimeout);

            try
            {
                var response = await _transport.RequestVoteAsync(
                    peerId,
                    request,
                    rpcCts.Token);

                return (peerId, response);
            }
            catch (OperationCanceledException)
                when (!cancellationToken.IsCancellationRequested)
            {
                return (peerId, null);
            }
            catch (Exception)
                when (!cancellationToken.IsCancellationRequested)
            {
                return (peerId, null);
            }
        }

        var pending = _peers
            .Select(RequestPeerAsync)
            .ToList();

        while (pending.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Process whichever RPC finishes first.
            var completed = await Task.WhenAny(pending);
            pending.Remove(completed);

            var (peerId, response) = await completed;

            if (response is null)
                continue;

            // Responses with a higher term must be processed even
            // if the peer rejected our vote request.
            bool elected = await _node.ReceiveVoteAsync(
                peerId,
                response.Term,
                response.VoteGranted,
                electionTerm,
                cancellationToken);

            if (_node.CurrentTerm > electionTerm)
            {
                electionCts.Cancel();
                return false;
            }

            if (elected)
            {
                electionCts.Cancel();
                return true;
            }
        }

        return _node.State == RaftNodeState.Leader &&
               _node.CurrentTerm == electionTerm;
    }

}
