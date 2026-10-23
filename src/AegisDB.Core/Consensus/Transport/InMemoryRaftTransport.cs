
using System.Collections.Concurrent;
using AegisDB.Core.Consensus.Messages;

namespace AegisDB.Core.Consensus.Transport;

/// <summary>
/// Routes Raft RPCs directly to nodes running in the same process.
///
/// Intended for simulations and integration tests, not network communication.
/// </summary>
public sealed class InMemoryRaftTransport : IRaftTransport
{
    private readonly ConcurrentDictionary<string, RaftNode> _nodes =
        new(StringComparer.Ordinal);

    public void Register(RaftNode node)
    {
        ArgumentNullException.ThrowIfNull(node);

        if (!_nodes.TryAdd(node.NodeId, node))
        {
            throw new InvalidOperationException(
                $"Node '{node.NodeId}' is already registered.");
        }
    }

    public Task<RequestVoteResponse> RequestVoteAsync(
        string targetNodeId,
        RequestVoteRequest request,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        RaftNode node = ResolveNode(targetNodeId);

        return node.HandleRequestVoteAsync(request, cancellationToken);
    }

    public Task<AppendEntriesResponse> AppendEntriesAsync(
        string targetNodeId,
        AppendEntriesRequest request,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        RaftNode node = ResolveNode(targetNodeId);

        return node.HandleAppendEntriesAsync(request, cancellationToken);
    }

    private RaftNode ResolveNode(string nodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

        if (!_nodes.TryGetValue(nodeId, out RaftNode? node))
        {
            throw new InvalidOperationException(
                $"Node '{nodeId}' is not registered.");
        }

        return node;
    }

    /// <summary>
    /// Removes a node from the transport, simulating an unavailable peer.
    /// </summary>
    public bool Unregister(string nodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

        return _nodes.TryRemove(nodeId, out _);
    }
}
