using AegisDB.Core.Consensus;
using AegisDB.Core.Consensus.Messages;
using AegisDB.Core.Consensus.Transport;

namespace AegisDB.Server.Monitoring;

/// <summary>
/// Adds instance admission and draining to the existing in-memory transport.
/// </summary>
public sealed class LocalRaftTransport : IRaftTransport
{
    private readonly object _sync = new();
    private readonly InMemoryRaftTransport _inner = new();
    private readonly Dictionary<string, NodeLifetime> _lifetimes = new(StringComparer.Ordinal);

    public IRaftTransport ForNode(NodeLifetime lifetime) => new SourceTransport(this, lifetime);

    public void Register(RaftNode node, NodeLifetime lifetime)
    {
        lock (_sync)
        {
            _inner.Register(node);
            _lifetimes.Add(node.NodeId, lifetime);
        }
    }

    public void Unregister(string nodeId)
    {
        lock (_sync)
        {
            _lifetimes.Remove(nodeId);
            _inner.Unregister(nodeId);
        }
    }

    public Task<RequestVoteResponse> RequestVoteAsync(
        string targetNodeId, RequestVoteRequest request, CancellationToken cancellationToken = default) =>
        SendAsync(targetNodeId, token => _inner.RequestVoteAsync(targetNodeId, request, token), cancellationToken);

    public Task<AppendEntriesResponse> AppendEntriesAsync(
        string targetNodeId, AppendEntriesRequest request, CancellationToken cancellationToken = default) =>
        SendAsync(targetNodeId, token => _inner.AppendEntriesAsync(targetNodeId, request, token), cancellationToken);

    private async Task<T> SendAsync<T>(
        string targetNodeId, Func<CancellationToken, Task<T>> send, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        NodeLifetime lifetime;
        IDisposable lease;
        lock (_sync)
        {
            if (!_lifetimes.TryGetValue(targetNodeId, out lifetime!) ||
                lifetime.TryAcquire() is not { } acquired)
                throw new RaftTransportException($"Node '{targetNodeId}' is unavailable.");
            lease = acquired;
        }

        using (lease)
        using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, lifetime.Token))
        {
            try
            {
                return await send(linked.Token);
            }
            catch (OperationCanceledException) when (
                lifetime.Token.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                // A stopped target is an unavailable peer, not a failure of the caller's runtime.
                throw new RaftTransportException($"Node '{targetNodeId}' stopped during its RPC.");
            }
        }

    }

    private sealed class SourceTransport(LocalRaftTransport inner, NodeLifetime lifetime) : IRaftTransport
    {
        public Task<RequestVoteResponse> RequestVoteAsync(
            string targetNodeId, RequestVoteRequest request, CancellationToken cancellationToken = default) =>
            SendAsync(token => inner.RequestVoteAsync(targetNodeId, request, token), cancellationToken);

        public Task<AppendEntriesResponse> AppendEntriesAsync(
            string targetNodeId, AppendEntriesRequest request, CancellationToken cancellationToken = default) =>
            SendAsync(token => inner.AppendEntriesAsync(targetNodeId, request, token), cancellationToken);

        private async Task<T> SendAsync<T>(Func<CancellationToken, Task<T>> send, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using IDisposable lease = lifetime.TryAcquire()
                ?? throw new RaftTransportException("The sending node is stopping.");
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, lifetime.Token);
            return await send(linked.Token);
        }
    }
}
