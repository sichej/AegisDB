
using AegisDB.Core.Consensus.Messages;

namespace AegisDB.Core.Consensus.Transport;

/// <summary>
/// Defines communication between nodes participating in Raft.
///
/// The transport is responsible for sending RPC requests to peers.
/// RaftNode remains responsible for consensus decisions.
/// </summary>
public interface IRaftTransport
{
    Task<RequestVoteResponse> RequestVoteAsync(
        string targetNodeId,
        RequestVoteRequest request,
        CancellationToken cancellationToken = default);

    Task<AppendEntriesResponse> AppendEntriesAsync(
        string targetNodeId,
        AppendEntriesRequest request,
        CancellationToken cancellationToken = default);
}
