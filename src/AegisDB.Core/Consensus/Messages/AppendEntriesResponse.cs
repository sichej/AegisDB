
namespace AegisDB.Core.Consensus.Messages;

/// <summary>
/// Represents the result of an AppendEntries RPC.
///
/// Success indicates that the follower accepted the log
/// consistency check and the supplied entries.
/// </summary>
public sealed record AppendEntriesResponse(
    long Term,
    bool Success
);
