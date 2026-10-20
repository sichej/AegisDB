
namespace AegisDB.Core.Consensus.Messages;

/// <summary>
/// Represents an AppendEntries RPC sent by a Raft leader.
///
/// The message is used both for heartbeats and log replication.
/// A heartbeat contains an empty Entries collection.
/// </summary>
public sealed record AppendEntriesRequest(
    long Term,
    string LeaderId,
    long PrevLogIndex,
    long PrevLogTerm,
    IReadOnlyList<RaftLogEntry> Entries,
    long LeaderCommit
);
