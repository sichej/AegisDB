namespace AegisDB.Core.Consensus;

/// <summary>
/// Represents a consistent, immutable view of a Raft node's
/// consensus state at a specific point in time.
/// </summary>
public sealed record RaftNodeStateSnapshot(
    string NodeId,
    RaftNodeState State,
    long CurrentTerm,
    string? VotedFor,
    long CommitIndex,
    long LastApplied,
    long LastLogIndex,
    long LastLogTerm);