namespace AegisDB.Core.Consensus.Persistence;

/// <summary>
/// Represents Raft election metadata that must survive node restarts.
/// </summary>
public sealed record RaftPersistentState(
    long CurrentTerm,
    string? VotedFor
);