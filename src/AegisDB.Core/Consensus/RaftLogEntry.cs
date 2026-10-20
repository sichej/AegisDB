using AegisDB.Core.Persistence;

namespace AegisDB.Core.Consensus;

/// <summary>
/// Represents a single entry in the replicated Raft log.
///
/// Each entry has a logical index and the term of the leader
/// that originally appended it.
///
/// Entries are applied to the database state machine only
/// after they have been committed according to Raft.
/// </summary>
public sealed record RaftLogEntry(
    long Index,
    long Term,
    WalRecord Command
);