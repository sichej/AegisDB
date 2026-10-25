namespace AegisDB.Server.Distributed;

/// <summary>
/// Supplies UTF-8 text, including JSON text, for a replicated PUT.
/// </summary>
public sealed record PutValueRequest(string? Value);

/// <summary>
/// Confirms quorum commitment and local application of one Raft command.
/// </summary>
public sealed record DistributedWriteResponse(
    string Key,
    string Operation,
    string LeaderId,
    string LogIndex,
    string Term,
    bool Committed,
    bool Applied);

/// <summary>
/// Observes an applied value on the locally known leader without a quorum read barrier.
/// A null value means the key was not found; an empty string is a valid value.
/// </summary>
public sealed record DistributedReadResponse(
    string Key,
    string? Value,
    string LeaderId,
    string Term,
    string LastApplied,
    string Consistency,
    bool Linearizable);
