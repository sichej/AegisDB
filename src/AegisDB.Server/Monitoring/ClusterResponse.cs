namespace AegisDB.Server.Monitoring;

/// <summary>
/// Reports the nodes hosted by this process, not a network-wide health assessment.
/// </summary>
public sealed record ClusterResponse(
    bool Enabled,
    string Mode,
    string AvailabilityDescription,
    DateTimeOffset ObservedAt,
    IReadOnlyList<NodeStatusResponse> Nodes);

/// <summary>
/// Reports a locked Raft snapshot and local runtime availability.
/// Terms and indices are decimal strings to preserve Int64 precision in browsers.
/// </summary>
public sealed record NodeStatusResponse(
    string NodeId,
    string Role,
    string CurrentTerm,
    string CommitIndex,
    string LastApplied,
    string LastLogIndex,
    string LastLogTerm,
    string Availability);
