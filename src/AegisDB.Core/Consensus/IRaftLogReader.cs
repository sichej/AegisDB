
namespace AegisDB.Core.Consensus;

/// <summary>
/// Exposes read-only access to a Raft log.
///
/// Mutations must be coordinated by the owning Raft node.
/// </summary>
public interface IRaftLogReader
{
    long LastIndex { get; }

    long LastTerm { get; }

    RaftLogEntry? GetEntry(long index);

    bool Matches(long index, long term);

    IReadOnlyList<RaftLogEntry> CreateSnapshot();
}
