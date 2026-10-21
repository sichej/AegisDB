
using AegisDB.Core.Consensus;

namespace AegisDB.Tests.Helpers;

/// <summary>
/// Records applied Raft entries to verify execution order
/// without depending on a concrete storage implementation.
/// </summary>
public sealed class RecordingRaftStateMachine : IRaftStateMachine
{
    private readonly List<long> _appliedIndices = new();

    public IReadOnlyList<long> AppliedIndices => _appliedIndices.AsReadOnly();

    public void Apply(RaftLogEntry entry)
    {
        _appliedIndices.Add(entry.Index);
    }
}
