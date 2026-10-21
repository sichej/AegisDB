
namespace AegisDB.Core.Consensus;

/// <summary>
/// Defines the state machine that executes committed Raft commands.
///
/// Implementations must apply commands deterministically so
/// replicas processing the same log produce the same state.
/// </summary>
public interface IRaftStateMachine
{
    void Apply(RaftLogEntry entry);
}
