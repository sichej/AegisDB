
using AegisDB.Core.Persistence;
using AegisDB.Core.Storage;

namespace AegisDB.Core.Consensus;

/// <summary>
/// Applies committed Raft commands to the local key-value state machine.
///
/// This component does not decide which entries are committed.
/// It only executes commands supplied by the consensus layer.
/// </summary>
public sealed class RaftStateMachine : IRaftStateMachine
{
    private readonly KeyValueStore _store;

    public RaftStateMachine(KeyValueStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
    }

    /// <summary>
    /// Applies a single Raft entry to the key-value store.
    ///
    /// Commands must be supplied in log order and only after commit.
    /// </summary>
    public void Apply(RaftLogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        switch (entry.Command.Operation)
        {
            case WalOperation.Put:
                if (entry.Command.Value is null)
                {
                    throw new InvalidDataException(
                        "PUT command must contain a value.");
                }

                _store.Put(
                    entry.Command.Key,
                    entry.Command.Value);
                break;

            case WalOperation.Delete:
                _store.Delete(entry.Command.Key);
                break;

            default:
                throw new InvalidDataException(
                    $"Unsupported operation: {entry.Command.Operation}");
        }
    }
}
