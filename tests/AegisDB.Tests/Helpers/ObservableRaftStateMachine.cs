
using AegisDB.Core.Consensus;
using AegisDB.Core.Persistence;

namespace AegisDB.Tests.Helpers;

/// <summary>
/// Test state machine that stores applied key-value commands
/// and exposes the resulting state for assertions.
/// </summary>
public sealed class ObservableRaftStateMachine : IRaftStateMachine
{
    private readonly object _lock = new();

    private readonly Dictionary<string, byte[]> _values =
        new(StringComparer.Ordinal);

    private readonly List<long> _appliedIndexes = [];

    public void Apply(RaftLogEntry entry)
    {
        lock (_lock)
        {
            switch (entry.Command.Operation)
            {
                case WalOperation.Put:
                    if (entry.Command.Value is null)
                    {
                        throw new InvalidOperationException(
                            "PUT requires a value.");
                    }

                    _values[entry.Command.Key] =
                        entry.Command.Value.ToArray();
                    break;

                case WalOperation.Delete:
                    _values.Remove(entry.Command.Key);
                    break;

                default:
                    throw new InvalidOperationException(
                        "Unsupported Raft command.");
            }

            _appliedIndexes.Add(entry.Index);
        }
    }

    public byte[]? Get(string key)
    {
        lock (_lock)
        {
            return _values.TryGetValue(key, out byte[]? value)
                ? value.ToArray()
                : null;
        }
    }

    public long[] GetAppliedIndexes()
    {
        lock (_lock)
        {
            return _appliedIndexes.ToArray();
        }
    }
}
