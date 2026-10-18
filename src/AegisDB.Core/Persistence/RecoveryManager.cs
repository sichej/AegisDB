using AegisDB.Core.Storage;

namespace AegisDB.Core.Persistence;

/// <summary>
/// Reconstructs the in-memory database state by replaying
/// persisted WAL operations in their original order.
/// </summary>
public sealed class RecoveryManager
{
    private readonly WriteAheadLog _wal;
    private readonly KeyValueStore _store;

    public RecoveryManager(WriteAheadLog wal, KeyValueStore store)
    {
        _wal = wal;
        _store = store;
    }

    /// <summary>
    /// Replays persisted mutations to restore the database state.
    /// This method must be called before accepting client requests.
    /// </summary>
    public async Task RecoverAsync(
        CancellationToken cancellationToken = default)
    {
        var records = await _wal.ReadAllAsync(cancellationToken);

        foreach (var record in records)
        {
            switch (record.Operation)
            {
                case WalOperation.Put:
                    if (record.Value is null)
                        throw new InvalidDataException(
                            "PUT record contains no value.");

                    _store.Put(record.Key, record.Value);
                    break;

                case WalOperation.Delete:
                    _store.Delete(record.Key);
                    break;

                default:
                    throw new InvalidDataException(
                        $"Unknown WAL operation: {record.Operation}");
            }
        }
    }
}