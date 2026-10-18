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
    /// Restores the database state from the WAL.
    ///
    /// Valid records are replayed in their original order.
    /// An incomplete final record is truncated before replay.
    ///
    /// Recovery must complete before the database accepts requests.
    /// </summary>
    public async Task RecoverAsync(
        CancellationToken cancellationToken = default)
    {
        // Read and validate the complete WAL before modifying the store.
        var result = await _wal.ReadForRecoveryAsync(cancellationToken);

        if (result.HasIncompleteTail)
        {
            // Remove the incomplete record so future appends
            // start at a valid record boundary.
            await _wal.RepairAsync(
                result.LastValidOffset,
                cancellationToken
            );
        }

        // Replay validated mutations in their original order.
        foreach (var record in result.Records)
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