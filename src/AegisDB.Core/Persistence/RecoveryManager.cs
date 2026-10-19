
using AegisDB.Core.Storage;

namespace AegisDB.Core.Persistence;

/// <summary>
/// Restores database state from a validated snapshot and
/// replays subsequent WAL mutations.
///
/// An incomplete WAL tail can be repaired during recovery.
/// </summary>
public sealed class RecoveryManager
{
    private readonly WriteAheadLog _wal;
    private readonly KeyValueStore _store;
    private readonly SnapshotStore? _snapshotStore;

    public RecoveryManager(
        WriteAheadLog wal,
        KeyValueStore store,
        SnapshotStore? snapshotStore = null)
    {
        _wal = wal;
        _store = store;
        _snapshotStore = snapshotStore;
    }

    /// <summary>
    /// Restores the database state before client requests are accepted.
    ///
    /// The complete WAL is validated before applying any mutations.
    /// Snapshot offsets must match a valid WAL record boundary.
    /// </summary>
    public async Task RecoverAsync(
        CancellationToken cancellationToken = default)
    {
        var result = await _wal.ReadForRecoveryAsync(cancellationToken);

        // Load the snapshot only if snapshot storage is configured.
        Snapshot? snapshot = _snapshotStore is null
            ? null
            : await _snapshotStore.LoadAsync(cancellationToken);

        long replayOffset = snapshot?.WalOffset ?? 0;

        if (replayOffset < 0 ||
            replayOffset > result.LastValidOffset)
        {
            throw new InvalidDataException(
                "Snapshot WAL offset is outside the valid WAL range.");
        }

        // Verify that the snapshot offset is a record boundary.
        long currentOffset = 0;
        bool validBoundary = replayOffset == 0;

        foreach (var record in result.Records)
        {
            currentOffset += WalSerializer.Serialize(record).Length;

            if (currentOffset == replayOffset)
                validBoundary = true;
        }

        if (!validBoundary)
        {
            throw new InvalidDataException(
                "Snapshot WAL offset does not match a record boundary.");
        }

        // Repair only after validating the snapshot and WAL.
        if (result.HasIncompleteTail)
        {
            await _wal.RepairAsync(
                result.LastValidOffset,
                cancellationToken
            );
        }

        // Restore the snapshot state, if available.
        if (snapshot is not null)
            _store.RestoreSnapshot(snapshot.Data);

        // Replay only the operations that follow the snapshot.
        currentOffset = 0;

        foreach (var record in result.Records)
        {
            currentOffset += WalSerializer.Serialize(record).Length;

            if (currentOffset <= replayOffset)
                continue;

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
