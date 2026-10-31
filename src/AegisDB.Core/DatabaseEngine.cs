using AegisDB.Core.Persistence;
using AegisDB.Core.Storage;

namespace AegisDB.Core;

/// <summary>
/// Coordinates database operations between the persistent WAL
/// and the in-memory key-value store.
/// </summary>
public sealed class DatabaseEngine
{
    private readonly KeyValueStore _store;
    private readonly WriteAheadLog _wal;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public DatabaseEngine(KeyValueStore store, WriteAheadLog wal)
    {
        _store = store;
        _wal = wal;
    }

    /// <summary>
    /// Persists a mutation before applying it to the in-memory state.
    /// Concurrent mutations are processed in a consistent order.
    /// </summary>

    public async Task PutAsync(
        string key,
        byte[] value,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);

        // Take ownership of the caller's buffer before the first await.
        byte[] ownedValue = value.ToArray();

        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            var record = new WalRecord(
                WalOperation.Put,
                key,
                ownedValue
            );

            // Persist the owned value before applying it in memory.
            await _wal.AppendAsync(record, cancellationToken);

            _store.Put(key, ownedValue);
        }
        finally
        {
            _writeLock.Release();
        }
    }


    public byte[]? Get(string key)
    {
        return _store.Get(key);
    }

    public async Task<bool> DeleteAsync(
        string key,
        CancellationToken cancellationToken = default)
    {
        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            // Avoid recording a deletion for a missing key.
            if (_store.Get(key) is null)
                return false;

            var record = new WalRecord(
                WalOperation.Delete,
                key,
                null
            );

            await _wal.AppendAsync(record, cancellationToken);

            return _store.Delete(key);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    /// Captures a consistent database snapshot together with
    /// the WAL offset corresponding to the captured state.
    ///
    /// Mutations are blocked during the operation to prevent
    /// inconsistencies between the snapshot data and WAL position.
    /// </summary>
    public async Task<Snapshot> CreateSnapshotAsync(
        CancellationToken cancellationToken = default)
    {
        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            // Prevent PUT and DELETE operations from changing
            // the WAL or in-memory state during snapshot creation.
            var data = _store.CreateSnapshot();

            // The WAL offset corresponds to the state captured above
            // because both operations are protected by the same lock.
            long walOffset = await _wal.GetLengthAsync(cancellationToken);

            return new Snapshot(walOffset, data);
        }
        finally
        {
            _writeLock.Release();
        }
    }
}