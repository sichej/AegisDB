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
        // Acquire the write lock to ensure exclusive access for this mutation.
        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            var record = new WalRecord(
                WalOperation.Put,
                key,
                value
            );

            await _wal.AppendAsync(record, cancellationToken);

            _store.Put(key, value);
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
}