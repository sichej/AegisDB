namespace AegisDB.Core.Persistence;

using System.Runtime.CompilerServices;

using AegisDB.Core.Diagnostics;

/// <summary>
/// Manages the append-only log used to persist database mutations.
/// WAL entries will later be replayed to restore the database state.
/// </summary>
public sealed class WriteAheadLog
{
    private readonly string _filePath;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly IFaultInjector _faultInjector;

    /// <summary>
    /// Creates a WAL writer with an optional fault injector.
    /// Fault injection is disabled by default.
    /// </summary>
    public WriteAheadLog(
        string filePath,
        IFaultInjector? faultInjector = null)
    {
        _filePath = filePath;
        _faultInjector = faultInjector ?? new NoOpFaultInjector();
    }

    /// <summary>
    /// Appends a serialized record to the WAL file.
    /// Concurrent writes are serialized to preserve record boundaries.
    /// </summary>
    public async Task AppendAsync(
        WalRecord record,
        CancellationToken cancellationToken = default)
    {
        byte[] data = WalSerializer.Serialize(record);

        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            await using var stream = new FileStream(
                _filePath,
                FileMode.Append, // Append only, do not overwrite existing data
                FileAccess.Write,
                FileShare.Read,
                bufferSize: 4096,
                useAsync: true
            );

            await stream.WriteAsync(data, cancellationToken);

            // Allow tests to pause execution after writing the record,
            // but before requesting that buffered data be flushed to storage.
            await _faultInjector.CheckpointAsync(
                "wal.before_flush",
                cancellationToken
            );

            stream.Flush(flushToDisk: true);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    /// Reads and deserializes all records from the WAL in append order.
    /// Throws InvalidDataException if a record is incomplete or corrupted.
    /// </summary>
    public async Task<List<WalRecord>> ReadAllAsync(
        CancellationToken cancellationToken = default)
    {
        var records = new List<WalRecord>();

        if (!File.Exists(_filePath))
            return records;

        await using var stream = new FileStream(
            _filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 4096,
            useAsync: true
        );

        while (stream.Position < stream.Length)
        {
            // Read the 4-byte length prefix.
            byte[] lengthBuffer = new byte[sizeof(int)];

            await stream.ReadExactlyAsync(lengthBuffer, cancellationToken);

            int recordLength = BitConverter.ToInt32(lengthBuffer);

            // Reject invalid sizes before allocating memory.
            const int minPayloadLength = 13; // operation + lengths + checksum
            const int maxRecordLength = 16 * 1024 * 1024;

            if (recordLength < minPayloadLength ||
                recordLength > maxRecordLength)
            {
                throw new InvalidDataException("Invalid WAL record length.");
            }

            byte[] recordBytes = new byte[sizeof(int) + recordLength];

            lengthBuffer.CopyTo(recordBytes, 0);

            // Read exactly the number of bytes declared in the record.
            await stream.ReadExactlyAsync(
                recordBytes.AsMemory(sizeof(int), recordLength),
                cancellationToken
            );

            // Deserialize also validates the CRC32 checksum.
            records.Add(WalSerializer.Deserialize(recordBytes));
        }

        return records;
    }

    /// <summary>
    /// Reads the WAL and returns all complete, validated records.
    ///
    /// An incomplete final record is reported without being applied.
    /// Invalid record lengths and checksum failures stop recovery.
    ///
    /// This method does not modify the WAL file and must not run
    /// concurrently with append operations.
    /// </summary>
    public async Task<WalReadResult> ReadForRecoveryAsync(
        CancellationToken cancellationToken = default)
    {
        var records = new List<WalRecord>();

        if (!File.Exists(_filePath))
            return new WalReadResult(records, 0, false);

        await using var stream = new FileStream(
            _filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 4096,
            useAsync: true
        );

        long lastValidOffset = 0;

        const int minRecordLength = 13;
        const int maxRecordLength = 16 * 1024 * 1024;

        while (stream.Position < stream.Length)
        {
            long recordStart = stream.Position;
            long remainingBytes = stream.Length - recordStart;

            // A complete length prefix requires four bytes.
            if (remainingBytes < sizeof(int))
            {
                return new WalReadResult(
                    records,
                    lastValidOffset,
                    true
                );
            }

            byte[] lengthBuffer = new byte[sizeof(int)];

            await stream.ReadExactlyAsync(
                lengthBuffer,
                cancellationToken
            );

            int recordLength = BitConverter.ToInt32(lengthBuffer);

            // Invalid metadata is treated as corruption, not as
            // a recoverable incomplete write.
            if (recordLength < minRecordLength ||
                recordLength > maxRecordLength)
            {
                throw new InvalidDataException(
                    $"Invalid WAL record length at offset {recordStart}."
                );
            }

            long totalRecordLength = sizeof(int) + (long)recordLength;

            // The declared record extends beyond the end of the file.
            if (remainingBytes < totalRecordLength)
            {
                return new WalReadResult(
                    records,
                    lastValidOffset,
                    true
                );
            }

            byte[] recordBytes = new byte[(int)totalRecordLength];

            lengthBuffer.CopyTo(recordBytes, 0);

            await stream.ReadExactlyAsync(
                recordBytes.AsMemory(sizeof(int), recordLength),
                cancellationToken
            );

            try
            {
                // Deserialize validates the record structure and CRC32.
                WalRecord record = WalSerializer.Deserialize(recordBytes);

                records.Add(record);
            }
            catch (InvalidDataException ex)
            {
                throw new InvalidDataException(
                    $"Corrupted WAL record at offset {recordStart}.",
                    ex
                );
            }

            // Only advance after the complete record has been validated.
            lastValidOffset = stream.Position;
        }

        return new WalReadResult(
            records,
            lastValidOffset,
            false
        );
    }

    /// <summary>
    /// Truncates the WAL to the last validated record boundary.
    ///
    /// This operation must only run during recovery, before the database
    /// starts accepting client requests.
    /// </summary>
    public async Task RepairAsync(
        long lastValidOffset,
        CancellationToken cancellationToken = default)
    {
        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            await using var stream = new FileStream(
                _filePath,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.None,
                bufferSize: 4096,
                useAsync: true
            );

            if (lastValidOffset < 0 ||
                lastValidOffset > stream.Length)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(lastValidOffset),
                    "Offset must be within WAL file boundaries."
                );
            }

            // Remove all bytes after the last validated record.
            stream.SetLength(lastValidOffset);

            // Request that the truncation is persisted to storage.
            stream.Flush(flushToDisk: true);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    /// Streams WAL records sequentially without loading the entire
    /// log into memory.
    ///
    /// Each record is validated before being returned to the caller.
    /// Incomplete or corrupted records cause recovery to fail.
    ///
    /// This method must not run concurrently with WAL writes.
    /// </summary>
    public async IAsyncEnumerable<WalRecord> StreamRecordsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!File.Exists(_filePath))
            yield break;

        await using var stream = new FileStream(
            _filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 4096,
            useAsync: true
        );

        const int minRecordLength = 13;
        const int maxRecordLength = 16 * 1024 * 1024;

        while (stream.Position < stream.Length)
        {
            long recordStart = stream.Position;

            byte[] lengthBuffer = new byte[sizeof(int)];

            // ReadExactlyAsync throws if the length prefix is incomplete.
            await stream.ReadExactlyAsync(
                lengthBuffer,
                cancellationToken
            );

            int recordLength = BitConverter.ToInt32(lengthBuffer);

            if (recordLength < minRecordLength ||
                recordLength > maxRecordLength)
            {
                throw new InvalidDataException(
                    $"Invalid WAL record length at offset {recordStart}."
                );
            }

            byte[] recordBytes = new byte[sizeof(int) + recordLength];

            lengthBuffer.CopyTo(recordBytes, 0);

            // Read a single complete record into memory.
            await stream.ReadExactlyAsync(
                recordBytes.AsMemory(sizeof(int), recordLength),
                cancellationToken
            );

            // Deserialize validates the record structure and checksum.
            yield return WalSerializer.Deserialize(recordBytes);
        }
    }

    /// <summary>
    /// Returns the current WAL file length in bytes.
    ///
    /// The returned offset can be used to identify the last
    /// persisted operation included in a database snapshot.
    ///
    /// Callers must coordinate this operation with database mutations
    /// to ensure the offset matches the captured state.
    /// </summary>
    public async Task<long> GetLengthAsync(
        CancellationToken cancellationToken = default)
    {
        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            return File.Exists(_filePath)
                ? new FileInfo(_filePath).Length
                : 0;
        }
        finally
        {
            _writeLock.Release();
        }
    }
}