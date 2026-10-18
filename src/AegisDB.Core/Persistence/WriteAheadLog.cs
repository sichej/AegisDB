namespace AegisDB.Core.Persistence;

/// <summary>
/// Manages the append-only log used to persist database mutations.
/// WAL entries will later be replayed to restore the database state.
/// </summary>
public sealed class WriteAheadLog
{
    private readonly string _filePath;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public WriteAheadLog(string filePath)
    {
        _filePath = filePath;
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

            // Request that buffered file data be flushed to storage.
            // Flush every operation is expensive but ensures durability of each write.
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
}