
using System.Text;
using System.IO.Hashing;
using System.Buffers.Binary;

namespace AegisDB.Core.Persistence;

/// <summary>
/// Persists database snapshots to disk.
///
/// Snapshots contain the complete key-value state and the WAL
/// offset corresponding to that state.
///
/// Files are written to a temporary location before replacing
/// the previous snapshot to avoid exposing partially written files.
/// </summary>
public sealed class SnapshotStore
{
    private readonly string _filePath;
    private readonly SemaphoreSlim _saveLock = new(1, 1);

    public SnapshotStore(string filePath)
    {
        _filePath = filePath;
    }


    /// <summary>
    /// Persists a database snapshot using a temporary file.
    ///
    /// Concurrent saves are serialized to prevent simultaneous
    /// modifications of the snapshot file.
    ///
    /// The temporary file is flushed before replacing the existing
    /// snapshot, reducing the risk of exposing incomplete data.
    /// </summary>
    public async Task SaveAsync(
        Snapshot snapshot,
        CancellationToken cancellationToken = default)
    {
        // Avoid concurrent writes in the same ShanshotStore instances
        await _saveLock.WaitAsync(cancellationToken);

        try
        {
            // Never replace a newer snapshot with an older one.
            // The check and subsequent save are protected by _saveLock.
            Snapshot? existing = await LoadAsync(cancellationToken);

            if (existing is not null && snapshot.WalOffset <= existing.WalOffset)
            {
                return;
            }
            
            string tempPath = _filePath + "." + Guid.NewGuid().ToString("N") + ".tmp";

            try
            {
                await using (var stream = new FileStream(
                    tempPath,
                    FileMode.CreateNew,
                    FileAccess.ReadWrite,
                    FileShare.None,
                    bufferSize: 4096,
                    useAsync: true))
                {
                    using var writer = new BinaryWriter(
                        stream,
                        Encoding.UTF8,
                        leaveOpen: true
                    );

                    // Snapshot format version 2 includes CRC32.
                    writer.Write(2);

                    // WAL offset represented by this snapshot.
                    writer.Write(snapshot.WalOffset);

                    writer.Write(snapshot.Data.Count);

                    foreach (var (key, value) in snapshot.Data)
                    {
                        byte[] keyBytes = Encoding.UTF8.GetBytes(key);

                        writer.Write(keyBytes.Length);
                        writer.Write(value.Length);

                        writer.Write(keyBytes);
                        writer.Write(value);
                    }

                    writer.Flush();

                    // Calculate CRC32 over the complete snapshot payload,
                    // excluding the checksum field itself.
                    stream.Position = 0;

                    byte[] snapshotBytes =
                        new byte[checked((int)stream.Length)];

                    stream.ReadExactly(snapshotBytes);

                    uint checksum = Crc32.HashToUInt32(snapshotBytes);

                    // Append the checksum after the snapshot payload.
                    stream.Position = stream.Length;

                    writer.Write(checksum);
                    writer.Flush();

                    // Request that all snapshot data is flushed to storage.
                    stream.Flush(flushToDisk: true);
                }

                // Replace the previous snapshot only after the
                // temporary file has been completely written.
                File.Move(tempPath, _filePath, overwrite: true);
            }
            finally
            {
                // Remove temporary files left by failed save operations.
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
        }
        finally
        {
            _saveLock.Release();
        }
    }


    /// <summary>
    /// Loads a database snapshot from disk.
    ///
    /// Returns null if no snapshot exists.
    /// Validates the format version, entry sizes and file structure
    /// before accepting the snapshot.
    /// </summary>
    public async Task<Snapshot?> LoadAsync(
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(_filePath))
            return null;

        await using var stream = new FileStream(
            _filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 4096,
            useAsync: true
        );

        using var reader = new BinaryReader(
            stream,
            Encoding.UTF8,
            leaveOpen: true
        );

        const int maxKeyLength = 1024 * 1024;
        const int maxValueLength = 16 * 1024 * 1024;
        const int maxEntries = 1_000_000;

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            int version = reader.ReadInt32();

            if (version != 2)
                throw new InvalidDataException(
                    $"Unsupported snapshot version: {version}.");

            long walOffset = reader.ReadInt64();

            if (walOffset < 0)
                throw new InvalidDataException(
                    "Invalid snapshot WAL offset.");

            int entryCount = reader.ReadInt32();

            if (entryCount < 0 || entryCount > maxEntries)
                throw new InvalidDataException(
                    "Invalid snapshot entry count.");

            var data = new Dictionary<string, byte[]>(entryCount);

            for (int i = 0; i < entryCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int keyLength = reader.ReadInt32();
                int valueLength = reader.ReadInt32();

                // Reject invalid lengths before allocating buffers.
                if (keyLength < 0 || keyLength > maxKeyLength ||
                    valueLength < 0 || valueLength > maxValueLength)
                {
                    throw new InvalidDataException(
                        "Invalid snapshot entry length.");
                }

                long remainingBytes = stream.Length - stream.Position;

                if ((long)keyLength + valueLength > remainingBytes)
                    throw new InvalidDataException(
                        "Incomplete snapshot entry.");

                byte[] keyBytes = reader.ReadBytes(keyLength);
                byte[] valueBytes = reader.ReadBytes(valueLength);

                string key = new UTF8Encoding(
                    false,
                    true
                ).GetString(keyBytes);

                if (!data.TryAdd(key, valueBytes))
                    throw new InvalidDataException(
                        $"Duplicate snapshot key: {key}");
            }

            // A version 2 snapshot must end with a 4-byte CRC32.
            if (stream.Length - stream.Position != sizeof(uint))
            {
                throw new InvalidDataException(
                    "Invalid snapshot checksum field.");
            }

            uint storedChecksum = reader.ReadUInt32();

            // Verify the complete snapshot payload, excluding the final checksum.
            stream.Position = 0;

            byte[] payload = new byte[checked((int)(stream.Length - sizeof(uint)))];

            stream.ReadExactly(payload);

            uint actualChecksum = Crc32.HashToUInt32(payload);

            if (storedChecksum != actualChecksum)
            {
                throw new InvalidDataException(
                    "Snapshot checksum mismatch.");
            }

            return new Snapshot(walOffset, data);
        }
        catch (EndOfStreamException ex)
        {
            throw new InvalidDataException(
                "Snapshot file is truncated.", ex);
        }
        catch (DecoderFallbackException ex)
        {
            throw new InvalidDataException(
                "Snapshot contains invalid UTF-8.", ex);
        }
    }

}
