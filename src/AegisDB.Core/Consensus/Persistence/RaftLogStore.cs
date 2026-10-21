
using System.Text.Json;
using AegisDB.Core.Persistence;

namespace AegisDB.Core.Consensus.Persistence;

/// <summary>
/// Persists the complete Raft log as a replaceable file.
///
/// Writes are serialized and use a temporary file to avoid
/// exposing partially written log contents.
/// </summary>
public sealed class RaftLogStore
{
    private readonly string _filePath;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public RaftLogStore(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        _filePath = Path.GetFullPath(filePath);
    }

    /// <summary>
    /// Saves a complete Raft log image to disk.
    /// </summary>
    public async Task SaveAsync(
        IReadOnlyList<RaftLogEntry> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);

        await _lock.WaitAsync(cancellationToken);

        try
        {
            string directory = Path.GetDirectoryName(_filePath)!;
            Directory.CreateDirectory(directory);

            string tempPath = _filePath + "." +
                Guid.NewGuid().ToString("N") + ".tmp";

            try
            {
                // Materialize the collection before serialization.
                RaftLogEntry[] snapshot = entries.ToArray();

                byte[] data = JsonSerializer.SerializeToUtf8Bytes(snapshot);

                await using (var stream = new FileStream(
                    tempPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize: 4096,
                    useAsync: true))
                {
                    await stream.WriteAsync(data, cancellationToken);

                    // Flush the new log image before replacing the old file.
                    stream.Flush(flushToDisk: true);
                }

                File.Move(tempPath, _filePath, overwrite: true);
            }
            finally
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Loads and validates the persisted Raft log.
    /// A missing file represents a new, empty log.
    /// </summary>
    public async Task<IReadOnlyList<RaftLogEntry>> LoadAsync(
        CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            if (!File.Exists(_filePath))
                return [];

            await using var stream = new FileStream(
                _filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 4096,
                useAsync: true);

            var entries = await JsonSerializer.DeserializeAsync<RaftLogEntry[]>(
                stream,
                cancellationToken: cancellationToken);

            if (entries is null)
            {
                throw new InvalidDataException(
                    "Invalid persisted Raft log.");
            }

            for (int i = 0; i < entries.Length; i++)
            {
                RaftLogEntry? entry = entries[i];

                if (entry is null ||
                    entry.Index != i + 1L ||
                    entry.Term < 0 ||
                    entry.Command is null ||
                    string.IsNullOrEmpty(entry.Command.Key) ||
                    !Enum.IsDefined(entry.Command.Operation) ||
                    (entry.Command.Operation == WalOperation.Put &&
                     entry.Command.Value is null) ||
                    (entry.Command.Operation == WalOperation.Delete &&
                     entry.Command.Value is not null))
                {
                    throw new InvalidDataException(
                        $"Invalid Raft log entry at position {i}.");
                }
            }

            return entries;
        }
        finally
        {
            _lock.Release();
        }
    }
}
