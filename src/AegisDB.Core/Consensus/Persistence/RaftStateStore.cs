
using System.Text.Json;

namespace AegisDB.Core.Consensus.Persistence;

/// <summary>
/// Persists and restores Raft election metadata.
///
/// Writes use a temporary file followed by an atomic replacement
/// on supported filesystems to avoid exposing partially written state.
/// </summary>
public sealed class RaftStateStore
{
    private readonly string _filePath;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public RaftStateStore(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        _filePath = Path.GetFullPath(filePath);
    }

    public async Task SaveAsync(
        RaftPersistentState state,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(state);

        if (state.CurrentTerm < 0)
            throw new ArgumentOutOfRangeException(nameof(state));

        await _lock.WaitAsync(cancellationToken);

        try
        {
            string directory = Path.GetDirectoryName(_filePath)!;
            Directory.CreateDirectory(directory);

            string tempPath = _filePath + "." +
                Guid.NewGuid().ToString("N") + ".tmp";

            try
            {
                byte[] data = JsonSerializer.SerializeToUtf8Bytes(state);

                await using (var stream = new FileStream(
                    tempPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize: 4096,
                    useAsync: true))
                {
                    await stream.WriteAsync(data, cancellationToken);

                    // Flush the temporary file before replacing the old state.
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

    public async Task<RaftPersistentState> LoadAsync(
        CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            if (!File.Exists(_filePath))
                return new RaftPersistentState(0, null);

            await using var stream = new FileStream(
                _filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 4096,
                useAsync: true);

            var state = await JsonSerializer.DeserializeAsync<RaftPersistentState>(
                stream,
                cancellationToken: cancellationToken);

            if (state is null || state.CurrentTerm < 0 ||
                (state.VotedFor is not null &&
                 string.IsNullOrWhiteSpace(state.VotedFor)))
            {
                throw new InvalidDataException(
                    "Invalid persisted Raft state.");
            }

            return state;
        }
        finally
        {
            _lock.Release();
        }
    }
}
