namespace AegisDB.Core.Diagnostics;

/// <summary>
/// Coordinates fault injection checkpoints through files.
///
/// Intended exclusively for integration testing where the
/// database server runs in a separate process.
/// </summary>
public sealed class FileFaultInjector : IFaultInjector
{
    private readonly string _directory;

    public FileFaultInjector(string directory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        _directory = directory;

        Directory.CreateDirectory(_directory);
    }

    /// <summary>
    /// Signals that a checkpoint has been reached and waits
    /// until the test process explicitly releases it.
    /// </summary>
    public async Task CheckpointAsync(
        string checkpoint,
        CancellationToken cancellationToken = default)
    {
        // Only explicitly configured checkpoints are activated.
        string enabledPath = Path.Combine(
            _directory,
            $"{checkpoint}.enabled"
        );

        if (!File.Exists(enabledPath))
            return;

        string reachedPath = Path.Combine(
            _directory,
            $"{checkpoint}.reached"
        );

        string resumePath = Path.Combine(
            _directory,
            $"{checkpoint}.resume"
        );

        // Notify the test process that execution has reached
        // the selected checkpoint.
        await File.WriteAllTextAsync(
            reachedPath,
            "reached",
            cancellationToken
        );

        // Wait until the test allows execution to continue.
        while (!File.Exists(resumePath))
        {
            cancellationToken.ThrowIfCancellationRequested();

            await Task.Delay(20, cancellationToken);
        }
    }
}