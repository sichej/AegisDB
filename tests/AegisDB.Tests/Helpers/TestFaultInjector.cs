using AegisDB.Core.Diagnostics;

namespace AegisDB.Tests.Helpers;

/// <summary>
/// Allows tests to pause execution at a specific fault injection
/// checkpoint and resume it explicitly.
/// </summary>
public sealed class TestFaultInjector : IFaultInjector
{
    private readonly string _targetCheckpoint;

    private readonly TaskCompletionSource _checkpointReached =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly TaskCompletionSource _resume =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TestFaultInjector(string targetCheckpoint)
    {
        _targetCheckpoint = targetCheckpoint;
    }

    /// <summary>
    /// Signals that the target checkpoint has been reached and
    /// blocks execution until the test explicitly resumes it.
    /// </summary>
    public async Task CheckpointAsync(
        string checkpoint,
        CancellationToken cancellationToken = default)
    {
        if (checkpoint != _targetCheckpoint)
            return;

        _checkpointReached.TrySetResult();

        await _resume.Task.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// Waits until execution reaches the configured checkpoint.
    /// </summary>
    public Task WaitForCheckpointAsync(
        CancellationToken cancellationToken = default)
    {
        return _checkpointReached.Task.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// Releases the blocked operation so it can continue.
    /// </summary>
    public void Resume()
    {
        _resume.TrySetResult();
    }
}