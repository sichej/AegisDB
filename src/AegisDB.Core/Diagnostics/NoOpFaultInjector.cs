namespace AegisDB.Core.Diagnostics;

/// <summary>
/// Default fault injector that performs no action.
///
/// Used during normal database execution to ensure that
/// fault injection checkpoints do not affect operations.
/// </summary>
public sealed class NoOpFaultInjector : IFaultInjector
{
    public Task CheckpointAsync(
        string checkpoint,
        CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}