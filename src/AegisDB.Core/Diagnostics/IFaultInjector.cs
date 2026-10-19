namespace AegisDB.Core.Diagnostics;

/// <summary>
/// Defines optional fault injection points used to test
/// database behavior under controlled failure conditions.
/// </summary>
public interface IFaultInjector
{
    /// <summary>
    /// Executes a configured fault at the specified checkpoint.
    /// Production implementations should perform no action.
    /// </summary>
    Task CheckpointAsync(
        string checkpoint,
        CancellationToken cancellationToken = default);
}