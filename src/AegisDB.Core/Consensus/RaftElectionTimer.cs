
namespace AegisDB.Core.Consensus;

/// <summary>
/// Tracks randomized Raft election timeouts.
///
/// A valid leader heartbeat or an eligible granted vote can
/// reset the timer. The owner decides when to start an election.
/// </summary>
public sealed class RaftElectionTimer
{
    private readonly TimeSpan _minimumTimeout;
    private readonly TimeSpan _maximumTimeout;
    private readonly object _lock = new();

    private CancellationTokenSource _resetSignal = new();

    private long _generation;

    public RaftElectionTimer(
        TimeSpan minimumTimeout,
        TimeSpan maximumTimeout)
    {
        if (minimumTimeout <= TimeSpan.Zero ||
            maximumTimeout <= minimumTimeout)
        {
            throw new ArgumentException(
                "Election timeout range is invalid.");
        }

        _minimumTimeout = minimumTimeout;
        _maximumTimeout = maximumTimeout;
    }

    public long Generation
    {
        get
        {
            lock (_lock)
            {
                return _generation;
            }
        }
    }

    /// <summary>
    /// Resets the election deadline by cancelling the current wait.
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            _generation++;

            var previous = _resetSignal;
            _resetSignal = new CancellationTokenSource();

            previous.Cancel();
        }
    }

    /// <summary>
    /// Waits until an election timeout expires without being reset.
    ///
    /// Returns true on timeout and false when reset.
    /// </summary>
    public async Task<bool> WaitForTimeoutAsync(
        CancellationToken cancellationToken = default)
    {
        CancellationToken resetToken;

        lock (_lock)
        {
            resetToken = _resetSignal.Token;
        }

        using var linkedCts =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                resetToken);

        double minMs = _minimumTimeout.TotalMilliseconds;
        double maxMs = _maximumTimeout.TotalMilliseconds;

        TimeSpan timeout = TimeSpan.FromMilliseconds(
            minMs + Random.Shared.NextDouble() * (maxMs - minMs));

        try
        {
            await Task.Delay(timeout, linkedCts.Token);
            return true;
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested &&
                  resetToken.IsCancellationRequested)
        {
            return false;
        }
    }
}
