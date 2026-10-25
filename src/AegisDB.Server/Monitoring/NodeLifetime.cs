namespace AegisDB.Server.Monitoring;

/// <summary>
/// Closes admission and drains RPCs and client operations for one node instance.
/// </summary>
public sealed class NodeLifetime : IDisposable
{
    private readonly object _sync = new();
    private readonly CancellationTokenSource _stopping = new();
    private readonly TaskCompletionSource _drained =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private bool _accepting = true;
    private int _operations;

    public NodeLifetime() => Token = _stopping.Token;

    public CancellationToken Token { get; }
    public Task Drained => _drained.Task;
    public bool IsAvailable
    {
        get { lock (_sync) return _accepting; }
    }

    public IDisposable? TryAcquire()
    {
        lock (_sync)
        {
            if (!_accepting) return null;
            _operations++;
            return new Lease(this);
        }
    }

    public void StopAccepting()
    {
        lock (_sync)
        {
            _accepting = false;
            if (_operations == 0) _drained.TrySetResult();
        }
    }

    public Task CancelAsync() => _stopping.CancelAsync();
    public void Dispose() => _stopping.Dispose();

    private void Release()
    {
        lock (_sync)
        {
            _operations--;
            if (!_accepting && _operations == 0) _drained.TrySetResult();
        }
    }

    private sealed class Lease(NodeLifetime owner) : IDisposable
    {
        private NodeLifetime? _owner = owner;
        public void Dispose() => Interlocked.Exchange(ref _owner, null)?.Release();
    }
}
