namespace JotDB.Threading;

public sealed class AtomicManualResetEventSlim : IDisposable
{
    private readonly ManualResetEventSlim _manualResetEvent;
    private int _isSignaled = 0;

    public AtomicManualResetEventSlim(bool initialState) =>
        _manualResetEvent = new ManualResetEventSlim(initialState);

    ~AtomicManualResetEventSlim()
    {
        Dispose();
    }

    public bool IsSet =>
        Volatile.Read(ref _isSignaled) == 1;

    public void Set()
    {
        if (Interlocked.CompareExchange(ref _isSignaled, 1, 0) == 0)
            _manualResetEvent.Set();
    }

    public bool TryReset()
    {
        if (Interlocked.CompareExchange(ref _isSignaled, 0, 1) != 1)
            return false;

        _manualResetEvent.Reset();
        return true;
    }

    public void Wait(CancellationToken cancellationToken) =>
        _manualResetEvent.Wait(cancellationToken);

    public void Dispose()
    {
        _manualResetEvent.Dispose();
        GC.SuppressFinalize(this);
    }
}