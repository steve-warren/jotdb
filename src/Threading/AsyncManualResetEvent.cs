namespace JotDB.Threading;

public sealed class AsyncManualResetEvent : IDisposable
{
    private readonly CancellationToken _token;
    private readonly CancellationTokenSource _cts;
    private readonly CancellationTokenRegistration _ctr;
    private readonly TaskCompletionSource _tcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public AsyncManualResetEvent(CancellationToken cancellationToken = default)
    {
        _ctr = cancellationToken.Register(() => _tcs.TrySetCanceled());
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _token = _cts.Token;
    }

    public void Set()
    {
        _tcs.TrySetResult();
    }

    public void SetException(Exception exception)
    {
        _tcs.TrySetException(exception);
    }

    public bool IsSet => _tcs.Task.IsCompleted;

    public Task WaitAsync()
    {
        _token.ThrowIfCancellationRequested();
        return _tcs.Task;
    }

    public void Dispose()
    {
        _cts.Cancel();
        _ctr.Dispose();
        _cts.Dispose();
    }
}