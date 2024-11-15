namespace JotDB.Storage;

public sealed class BackgroundWorker<TArg> where TArg : class
{
    private readonly CancellationTokenSource _cts = new();
    private readonly TArg _arg;
    private readonly Func<TArg, CancellationToken, Task> _backgroundTaskDelegate;
    private Task _backgroundTask = Task.CompletedTask;

    public BackgroundWorker(string name,
        Func<TArg, CancellationToken, Task> backgroundTask,
        TArg arg)
    {
        Name = name;
        _backgroundTaskDelegate = backgroundTask;
        _arg = arg;
    }

    public string Name { get; }

    public void Start()
    {
        _backgroundTask = _backgroundTaskDelegate(
            _arg,
            _cts.Token);
    }

    public Task StopAsync()
    {
        _cts.Cancel();
        return _backgroundTask;
    }
}