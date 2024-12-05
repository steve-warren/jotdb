using System.Diagnostics;

namespace JotDB.Storage;

public sealed class BackgroundWorker
{
    private readonly CancellationTokenSource _cts = new();
    private readonly Database _database;
    private readonly Func<Database, CancellationToken, Task> _backgroundTaskDelegate;

    public BackgroundWorker(
        Database database,
        string name,
        Func<Database, CancellationToken, Task> backgroundTask)
    {
        _database = database;
        _backgroundTaskDelegate = backgroundTask;
        Name = name;
    }

    public string Name { get; }
    public Task BackgroundTask { get; private set; } = Task.CompletedTask;

    public void Start()
    {
        BackgroundTask = _backgroundTaskDelegate(
            _database,
            _cts.Token);
    }

    public Task StopAsync()
    {
        _cts.Cancel();
        return BackgroundTask;
    }
}