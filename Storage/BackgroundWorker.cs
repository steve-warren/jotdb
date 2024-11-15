using System.Diagnostics;

namespace JotDB.Storage;

public sealed class BackgroundWorker
{
    private readonly CancellationTokenSource _cts = new();
    private readonly Database _database;
    private readonly Func<Database, CancellationToken, Task> _backgroundTaskDelegate;

    private Task _backgroundTask = Task.CompletedTask;

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

    public void Start()
    {
        Debug.WriteLine($"Starting background worker '{Name}'");

        _backgroundTask = _backgroundTaskDelegate(
            _database,
            _cts.Token);
    }

    public Task StopAsync()
    {
        Debug.WriteLine($"Stopping background worker '{Name}'");
        _cts.Cancel();
        return _backgroundTask;
    }
}