using JotDB.Storage;

namespace JotDB;

public sealed class Database : IDisposable
{
    private readonly List<BackgroundWorker> _backgroundWorkers = [];
    private readonly TaskCompletionSource _runningStateTask = new();
    private volatile DatabaseState _state = DatabaseState.Stopped;
    private Dictionary<ulong, ReadOnlyMemory<byte>> _cache = new();

    public Database()
    {
        Journal = JournalFile.Open("journal.txt");
    }

    public JournalFile Journal { get; }
    public DatabaseState State => _state;

    public void AddBackgroundWorker(
        string name,
        Func<Database, CancellationToken, Task> work)
    {
        var worker = new BackgroundWorker(this, name, work);
        _backgroundWorkers.Add(worker);
    }

    public Task InsertDocumentAsync(ReadOnlyMemory<byte> document)
    {
        return Journal
            .WriteAsync(
                document,
                JournalEntryType.Insert);
    }

    public void AddToCache(ulong documentId, ReadOnlyMemory<byte> document)
    {
        _cache.Add(documentId, document);
    }

    public IEnumerable<ulong> GetCachedDocuments()
    {
        return _cache.Keys;
    }

    /// <summary>
    /// Runs the database, starting all registered background workers and waiting for a shutdown signal.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation of running the database.</returns>
    public async Task RunAsync()
    {
        await OnStartingAsync().ConfigureAwait(false);
        await OnRunningAsync().ConfigureAwait(false);
        await OnStoppingAsync().ConfigureAwait(false);
        await OnStoppedAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Attempts to signal the database to shut down by setting the result of the running state task.
    /// </summary>
    /// <returns>A boolean indicating whether the shutdown signal was successfully set.</returns>
    public bool TryShutdown()
    {
        return _runningStateTask.TrySetResult();
    }

    public void Dispose()
    {
        Journal.Dispose();
    }

    public void Checkpoint(ulong operationId)
    {
        
    }

    private Task OnStartingAsync()
    {
        _state = DatabaseState.Starting;

        Console.WriteLine("starting database");

        foreach (var worker in _backgroundWorkers)
        {
            try
            {
                worker.Start();
            }

            catch (Exception ex)
            {
                Console.WriteLine(
                    $"Failed to start background worker '{worker.Name}'. Exception: {ex}");
            }
        }

        return Task.CompletedTask;
    }

    private Task OnRunningAsync()
    {
        _state = DatabaseState.Running;

        Console.WriteLine("running database");

        return _runningStateTask.Task;
    }

    private async Task OnStoppingAsync()
    {
        _state = DatabaseState.Stopping;

        Console.WriteLine("shutting down database.");

        foreach (var worker in _backgroundWorkers)
        {
            try
            {
                await worker.StopAsync().ConfigureAwait(false);
            }

            catch (OperationCanceledException)
            {
                // ignore cancellation exceptions
            }
        }

        Journal.Dispose();
    }

    private Task OnStoppedAsync()
    {
        _state = DatabaseState.Stopped;
        return Task.CompletedTask;
    }
}