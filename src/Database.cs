using System.Runtime.CompilerServices;
using JotDB.Storage;

namespace JotDB;

public sealed class Database : IDisposable
{
    private readonly List<BackgroundWorker> _backgroundWorkers = [];
    private readonly TaskCompletionSource _runningStateTask = new();
    private volatile DatabaseState _state = DatabaseState.Stopped;
    private readonly TransactionQueue _transactions = new();
    private readonly CancellationTokenSource _shutdownTokenSource = new();
    private Task _flushTransactionsToDataPagesTask = Task.CompletedTask;

    public Database()
    {
        File.Delete("journal.txt");
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
        var transaction = new StorageTransaction
        {
            Data = document,
            Type = TransactionType.Insert
        };

        return Task.WhenAll(
            _transactions.EnqueueAsync(transaction).AsTask(),
            transaction.WaitAsync(CancellationToken.None));
    }
    
    int _pageCount = 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private DataPage AllocatePage()
    {
        _pageCount++;
        
        Console.WriteLine("allocating new page " + _pageCount);
        var page = new DataPage(
            pageNumber: (ulong)_pageCount,
            timestamp: (ulong)DateTime.Now.Ticks);

        page.ZeroUnusedBytes();

        return page;
    }

    private async Task FlushTransactionsToDataPagesAsync()
    {
        var token = _shutdownTokenSource.Token;
        var pendingCommits = new Queue<StorageTransaction>();

        while (await _transactions.WaitAsync(token))
        {
            List<DataPage> pages = [AllocatePage()];
            
            // transactions are available for write
            var transactions = _transactions
                .GetConsumingEnumerable();

            try
            {
                foreach (var transaction in transactions)
                {
                    if (!pages.Last().TryWrite(transaction.Data.Span))
                    {
                        // page is full, allocate a new one
                        pages.Add(AllocatePage());

                        if (!pages.Last().TryWrite(transaction.Data.Span))
                            throw new InvalidOperationException("failed to write to data page.");
                    }

                    pendingCommits.Enqueue(transaction);
                }

                Console.WriteLine($"flushed {pendingCommits.Count} transactions across {pages.Count} data pages.");

                Journal.WriteToDisk(pages);
            }

            finally
            {
                // ensure that all transactions are committed
                // even if an exception is thrown to avoid deadlocks
                while (pendingCommits.Count > 0)
                    pendingCommits.Dequeue().Commit();
            }

            foreach (var page in pages)
            {
                Console.WriteLine($"freeing page {page.PageNumber}.");
                page.Dispose();
            }
        }
    }

    /// <summary>
    /// Runs the database, starting all registered background workers and waiting for a shutdown signal.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation of running the database.</returns>
    public async Task RunAsync()
    {
        await OnStartingAsync();
        await OnRunningAsync();
        await OnStoppingAsync();
        await OnStoppedAsync();
    }

    /// <summary>
    /// Attempts to signal the database to shut down by setting the result of the running state task.
    /// </summary>
    /// <returns>A boolean indicating whether the shutdown signal was successfully set.</returns>
    public bool TryShutdown()
    {
        _shutdownTokenSource.Cancel();
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
                Console.WriteLine($"starting background worker '{worker.Name}'");
                worker.Start();
                Console.WriteLine($"started background worker '{worker.Name}'");
            }

            catch (Exception ex)
            {
                Console.WriteLine(
                    $"Failed to start background worker '{worker.Name}'. Exception: {ex}");
            }
        }

        _flushTransactionsToDataPagesTask = FlushTransactionsToDataPagesAsync();

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
                Console.WriteLine($"stopping background worker '{worker.Name}'");
                await worker.StopAsync();
                Console.WriteLine($"stopped background worker '{worker.Name}'");
            }

            catch (OperationCanceledException)
            {
                Console.WriteLine($"stopped background worker '{worker.Name}' through cancellation.");
            }

            catch (Exception ex)
            {
                Console.WriteLine(
                    $"stopped background worker '{worker.Name}' but an unhandled exception occurred: {ex}");
            }
        }

        await _flushTransactionsToDataPagesTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        Console.WriteLine("journal fsync");
        Journal.FlushToDisk();
        Journal.Dispose();
    }

    private Task OnStoppedAsync()
    {
        Console.WriteLine("database gracefully shut down.\nit is now safe to turn off your computer.");
        _state = DatabaseState.Stopped;
        return Task.CompletedTask;
    }
}