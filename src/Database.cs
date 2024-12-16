using System.Diagnostics;
using JotDB.Storage;

namespace JotDB;

public sealed class Database : IDisposable
{
    private readonly StorageEnvironment _storageEnvironment = new();
    private readonly List<BackgroundWorker> _backgroundWorkers = [];
    private readonly TaskCompletionSource _runningStateTask = new();
    private volatile DatabaseState _state = DatabaseState.Stopped;
    private readonly CancellationTokenSource _shutdownTokenSource = new();
    private Task _flushTransactionsToDataPagesTask = Task.CompletedTask;

    public DatabaseState State => _state;

    public void AddBackgroundWorker(
        string name,
        Func<Database, CancellationToken, Task> work)
    {
        var worker = new BackgroundWorker(this, name, work);
        _backgroundWorkers.Add(worker);
    }

    public async Task InsertDocumentAsync(ReadOnlyMemory<byte> document)
    {
        using var transaction =
            _storageEnvironment.CreateTransaction(document,
                TransactionType.Insert);
        await transaction.CommitAsync().ConfigureAwait(false);

        Console.WriteLine($"trx completed in {transaction.ExecutionTime
            .TotalMilliseconds} ms");
    }

    /// <summary>
    /// Runs the database, starting all registered background workers and waits for the shutdown signal.
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
        _storageEnvironment.WriteAheadLog.Dispose();
    }

    private async Task OnStartingAsync()
    {
        _state = DatabaseState.Starting;

        Console.WriteLine("starting JotDB");
        Console.WriteLine(
            $"{Environment.OSVersion.VersionString} ({Environment.OSVersion.Platform})");
        Console.WriteLine($"{Environment.ProcessorCount} cores");

        ThreadPool.GetAvailableThreads(out var availableWorkerThreads,
            out var availableIoThreads);
        ThreadPool.GetMaxThreads(out var maxWorkerThreads,
            out var maxIoThreads);
        ThreadPool.GetMinThreads(out var minWorkerThreads,
            out var minIoThreads);

        Console.WriteLine("Thread Pool Status:");
        Console.WriteLine(
            $"  Worker Threads: {availableWorkerThreads}/{maxWorkerThreads} (Min: {minWorkerThreads})");
        Console.WriteLine(
            $"  IO Threads: {availableIoThreads}/{maxIoThreads} (Min: {minIoThreads})");

        foreach (var worker in _backgroundWorkers)
        {
            try
            {
                Console.WriteLine(
                    $"starting background worker '{worker.Name}'");
                worker.Start();
                Console.WriteLine($"started background worker '{worker.Name}'");
            }

            catch (Exception ex)
            {
                Console.WriteLine(
                    $"Failed to start background worker '{worker.Name}'. Exception: {ex}");
            }
        }

        await Task.Delay(1000);
    }

    private Task OnRunningAsync()
    {
        _state = DatabaseState.Running;

        Console.WriteLine("running database");

        _flushTransactionsToDataPagesTask =
            _storageEnvironment.WriteAheadLog.FlushBufferAsync(
                _shutdownTokenSource.Token);

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
                Console.WriteLine(
                    $"stopping background worker '{worker.Name}'");
                await worker.StopAsync();
                Console.WriteLine($"stopped background worker '{worker.Name}'");
            }

            catch (OperationCanceledException)
            {
                Console.WriteLine(
                    $"stopped background worker '{worker.Name}' through cancellation.");
            }

            catch (Exception ex)
            {
                Console.WriteLine(
                    $"stopped background worker '{worker.Name}' but an unhandled exception occurred: {ex}");
            }
        }

        await _flushTransactionsToDataPagesTask.ConfigureAwait(
            ConfigureAwaitOptions.SuppressThrowing);

        Console.WriteLine("journal fsync");
        _storageEnvironment.FlushToDisk();
        _storageEnvironment.WriteAheadLog.Dispose();
    }

    private Task OnStoppedAsync()
    {
        Console.WriteLine(
            "database gracefully shut down.\nit is now safe to turn off your computer.");
        _state = DatabaseState.Stopped;
        return Task.CompletedTask;
    }
}