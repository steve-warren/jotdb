using JotDB.Metrics;
using JotDB.Storage;
using JotDB.Storage.Journal;

namespace JotDB;

public sealed class Database : IDisposable
{
    private readonly List<BackgroundWorker> _backgroundWorkers = [];
    private readonly TaskCompletionSource _runningStateTask = new();
    private volatile DatabaseState _state = DatabaseState.Stopped;
    private readonly CancellationTokenSource _shutdownTokenSource = new();
    private Task _flushTransactionsToDataPagesTask = Task.CompletedTask;
    private ExponentialMovingAverage _ema = new(0.2);
    private ulong _transactionSequence;

    public Database(bool inMemory = true)
    {
        WriteAheadLog = new WriteAheadLog(inMemory);
    }

    public DatabaseState State => _state;
    public TimeSpan AverageTransactionExecutionTime => _ema.Value;
    public WriteAheadLog WriteAheadLog { get; }

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
            CreateTransaction(document, TransactionType.Write);
        await transaction.CommitAsync().ConfigureAwait(false);
        _ema.Update(transaction.ExecutionTime);
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
        WriteAheadLog.Dispose();
    }

    private DatabaseTransaction CreateTransaction(
        ReadOnlyMemory<byte> data,
        TransactionType transactionType)
    {
        var transaction = new DatabaseTransaction(15_000, WriteAheadLog)
        {
            Type = transactionType,
            Data = data,
            TransactionSequenceNumber =
                Interlocked.Increment(ref _transactionSequence)
        };

        return transaction;
    }

    private void FlushToDisk()
    {
        WriteAheadLog.FlushToDisk();
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
            Task.Factory.StartNew(() =>
                    WriteAheadLog.FlushBuffer(
                        _shutdownTokenSource.Token),
                TaskCreationOptions.LongRunning);

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
        FlushToDisk();
        WriteAheadLog.Dispose();
    }

    private Task OnStoppedAsync()
    {
        Console.WriteLine(
            "database gracefully shut down.\nit is now safe to turn off your computer.");
        _state = DatabaseState.Stopped;
        return Task.CompletedTask;
    }
}