using System.Diagnostics;
using JotDB.Storage.Journal;

namespace JotDB.Storage;

/// <summary>
/// Represents a transactional operation within the system.
/// </summary>
/// <remarks>
/// A transaction encapsulates a series of operations that can be committed or rolled back as a single unit.
/// Transactions are uniquely identified by their sequence numbers and can store associated data.
/// </remarks>
public sealed class Transaction : IDisposable
{
    private readonly WriteAheadLog _wal;
    private readonly CancellationTokenSource _cts;

    public Transaction(
        int timeout,
        WriteAheadLog wal)
    {
        _wal = wal;
        _cts = new CancellationTokenSource(timeout);
    }

    public ulong TransactionSequenceNumber { get; init; }
    public ReadOnlyMemory<byte> Data { get; init; }
    public TransactionType Type { get; init; }
    public TimeSpan ExecutionTime { get; private set; }
    public async Task CommitAsync()
    {
        var watch = Stopwatch.StartNew();
        await _wal.AppendAsync(this).ConfigureAwait(false);
        ExecutionTime = watch.Elapsed;
    }

    public void Dispose()
    {
        _cts.Cancel();
        _cts.Dispose();
    }
}