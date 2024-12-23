using System.Diagnostics;
using JotDB.Pages;
using JotDB.Storage;
using JotDB.Storage.Journal;

namespace JotDB;

/// <summary>
/// Represents a transactional operation within the database system.
/// </summary>
/// <remarks>
/// A transaction encapsulates a series of operations that can be committed or rolled back as a single unit.
/// Transactions are uniquely identified by their sequence numbers and can store associated data.
/// </remarks>
public sealed class DatabaseTransaction : IDisposable
{
    private readonly WriteAheadLog _wal;
    private readonly PageBuffer _pageBuffer = new();
    private readonly CancellationTokenSource _cts;
    private uint _operationSequenceNumber = 0;

    public DatabaseTransaction(
        int timeout,
        WriteAheadLog wal)
    {
        _wal = wal;
        _cts = new CancellationTokenSource(timeout);
    }

    public ulong TransactionSequenceNumber { get; init; }
    public ReadOnlyMemory<byte> Data { get; set; }
    public TransactionType Type { get; set; }
    public TimeSpan ExecutionTime { get; private set; }
    public uint Size { get; private set; }
    public uint OperationCount { get; private set; }
    public List<DatabaseOperation> Operations { get; } = [];

    public DatabaseOperation AddOperation(
        ReadOnlyMemory<byte> data,
        DatabaseOperationType type)
    {
        var operation = new DatabaseOperation(
            ++_operationSequenceNumber,
            TransactionSequenceNumber,
            data,
            type);

        Operations.Add(operation);

        Size += (uint)data.Length;
        OperationCount++;

        return operation;
    }

    public async Task CommitAsync()
    {
        var watch = Stopwatch.StartNew();
        await _wal.AppendAsync(this).ConfigureAwait(false);
        _pageBuffer.Write(this);
        ExecutionTime = watch.Elapsed;
    }

    public void Dispose()
    {
        _cts.Cancel();
        _cts.Dispose();
    }
}
