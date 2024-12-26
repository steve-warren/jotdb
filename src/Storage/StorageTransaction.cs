using JotDB.Memory;
using JotDB.Metrics;
using JotDB.Storage.Journal;
using JotDB.Threading;

namespace JotDB.Storage;

/// <summary>
/// Represents a transaction spanning multiple write-ahead log transactions in a storage context.
/// </summary>
/// <remarks>
/// This class is responsible for managing the lifecycle of a transaction, including committing
/// the associated write-ahead log transactions to storage and handling any necessary cleanup during disposal.
/// </remarks>
public sealed class StorageTransaction
{
    private readonly WriteAheadLogFile _writeAheadLogFile;
    private readonly WriteAheadLogTransactionBuffer _transactionBuffer;
    private readonly AlignedMemory _storageMemory;

    public StorageTransaction(
        ulong transactionNumber,
        WriteAheadLogFile writeAheadLogFile,
        WriteAheadLogTransactionBuffer transactionBuffer,
        AlignedMemory storageMemory)
    {
        TransactionNumber = transactionNumber;
        _writeAheadLogFile = writeAheadLogFile;
        _transactionBuffer = transactionBuffer;
        _storageMemory = storageMemory;
    }

    public ulong TransactionNumber { get; }
    public int TransactionMergeCount { get; private set; }
    public TimeSpan ExecutionTime { get; private set; }
    public int BytesCommitted { get; private set; }

    /// <summary>
    /// Commits the current storage transaction.
    /// This method ensures that all pending operations in the transaction buffer are written
    /// to storage and properly handled before signaling completion.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    public void MergeCommit(CancellationToken cancellationToken = default)
    {
        using var commitAwaiter = new AsyncAwaiter(cancellationToken);
        var commitSequenceNumber = 0U;
        var writer = new AlignedMemoryWriter(_storageMemory);
        var walTransactionCount = 0;

        try
        {
            var now = DateTime.UtcNow.Ticks;
            using var enumerator =
                new WriteAheadLogTransactionBuffer.Enumerator(
                    _transactionBuffer,
                    _storageMemory.Size);

            var watch = StopwatchSlim.StartNew();

            while (enumerator.MoveNext(out var transaction))
            {
                walTransactionCount++;
                transaction.Write(
                    ref writer,
                    ++commitSequenceNumber,
                    now);
                transaction.CommitWhen(after: commitAwaiter.CompletedTask);
            }

            _writeAheadLogFile.Write(writer.AlignedSpan);
            ExecutionTime = watch.Elapsed;
        }

        finally
        {
            // signal all waiting tasks that their transactions are completed.
            commitAwaiter.SignalCompletion();

            // clear the bytes used
            writer.ZeroUsedBytes();
            TransactionMergeCount = walTransactionCount;
            BytesCommitted = writer.BytesWritten;

            MetricSink.StorageTransactions.Apply(this);
        }
    }
}