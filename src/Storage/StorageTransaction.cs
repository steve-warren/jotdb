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
    private readonly Queue<WriteAheadLogTransaction> _completedBuffer;
    private readonly AlignedMemory _storageMemory;

    public StorageTransaction(
        uint storageTransactionSequenceNumber,
        WriteAheadLogFile writeAheadLogFile,
        WriteAheadLogTransactionBuffer transactionBuffer,
        Queue<WriteAheadLogTransaction> completedBuffer,
        AlignedMemory storageMemory)
    {
        StorageTransactionSequenceNumber = storageTransactionSequenceNumber;
        _writeAheadLogFile = writeAheadLogFile;
        _transactionBuffer = transactionBuffer;
        _completedBuffer = completedBuffer;
        _storageMemory = storageMemory;
    }

    public uint StorageTransactionSequenceNumber { get; }
    public int TransactionMergeCount { get; private set; }
    public TimeSpan ExecutionTime { get; private set; }
    public int BytesCommitted { get; private set; }

    /// <summary>
    /// Commits the current storage transaction.
    /// This method ensures that all pending operations in the transaction buffer are written
    /// to storage and properly handled before signaling completion.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    public void Commit(CancellationToken cancellationToken = default)
    {
        using var commitAwaiter = new AsyncAwaiter(cancellationToken);
        var writer = new AlignedMemoryWriter(_storageMemory);
        var transactionMergeCount = 0;
        var commitSequenceNumber = 0U;

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
                transactionMergeCount++;
                transaction.Write(
                    ref writer,
                    StorageTransactionSequenceNumber,
                    ++commitSequenceNumber,
                    now);

                transaction.Commit(when: commitAwaiter.CompletedTask);

                _completedBuffer.Enqueue(transaction);
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
            TransactionMergeCount = transactionMergeCount;
            BytesCommitted = writer.BytesWritten;

            MetricSink.StorageTransactions.Apply(this);
        }
    }
}