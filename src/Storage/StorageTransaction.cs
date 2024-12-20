using System.Diagnostics;
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
    private readonly IWriteAheadLogFile _writeAheadLogFile;
    private readonly WriteAheadLogTransactionBuffer _transactionBuffer;

    public StorageTransaction(
        ulong transactionNumber,
        IWriteAheadLogFile writeAheadLogFile,
        WriteAheadLogTransactionBuffer transactionBuffer)
    {
        TransactionNumber = transactionNumber;
        _writeAheadLogFile = writeAheadLogFile;
        _transactionBuffer = transactionBuffer;
    }

    public ulong TransactionNumber { get; }
    public TimeSpan ExecutionTime { get; private set; }

    /// <summary>
    /// Commits the current storage transaction.
    /// This method ensures that all pending operations in the transaction buffer are written
    /// to storage and properly handled before signaling completion.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    public void Commit(CancellationToken cancellationToken = default)
    {
        using var commitAwaiter = new AsyncAwaiter(cancellationToken);
        var commitSequenceNumber = 0U;
        var memory = AlignedMemoryPool.Default.Rent();
        var writer = new AlignedMemoryWriter(memory);
        var watch = StopwatchSlim.StartNew();

        try
        {
            var enumerator = new WriteAheadLogTransactionBuffer.Enumerator(
                _transactionBuffer,
                4096,
                cancellationToken);
            
            var timestamp = DateTime.UtcNow.Ticks;

            while(enumerator.MoveNext(out var transaction))
            {
                if (transaction.TryWriteToMemory(
                        ref writer,
                        ++commitSequenceNumber,
                        timestamp))
                    transaction.Commit(after: commitAwaiter.Task);

                else if (transaction.Size > memory.Size)
                    transaction.Abort(
                        new Exception("Data would be truncated."));
            }

            if (writer.BytesWritten == 0)
                return;

            writer.ZeroRemainingBytes();

            _writeAheadLogFile.WriteToDisk(memory);
            ExecutionTime = watch.Elapsed;
        }

        finally
        {
            commitAwaiter.SignalCompletion();

            AlignedMemoryPool.Default.Return(memory);
        }
    }
}