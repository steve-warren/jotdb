using System.Diagnostics;
using JotDB.Memory;
using JotDB.Threading;

namespace JotDB.Storage;

/// <summary>
/// Represents a transaction spanning multiple write-ahead log transactions in a storage context.
/// </summary>
/// <remarks>
/// This class is responsible for managing the lifecycle of a transaction, including committing
/// the associated write-ahead log transactions to storage and handling any necessary cleanup during disposal.
/// </remarks>
public sealed class StorageTransaction : IDisposable
{
    private readonly WriteAheadLogFile _writeAheadLogFile;
    private readonly WriteAheadLogTransactionBuffer _transactionBuffer;

    public StorageTransaction(
        ulong transactionNumber,
        WriteAheadLogFile writeAheadLogFile,
        WriteAheadLogTransactionBuffer transactionBuffer)
    {
        TransactionNumber = transactionNumber;
        _writeAheadLogFile = writeAheadLogFile;
        _transactionBuffer = transactionBuffer;
    }

    public ulong TransactionNumber { get; }

    /// <summary>
    /// Commits the current storage transaction asynchronously.
    /// This method ensures that all pending operations in the transaction buffer are written
    /// to storage and properly handled before signaling completion.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous commit operation.</returns>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        using var commitAwaiter = new AsyncAwaiter(cancellationToken);
        var commitSequenceNumber = 0UL;
        var watch = Stopwatch.StartNew();

        var memory = AlignedMemoryPool.Default.Rent();
        var writer = new AlignedMemoryWriter(memory);

        try
        {
            await foreach (var transaction in _transactionBuffer
                               .ReadTransactionsAsync(
                                   4096,
                                   TimeSpan.FromMilliseconds(1),
                                   cancellationToken))
            {
                if (transaction.TryWrite(
                        ref writer,
                        ++commitSequenceNumber,
                        DateTime.UtcNow.Ticks))
                        
                    transaction.Commit(after: commitAwaiter.Task);

                else if (transaction.Size > memory.Size)
                    transaction.Abort(
                        new Exception("Data would be truncated."));
            }
            
            if (writer.BytesWritten == 0)
                return;

            writer.ZeroUnusedBytes();

            //_writeAheadLogFile.WriteToDisk(memory);
            await Task.Delay(TimeSpan.FromMilliseconds(0.01), CancellationToken.None);

            Console.WriteLine(
                $"strx {TransactionNumber} committed {writer.BytesWritten} bytes from {commitSequenceNumber} trx in {watch.Elapsed.TotalMilliseconds} ms");
        }

        finally
        {
            commitAwaiter.SignalCompletion();

            AlignedMemoryPool.Default.Return(memory);
        }
    }

    public void Dispose()
    {
    }
}