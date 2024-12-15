using System.Diagnostics;
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
public class StorageTransaction : IDisposable
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

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        using var commitAwaiter = new AsyncAwaiter(cancellationToken);
        var commitSequenceNumber = 0UL;
        var watch = Stopwatch.StartNew();

        var block =
            new StorageBlock(0, 0, AlignedMemoryPool.Default.Rent());

        try
        {
            await foreach (var transaction in _transactionBuffer
                               .ReadTransactionsAsync(
                                   4096,
                                   TimeSpan.FromMilliseconds(10),
                                   cancellationToken))
            {
                transaction.Prepare(++commitSequenceNumber,
                    timestamp: DateTime.UtcNow.Ticks);

                if (transaction.TryCopyTo(block))
                    transaction.Commit(after: commitAwaiter.Task);

                else if (transaction.Size > block.Size)
                    transaction.Abort(
                        new Exception("Data would be truncated."));
            }

            if (block.BytesWritten <= 0)
                return;

            _writeAheadLogFile.WriteToDisk(block);

            Console.WriteLine(
                $"strx {TransactionNumber} committed {block.BytesWritten} bytes from {commitSequenceNumber} trx in {watch.Elapsed.TotalMilliseconds} ms");
        }

        finally
        {
            commitAwaiter.SignalCompletion();

            AlignedMemoryPool.Default.Return(block.Memory);
        }
    }

    public void Dispose()
    {
    }
}