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
    private readonly IEnumerable<WriteAheadLogTransaction> _transactions;
    private readonly WriteAheadLogFile _writeAheadLog;

    public StorageTransaction(
        ulong transactionNumber,
        IEnumerable<WriteAheadLogTransaction> transactions,
        WriteAheadLogFile writeAheadLog)
    {
        TransactionNumber = transactionNumber;
        _transactions = transactions;
        _writeAheadLog = writeAheadLog;
    }

    public ulong TransactionNumber { get; }

    public void Commit(CancellationToken cancellationToken = default)
    {
        using var commitAwaiter = new AsyncAwaiter(cancellationToken);
        var transactionCount = 0;
        var watch = Stopwatch.StartNew();

        var blocks = new LinkedList<StorageBlock>();
        var block = new StorageBlock(0, 0, AlignedMemoryPool.Default.Rent());

        blocks.AddFirst(block);

        try
        {
            foreach (var transaction in _transactions.Take(1024))
            {
                transactionCount++;
                if (block.TryWrite(transaction.Transaction.Data.Span))
                    transaction.CommitAfter(commitAwaiter.Task);
                else if (transaction.Transaction.Data.Length > block.Size)
                    transaction.Abort(new Exception("Failed to write to storage block"));
                else
                {
                    block = new StorageBlock(0, 0, AlignedMemoryPool.Default.Rent());
                    blocks.AddLast(block);

                    if (block.TryWrite(transaction.Transaction.Data.Span))
                        transaction.CommitAfter(commitAwaiter.Task);
                    else
                        transaction.Abort(new Exception("Failed to write to storage block"));
                }
            }

            _writeAheadLog.WriteToDisk(blocks);
        }

        finally
        {
            commitAwaiter.SignalCompletion();

            foreach (var usedBlock in blocks)
                AlignedMemoryPool.Default.Return(usedBlock.Memory);
        }

        Console.WriteLine($"strx {TransactionNumber} committed {transactionCount} trx in {watch.ElapsedTicks} ticks");
    }

    public void Dispose()
    {
    }
}