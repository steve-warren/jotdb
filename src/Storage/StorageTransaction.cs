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
    private readonly WriteAheadLogFile _writeAheadLogFile;

    public StorageTransaction(
        ulong transactionNumber,
        IEnumerable<WriteAheadLogTransaction> transactions,
        WriteAheadLogFile writeAheadLogFile)
    {
        TransactionNumber = transactionNumber;
        _transactions = transactions;
        _writeAheadLogFile = writeAheadLogFile;
    }

    public ulong TransactionNumber { get; }

    public void Commit(CancellationToken cancellationToken = default)
    {
        using var commitAwaiter = new AsyncAwaiter(cancellationToken);
        var commitSequenceNumber = 0UL;
        var watch = Stopwatch.StartNew();

        var blocks = new LinkedList<StorageBlock>();
        var currentBlock = new StorageBlock(0, 0, AlignedMemoryPool.Default.Rent());

        blocks.AddFirst(currentBlock);

        try
        {
            foreach (var transaction in _transactions.Take(1024))
            {
                commitSequenceNumber++;

                transaction.Prepare(commitSequenceNumber, timestamp: DateTime.UtcNow.Ticks);

                if (currentBlock.TryWrite(transaction.Transaction.Data.Span))
                    transaction.Commit(after: commitAwaiter.Task);

                else if (transaction.Transaction.Data.Length > currentBlock.Size)
                    transaction.Abort(new Exception("Failed to write to storage block"));

                else
                {
                    currentBlock = new StorageBlock(0, 0, AlignedMemoryPool.Default.Rent());
                    blocks.AddLast(currentBlock);

                    if (currentBlock.TryWrite(transaction.Transaction.Data.Span))
                        transaction.Commit(after: commitAwaiter.Task);
                    else
                        transaction.Abort(new Exception("Failed to write to storage block"));
                }
            }

            _writeAheadLogFile.WriteToDisk(blocks);
        }

        finally
        {
            commitAwaiter.SignalCompletion();

            foreach (var usedBlock in blocks)
                AlignedMemoryPool.Default.Return(usedBlock.Memory);
        }

        Console.WriteLine(
            $"strx {TransactionNumber} committed {commitSequenceNumber} trx in {watch.ElapsedTicks} ticks");
    }

    public void Dispose()
    {
    }
}