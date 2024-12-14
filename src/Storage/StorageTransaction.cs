using System.Diagnostics;
using JotDB.Threading;

namespace JotDB.Storage;

public class StorageTransaction : IDisposable
{
    private readonly IEnumerable<Transaction> _transactions;
    private readonly JournalFile _journal;

    public StorageTransaction(
        ulong transactionNumber,
        IEnumerable<Transaction> transactions,
        JournalFile journal)
    {
        TransactionNumber = transactionNumber;
        _transactions = transactions;
        _journal = journal;
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
                if (block.TryWrite(transaction.Data.Span))
                    transaction.FinalizeCommit(commitAwaiter.Task);
                else if (transaction.Data.Length > block.Size)
                    transaction.Abort(new Exception("Failed to write to storage block"));
                else
                {
                    block = new StorageBlock(0, 0, AlignedMemoryPool.Default.Rent());
                    blocks.AddLast(block);

                    if (block.TryWrite(transaction.Data.Span))
                        transaction.FinalizeCommit(commitAwaiter.Task);
                    else
                        transaction.Abort(new Exception("Failed to write to storage block"));
                }
            }

            _journal.WriteToDisk(blocks);
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