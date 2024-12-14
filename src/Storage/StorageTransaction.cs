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

        try
        {
            foreach (var transaction in _transactions.Take(8))
            {
                transactionCount++;

                try
                {
                    transaction.FinalizeCommit(commitAwaiter.Task);
                }

                catch (Exception ex)
                {
                    transaction.Abort(ex);
                }
            }
        }

        finally
        {
            commitAwaiter.SignalCompletion();
        }

        Console.WriteLine($"strx {TransactionNumber} committed {transactionCount} trx in {watch.ElapsedTicks} ticks");
    }

    public void Rollback()
    {
    }

    public void Dispose()
    {
    }
}