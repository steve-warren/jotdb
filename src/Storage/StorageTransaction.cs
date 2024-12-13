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
        var transactionCount = 0;
        using var mre = new AsyncManualResetEvent(cancellationToken);

        try
        {
            foreach (var transaction in _transactions.Take(8))
            {
                transactionCount++;
                transaction.CompleteCommitAfter(mre.Task);
            }
        }

        finally
        {
            mre.SetCompleted();
        }

        Console.WriteLine($"strx {TransactionNumber} committed {transactionCount} trx.");
    }

    public void Rollback()
    {
    }

    public void Dispose()
    {
    }
}