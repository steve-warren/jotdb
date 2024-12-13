namespace JotDB.Storage;

public sealed class StorageEnvironment : IDisposable
{
    private readonly TransactionStream _transactionStream = new();
    private ulong _storageTransactionSequence;
    private ulong _transactionSequence;

    public StorageEnvironment()
    {
        File.Delete("journal.txt");
        Journal = JournalFile.Open("journal.txt");
    }

    public JournalFile Journal { get; }

    public void Dispose()
    {
        Journal.Dispose();
    }

    public Transaction CreateTransaction(ReadOnlyMemory<byte> data)
    {
        var transaction = new Transaction(15_000, _transactionStream)
        {
            Type = TransactionType.Insert,
            Data = data,
            Number = Interlocked.Increment(ref _transactionSequence)
        };

        return transaction;
    }

    public async Task WalWriteLoop(CancellationToken cancellationToken)
    {
        while (await _transactionStream.WaitForTransactionsAsync(
                   cancellationToken).ConfigureAwait(false))
        {
            var transactionNumber = Interlocked.Increment(ref _storageTransactionSequence);

            var storageTransaction = new StorageTransaction(
                transactionNumber: transactionNumber,
                transactions: _transactionStream.ReadTransactions(),
                Journal);

            cancellationToken.ThrowIfCancellationRequested();
            storageTransaction.Commit(cancellationToken);
        }
    }

    public void FlushToDisk()
    {
        Journal.FlushToDisk();
    }
}