namespace JotDB.Storage;

public sealed class StorageEnvironment : IDisposable
{
    private readonly TransactionBuffer _transactionBuffer = new();
    private ulong _storageTransactionSequence;
    private ulong _transactionSequence;

    public JournalFile Journal { get; private set; }

    public void Dispose()
    {
        Journal.Dispose();
    }

    public void CreateWriteAheadLog()
    {
        File.Delete("journal.txt");
        Journal = JournalFile.Open("journal.txt");
    }

    public Transaction CreateTransaction(ReadOnlyMemory<byte> data)
    {
        var transaction = new Transaction(15_000, _transactionBuffer)
        {
            Type = TransactionType.Insert,
            Data = data,
            Number = Interlocked.Increment(ref _transactionSequence)
        };

        return transaction;
    }

    public async Task FlushTransactionBufferAsync(CancellationToken cancellationToken)
    {
        while (await _transactionBuffer.WaitForTransactionsAsync(
                   cancellationToken).ConfigureAwait(false))
        {
            var transactionNumber = Interlocked.Increment(ref _storageTransactionSequence);

            var storageTransaction = new StorageTransaction(
                transactionNumber: transactionNumber,
                transactions: _transactionBuffer.ReadTransactions(),
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