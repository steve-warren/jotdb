namespace JotDB.Storage;

public class StorageEnvironment
{
    private readonly TransactionStream _transactionStream = new();
    private ulong _storageTransactionSequence;
    private ulong _transactionSequence;

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

    public async Task ReceiveTransactionsAsync(CancellationToken cancellationToken)
    {
        while (await _transactionStream.WaitForTransactionsAsync(
                   cancellationToken).ConfigureAwait(false))
        {
            var transactionNumber = Interlocked.Increment(ref _storageTransactionSequence);

            var storageTransaction = new StorageTransaction(
                transactionNumber: transactionNumber,
                transactions: _transactionStream.ReadTransactions());

            cancellationToken.ThrowIfCancellationRequested();
            storageTransaction.Commit(cancellationToken);
        }
    }
}