namespace JotDB.Storage.Journal;

public sealed class WriteAheadLog : IDisposable
{
    private readonly WriteAheadLogTransactionBuffer _buffer = new();
    private ulong _storageTransactionSequence;

    public WriteAheadLog()
    {
        File.Delete("journal.txt");
        WriteAheadLogFile = WriteAheadLogFile.Open("journal.txt");
    }

    public WriteAheadLogFile WriteAheadLogFile { get; }

    public void Dispose()
    {
        WriteAheadLogFile.Dispose();
    }

    public Task AppendAsync(Transaction transaction)
    {
        var walTransaction = new WriteAheadLogTransaction(transaction);

        return _buffer.WriteTransactionAsync(walTransaction);
    }

    public async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        while (await _buffer.WaitForTransactionsAsync(
                   cancellationToken).ConfigureAwait(false))
        {
            var transactionNumber = Interlocked.Increment(ref _storageTransactionSequence);

            var storageTransaction = new StorageTransaction(
                transactionNumber: transactionNumber,
                WriteAheadLogFile,
                _buffer);

            cancellationToken.ThrowIfCancellationRequested();
            storageTransaction.Commit(cancellationToken);
        }
    }

    public void FlushToDisk()
    {
        WriteAheadLogFile.FlushToDisk();
    }
}