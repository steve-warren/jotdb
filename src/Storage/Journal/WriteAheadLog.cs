namespace JotDB.Storage.Journal;

public sealed class WriteAheadLog : IDisposable
{
    private readonly WriteAheadLogTransactionBuffer _buffer = new();
    private ulong _storageTransactionSequence;

    public WriteAheadLog(bool inMemory)
    {
        LogFile = inMemory
            ? new NullWriteAheadLogFile()
            : WriteAheadLogFile.Open("journal.txt");
    }

    public IWriteAheadLogFile LogFile { get; }

    public void Dispose()
    {
        (LogFile as IDisposable)?.Dispose();
    }

    public Task AppendAsync(Transaction transaction)
    {
        var walTransaction = new WriteAheadLogTransaction(transaction);

        return _buffer.WriteTransactionAsync(walTransaction);
    }

    public void FlushBuffer(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            cancellationToken.ThrowIfCancellationRequested();

            _buffer.WaitForTransactions(cancellationToken);

            var transactionNumber =
                Interlocked.Increment(ref _storageTransactionSequence);

            var storageTransaction = new StorageTransaction(
                transactionNumber: transactionNumber,
                LogFile,
                _buffer);

            cancellationToken.ThrowIfCancellationRequested();
            storageTransaction.Commit(cancellationToken);
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    public void FlushToDisk()
    {
        LogFile.FlushToDisk();
    }
}