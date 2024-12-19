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

    public Task AppendAsync(DatabaseTransaction databaseTransaction)
    {
        var walTransaction = new WriteAheadLogTransaction(databaseTransaction);

        return _buffer.WriteTransactionAsync(walTransaction);
    }

    public void FlushBuffer(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            _buffer.WaitForTransactions(cancellationToken);

            var transactionNumber =
                Interlocked.Increment(ref _storageTransactionSequence);

            var storageTransaction = new StorageTransaction(
                transactionNumber: transactionNumber,
                LogFile,
                _buffer);

            storageTransaction.Commit(cancellationToken);
        }
    }

    public void FlushToDisk()
    {
        LogFile.FlushToDisk();
    }
}