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

        return _buffer.WriteAsync(walTransaction);
    }

    /// <summary>
    /// Waits for transactions to be available in the transaction buffer and processes them
    /// by committing each as a storage transaction. This method continues processing until
    /// the provided cancellation token requests cancellation.
    /// </summary>
    /// <param name="cancellationToken">A token that can be used to signal the request for
    /// cancellation of the waiting and flushing operation.</param>
    public void WaitAndCommitBufferUntil(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            _buffer.Wait(cancellationToken);

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