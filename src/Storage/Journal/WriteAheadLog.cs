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

    public async Task AppendAsync(DatabaseTransaction databaseTransaction)
    {
        Ensure.NotNull(databaseTransaction);
        Ensure.That(
            databaseTransaction.Size <= 4096,
            "Transaction size must be 4096 bytes or less.");

        using var walTransaction = new WriteAheadLogTransaction
            (databaseTransaction);

        await _buffer.WriteTransactionAsync(walTransaction).ConfigureAwait
            (false);
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