using System.Diagnostics.CodeAnalysis;

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

    /// <summary>
    /// Continuously flushes the transaction buffer to the write-ahead log until a cancellation is requested.
    /// </summary>
    /// <param name="cancellationToken">The token used to monitor for request cancellation and stop the operation.</param>
    [DoesNotReturn]
    public void MonitorAndFlushBuffers(CancellationToken cancellationToken)
    {
        while (true)
        {
            // early check
            cancellationToken.ThrowIfCancellationRequested();

            _buffer.Wait(cancellationToken);

            var transactionNumber =
                Interlocked.Increment(ref _storageTransactionSequence);

            var storageTransaction = new StorageTransaction(
                transactionNumber: transactionNumber,
                LogFile,
                _buffer);

            // early check
            cancellationToken.ThrowIfCancellationRequested();

            storageTransaction.Commit(cancellationToken);
        }
    }

    public void FlushToDisk()
    {
        LogFile.FlushToDisk();
    }
}