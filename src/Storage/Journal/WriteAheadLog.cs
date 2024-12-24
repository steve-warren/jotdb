using System.Diagnostics.CodeAnalysis;
using JotDB.Memory;
using JotDB.Metrics;

namespace JotDB.Storage.Journal;

public sealed class WriteAheadLog : IDisposable
{
    private readonly WriteAheadLogTransactionBuffer _transactionBuffer = new();
    private readonly AlignedMemory _storageBuffer;
    private ulong _storageTransactionSequence;

    public WriteAheadLog(bool inMemory)
    {
        File.Delete("journal.txt");
        LogFile = inMemory
            ? new NullWriteAheadLogFile()
            : WriteAheadLogFile.Open("journal.txt");

        _storageBuffer = AlignedMemory.Allocate(4 * 1024 * 1024);
    }

    ~WriteAheadLog()
    {
        Dispose();
    }

    public IWriteAheadLogFile LogFile { get; }

    public void Dispose()
    {
        (LogFile as IDisposable)?.Dispose();
        _storageBuffer.Dispose();

        GC.SuppressFinalize(this);
    }

    public async Task AppendAsync(DatabaseTransaction databaseTransaction)
    {
        Ensure.NotNull(databaseTransaction);
        Ensure.That(
            databaseTransaction.Size <= 4096,
            "Transaction size must be less than or equal to 4096 bytes.");

        using var walTransaction = new WriteAheadLogTransaction
            (databaseTransaction);

        await _transactionBuffer.WriteTransactionAsync(walTransaction)
            .ConfigureAwait
                (false);
    }

    /// <summary>
    /// Continuously flushes the transaction buffer to the write-ahead log until a cancellation is requested.
    /// </summary>
    /// <param name="cancellationToken">The token used to monitor for request cancellation and stop the operation.</param>
    /// <remarks>This method will block until a cancellation is requested.</remarks>
    [DoesNotReturn]
    public void MonitorAndFlushBuffers(CancellationToken cancellationToken)
    {
        while (true)
        {
            // early check
            cancellationToken.ThrowIfCancellationRequested();

            _transactionBuffer.Wait(cancellationToken);

            var transactionNumber =
                Interlocked.Increment(ref _storageTransactionSequence);

            var storageTransaction = new StorageTransaction(
                transactionNumber: transactionNumber,
                LogFile,
                _transactionBuffer,
                _storageBuffer);

            // early check
            cancellationToken.ThrowIfCancellationRequested();

            storageTransaction.Commit(cancellationToken);

            MetricSink.StorageTransactions.Apply(storageTransaction);
        }
    }

    public void FlushToDisk()
    {
        LogFile.FlushToDisk();
    }
}