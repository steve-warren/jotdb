using System.Diagnostics.CodeAnalysis;
using JotDB.Memory;
using JotDB.Metrics;

namespace JotDB.Storage.Journal;

public sealed class WriteAheadLog : IDisposable
{
    private readonly WriteAheadLogTransactionBuffer _transactionBuffer = new();
    private ulong _storageTransactionSequence;
    private readonly WriteAheadLogFile _file;
    private readonly AlignedMemory _fileBuffer;

    public WriteAheadLog(bool inMemory)
    {
        _file = inMemory
            ? new NullWriteAheadLogFile()
            : SafeFileHandleWriteAheadLogFile.Open();

        _fileBuffer = AlignedMemory.Allocate(4 * 1024 * 1024);
    }

    ~WriteAheadLog()
    {
        Dispose();
    }

    public void Dispose()
    {
        _file.Flush();
        _file.Dispose();
        _fileBuffer.Dispose();

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

            // block the current thread
            // until the buffer has transactions
            _transactionBuffer.Wait(cancellationToken);

            var transactionNumber =
                Interlocked.Increment(ref _storageTransactionSequence);

            var storageTransaction = new StorageTransaction(
                transactionNumber: transactionNumber,
                _file,
                _transactionBuffer,
                _fileBuffer);

            // early check
            cancellationToken.ThrowIfCancellationRequested();

            // merge and commit transactions from the buffer
            storageTransaction.MergeCommit(cancellationToken);

            MetricSink.StorageTransactions.Apply(storageTransaction);

            // here we need to check the health of the wal file
            // and roll over if necessary.

            // fsync and rotate the file if necessary
            _file.Rotate();
        }
    }
}