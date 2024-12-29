using System.Diagnostics.CodeAnalysis;
using JotDB.Memory;

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

    /// <summary>
    /// Appends a database transaction to the write-ahead log asynchronously.
    /// </summary>
    /// <param name="databaseTransaction">The database transaction to append to the write-ahead log. This transaction must not be null, and its size must not exceed 4096 bytes.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>If the transaction size exceeds the specified limit, an exception will be thrown. This method ensures the transaction is safely added to the write-ahead log buffer.</remarks>
    public async Task AppendAsync(
        DatabaseTransaction databaseTransaction)
    {
        Ensure.NotNull(databaseTransaction);
        Ensure.That(
            databaseTransaction.Size <= 4096,
            "Transaction size must be less than or equal to 4096 bytes.");

        using var walTransaction = new WriteAheadLogTransaction(databaseTransaction);

        _transactionBuffer.Append(walTransaction);

        // wait until the wal transaction has been flushed to disk
        await walTransaction.WaitForCommitAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Continuously flushes the transaction buffer to the write-ahead log until a cancellation is requested.
    /// </summary>
    /// <param name="cancellationToken">The token used to monitor for request cancellation and stop the operation.</param>
    /// <remarks>This method will block until a cancellation is requested.</remarks>
    [DoesNotReturn]
    public void Flush(CancellationToken cancellationToken)
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

            // fsync and rotate the file if necessary
            _file.Rotate();
        }
        // ReSharper disable once FunctionNeverReturns
    }
}