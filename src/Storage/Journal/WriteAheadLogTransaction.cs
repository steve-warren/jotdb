using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using JotDB.Memory;
using JotDB.Threading;

namespace JotDB.Storage.Journal;

/// <summary>
/// Represents a wrapper around a transaction, designed to work with a write-ahead log (WAL) mechanism.
/// </summary>
/// <remarks>
/// This class encapsulates a transaction and provides mechanisms to manage its state and commit handling
/// within the context of a write-ahead log. The transaction can wait for commit signals, be aborted with an exception,
/// or commit after the completion of a specific task.
/// </remarks>
public sealed class WriteAheadLogTransaction : IDisposable
{
    private readonly AsyncAwaiter _awaiter;

    /// <summary>
    /// Represents a wrapper around a transaction, designed to work with a write-ahead log (WAL) mechanism.
    /// </summary>
    /// <remarks>
    /// This class encapsulates a transaction and provides mechanisms to manage its state and commit handling
    /// within the context of a write-ahead log. The transaction can wait for commit signals, be aborted with an exception,
    /// or commit after the completion of a specific task.
    /// </remarks>
    public WriteAheadLogTransaction(
        DatabaseTransaction databaseTransaction)
    {
        _awaiter = new AsyncAwaiter(databaseTransaction.Timeout);
        DatabaseTransaction = databaseTransaction;
        Size = (uint)WriteAheadLogTransactionHeader.Size *
               databaseTransaction.OperationCount +
               databaseTransaction
                   .Size;
    }

    ~WriteAheadLogTransaction()
    {
        Dispose();
    }

    public uint Size { get; }
    public DatabaseTransaction DatabaseTransaction { get; }
    public uint CommitSequenceNumber { get; private set; }

    public void Dispose()
    {
        _awaiter.Dispose();

        GC.SuppressFinalize(this);
    }

    public Task WaitForCommitAsync()
    {
        return _awaiter.WaitForSignalAsync();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(
        ref AlignedMemoryWriter writer,
        uint storageTransactionSequenceNumber,
        uint commitSequenceNumber,
        long timestamp)
    {
        CommitSequenceNumber = commitSequenceNumber;

        var header = new WriteAheadLogTransactionHeader
        {
            StorageTransactionSequenceNumber = storageTransactionSequenceNumber,
            CommitSequenceNumber = commitSequenceNumber,
            DatabaseTransactionSequenceNumber =
                DatabaseTransaction.TransactionSequenceNumber,
            TransactionType = (int)DatabaseTransaction.Type,
            Timestamp = timestamp
        };

        // ReSharper disable once ForCanBeConvertedToForeach
        for(var i = 0; i < DatabaseTransaction.Operations.Count; i++)
        {
            var operation = DatabaseTransaction.Operations[i];

            var span = operation.Data.Span;
            header.DataLength = span.Length;
            header.Hash = MD5.HashData(span);

            writer.Write(header);
            writer.Write(span);
        }
    }

    public void Abort(string message)
    {
        _awaiter.SignalFault(new Exception(message));
    }

    /// <summary>
    /// Schedules the commit of a transaction to occur after the specified task completes.
    /// </summary>
    /// <remarks>
    /// This method allows deferring the commit operation of a transaction until a given task is completed.
    /// It leverages the asynchronous awaiter mechanism to signal the completion of the transaction.
    /// </remarks>
    /// <param name="after">The task after which the transaction will be committed.</param>
    public void CommitWhen(Task after)
    {
        _awaiter.SignalCompletionWhen(after);
    }
}