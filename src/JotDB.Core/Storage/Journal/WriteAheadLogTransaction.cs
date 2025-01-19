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
               databaseTransaction.CommandCount +
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

    /// <summary>
    /// Returns a task that completes after the transaction has been committed
    /// to disk.
    /// </summary>
    /// <returns></returns>
    public Task WaitForCommitAsync()
    {
        return _awaiter.WaitForSignalAsync();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public unsafe void Write(
        ref AlignedMemoryWriter writer,
        uint storageTransactionSequenceNumber,
        uint commitSequenceNumber,
        long timestamp)
    {
        CommitSequenceNumber = commitSequenceNumber;

        WriteAheadLogTransactionHeader header;
        var p = &header;

        p->StorageTransactionSequenceNumber =
            storageTransactionSequenceNumber;
        p->CommitSequenceNumber = commitSequenceNumber;
        p->DatabaseTransactionSequenceNumber =
            DatabaseTransaction.TransactionSequenceNumber;
        p->TransactionType = (int) DatabaseTransaction.Type;
        p->Timestamp = timestamp;

        // ReSharper disable once ForCanBeConvertedToForeach
        for(var i = 0; i < DatabaseTransaction.Commands.Count; i++)
        {
            var operation = DatabaseTransaction.Commands[i];

            var data = operation.Data.Span;
            p->DataLength = data.Length;

            var hash = new Span<byte>(p->Hash, MD5.HashSizeInBytes);
            MD5.HashData(data, hash);

            writer.Write(p);
            writer.Write(data);
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
    /// <param name="when">The task after which the transaction will be committed.</param>
    public void Commit(Task when)
    {
        _awaiter.SignalCompletionWhen(when);
    }
}