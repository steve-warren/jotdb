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
    private readonly AsyncAwaiter _awaiter = new();

    /// <summary>
    /// Represents a wrapper around a transaction, designed to work with a write-ahead log (WAL) mechanism.
    /// </summary>
    /// <remarks>
    /// This class encapsulates a transaction and provides mechanisms to manage its state and commit handling
    /// within the context of a write-ahead log. The transaction can wait for commit signals, be aborted with an exception,
    /// or commit after the completion of a specific task.
    /// </remarks>
    public WriteAheadLogTransaction(DatabaseTransaction databaseTransaction)
    {
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
        uint commitSequenceNumber,
        long timestamp)
    {
        CommitSequenceNumber = commitSequenceNumber;

        var header = new WriteAheadLogTransactionHeader
        {
            TransactionSequenceNumber =
                DatabaseTransaction.TransactionSequenceNumber,
            CommitSequenceNumber = commitSequenceNumber,
            TransactionType = (int)DatabaseTransaction.Type,
            Timestamp = timestamp
        };

        foreach (var operation in DatabaseTransaction.Operations)
        {
            var span = operation.Data.Span;
            header.DataLength = operation.Data.Length;
            header.Hash = MD5.HashData(span);
            writer.Write(header);
            writer.Write(span);
        }
    }

    public void Abort(string message)
    {
        _awaiter.SignalFault(new Exception(message));
    }

    public void Commit(Task after)
    {
        _awaiter.SignalCompletionAfter(after);
    }
}