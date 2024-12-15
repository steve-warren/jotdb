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
public sealed class WriteAheadLogTransaction
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
    public WriteAheadLogTransaction(Transaction transaction)
    {
        Transaction = transaction;
        Size = (uint)WriteAheadLogTransactionHeader.Size + (uint)transaction
            .Data.Length;
    }

    public uint Size { get; }

    public Transaction Transaction { get; }
    public ulong CommitSequenceNumber { get; private set; }

    public Task WaitForCommitAsync()
    {
        return _awaiter.WaitForSignalAsync();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryWrite(
        ref AlignedMemoryWriter writer,
        ulong commitSequenceNumber,
        long timestamp)
    {
        if (writer.BytesAvailable < Size)
            return false;

        CommitSequenceNumber = commitSequenceNumber;

        var header = new WriteAheadLogTransactionHeader
        {
            DataLength = Transaction.Data.Length,
            TransactionSequenceNumber = Transaction.TransactionSequenceNumber,
            CommitSequenceNumber = commitSequenceNumber,
            TransactionType = (int)Transaction.Type,
            Hash = MD5.HashData(Transaction.Data.Span),
            Timestamp = timestamp
        };

        writer.Write(header);
        writer.Write(Transaction.Data.Span);

        return true;
    }

    public void Abort(Exception ex)
    {
        _awaiter.SignalFault(ex);
    }

    public void Commit(Task after)
    {
        _awaiter.SignalCompletionAfter(after);
    }
}