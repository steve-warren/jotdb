using System.Security.Cryptography;
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
    }

    public Transaction Transaction { get; }
    public WriteAheadLogTransactionHeader Header { get; private set; }
    public ulong CommitSequenceNumber { get; private set; }
    
    public Task WaitForCommitAsync()
    {
        return _awaiter.WaitForSignalAsync();
    }

    public void Prepare(
        ulong commitSequenceNumber,
        long timestamp)
    {
        CommitSequenceNumber = commitSequenceNumber;
        Header = new WriteAheadLogTransactionHeader
        {
            DataLength = Transaction.Data.Length,
            TransactionSequenceNumber = Transaction.TransactionSequenceNumber,
            CommitSequenceNumber = commitSequenceNumber,
            TransactionType = (byte)Transaction.Type,
            Hash = MD5.HashData(Transaction.Data.Span),
            Timestamp = timestamp
        };
    }

    public void CopyTo(Span<byte> destination, int offset)
    {
        throw new NotImplementedException();
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