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
public sealed class WriteAheadLogTransaction(Transaction transaction)
{
    private readonly AsyncAwaiter _awaiter = new();

    public Transaction Transaction { get; } = transaction;

    public Task WaitForCommitAsync()
    {
        return _awaiter.WaitForSignalAsync();
    }

    public void Abort(Exception ex)
    {
        _awaiter.SignalFault(ex);
    }

    public void CommitAfter(Task awaiter)
    {
        _awaiter.SignalCompletionAfter(awaiter);
    }
}