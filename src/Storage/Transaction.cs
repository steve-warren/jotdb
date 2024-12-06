using System.Runtime.CompilerServices;

namespace JotDB.Storage;

public sealed class Transaction
{
    private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Gets a task that represents the asynchronous write operation's completion status for the journal entry.
    /// The task completes when the entry has been successfully written and flushed to the durable storage.
    /// </summary>
    public ReadOnlyMemory<byte> Data { get; init; }

    public TransactionType Type { get; init; }

    /// <summary>
    /// Marks the journal entry as written to disk and sets the task result.
    /// </summary>
    public void Commit()
    {
        _tcs.TrySetResult();
    }

    public void Abort(Exception exception)
    {
        _tcs.TrySetException(exception);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void CopyTo(Span<byte> destination, int offset)
    {
        Data.Span.CopyTo(destination.Slice(offset, Data.Length));
    }

    /// <summary>
    /// Waits asynchronously until the journal entry has been marked as written, or the cancellation token is triggered.
    /// </summary>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task WaitAsync(CancellationToken cancellationToken)
    {
        cancellationToken.Register(() => _tcs.TrySetCanceled());

        return _tcs.Task;
    }
}