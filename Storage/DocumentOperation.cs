namespace JotDB.Storage;

public sealed class DocumentOperation
{
    private readonly TaskCompletionSource _tcs = new();

    /// <summary>
    /// Gets a task that represents the asynchronous write operation's completion status for the journal entry.
    /// The task completes when the entry has been successfully written and flushed to the durable storage.
    /// </summary>
    public ReadOnlyMemory<byte> Data { get; init; }
    public DocumentOperationType OperationType { get; init; }
    public ulong OperationId { get; private set; }

    /// <summary>
    /// Marks the journal entry as written to disk and sets the task result.
    /// </summary>
    public void CompleteWriteToJournal() => _tcs.SetResult();

    public void AssignOperationId(ulong identity) => OperationId = identity;

    /// <summary>
    /// Waits asynchronously until the journal entry has been marked as written, or the cancellation token is triggered.
    /// </summary>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task WaitUntilWriteToDiskCompletesAsync(CancellationToken cancellationToken)
    {
        cancellationToken.Register(() => _tcs.TrySetCanceled());

        return _tcs.Task;
    }
}