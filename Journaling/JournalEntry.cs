namespace JotDB;

public sealed class JournalEntry
{
    private readonly TaskCompletionSource _tcs = new();

    /// <summary>
    /// Gets a task that represents the asynchronous write operation's completion status for the journal entry.
    /// The task completes when the entry has been successfully written and flushed to the durable storage.
    /// </summary>
    public ReadOnlyMemory<byte> Data { get; init; }
    public DatabaseOperation Operation { get; init; }
    public ulong Identity { get; private set; }

    /// <summary>
    /// Marks the journal entry as written to disk and sets the task result.
    /// </summary>
    public void CompleteWriteToDisk() => _tcs.SetResult();

    public void AssignIdentity(ulong identity) => Identity = identity;

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