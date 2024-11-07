namespace JotDB;

public class JournalEntry
{
    private readonly TaskCompletionSource _tcs = new();

    /// <summary>
    /// Gets a task that represents the asynchronous write operation's completion status for the journal entry.
    /// The task completes when the entry has been successfully written and flushed to the durable storage.
    /// </summary>
    public Task WriteCompletionTask => _tcs.Task;
    
    public ReadOnlyMemory<byte> Data { get; set; }
    public ulong Identity { get; set; }
    /// <summary>
    /// Marks the journal entry as written and sets the task result.
    /// </summary>
    public void FinishWriting() => _tcs.SetResult();
}