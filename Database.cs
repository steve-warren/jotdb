namespace JotDB;

public class Database
{
    private Journal? _journal;
    private Task? _journalWriterBackgroundTask;

    private CancellationTokenSource? _cancellationTokenSource;

    public Task<ulong> InsertDocumentAsync(ReadOnlyMemory<byte> document)
    {
        return _journal.WriteJournalEntryAsync(document);
    }

    public void Start()
    {
        _cancellationTokenSource = new CancellationTokenSource();
        _journal = new Journal(0, new AppendOnlyFile("journal.wal"));
        _journalWriterBackgroundTask = _journal.ProcessJournalEntriesAsync(_cancellationTokenSource.Token);
    }

    public async Task ShutdownAsync()
    {
        await _cancellationTokenSource.CancelAsync().ConfigureAwait(false);
        await _journalWriterBackgroundTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        await _journal.DisposeAsync().ConfigureAwait(false);
    }

    public void DeleteJournal()
    {
        File.Delete("journal.wal");
    }
}