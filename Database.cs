using System.Diagnostics;

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
        Debug.WriteLine("starting database.");
        _cancellationTokenSource = new CancellationTokenSource();
        _journal = new Journal(0, "journal.wal");
        _journalWriterBackgroundTask = _journal.ProcessJournalEntriesAsync(_cancellationTokenSource.Token);
    }

    public async Task ShutdownAsync()
    {
        Debug.WriteLine("shutting down database.");
        await _cancellationTokenSource.CancelAsync().ConfigureAwait(false);
        await _journalWriterBackgroundTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        await _journal.DisposeAsync().ConfigureAwait(false);
    }

    public void DeleteJournal()
    {
        Debug.WriteLine("deleting journal file.");
        File.Delete("journal.wal");
    }
}