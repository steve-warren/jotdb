using System.Diagnostics;

namespace JotDB;

public class Database
{
    private Journal? _journal;
    private DocumentCollection _documentCollection;
    private Task? _journalWriterBackgroundTask;
    private Task? _documentWriterBackgroundTask;

    private CancellationTokenSource? _cancellationTokenSource;
    
    public Database()
    {
    }
    
    public async Task<ulong> InsertDocumentAsync(ReadOnlyMemory<byte> document)
    {
        var journalEntry = await _journal.WriteJournalEntryAsync(
            document,
            DatabaseOperation.Insert).ConfigureAwait(false);

        return journalEntry.Identity;
    }

    public void Start()
    {
        Debug.WriteLine("starting database.");
        _cancellationTokenSource = new CancellationTokenSource();
        _documentCollection = new DocumentCollection("documents.jot");
        _journal = new Journal(0, "journal.wal", _documentCollection.PendingDocumentWriteQueue);
        _journalWriterBackgroundTask = _journal.ProcessJournalEntriesAsync(_cancellationTokenSource.Token);
        _documentWriterBackgroundTask =
            _documentCollection.ProcessPendingDocumentWriteOperationsAsync(_cancellationTokenSource.Token);
    }

    public async Task ShutdownAsync()
    {
        Debug.WriteLine("shutting down database.");
        await _cancellationTokenSource.CancelAsync().ConfigureAwait(false);
        await _journalWriterBackgroundTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        await _documentWriterBackgroundTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        await _journal.DisposeAsync().ConfigureAwait(false);
    }

    public void DeleteJournal()
    {
        Debug.WriteLine("deleting journal file.");
        File.Delete("journal.wal");
    }
}