using System.Diagnostics;

namespace JotDB;

public class Database
{
    private readonly Journal _journal;
    private DocumentCollection _documentCollection;
    private Task? _documentWriterBackgroundTask;
    private readonly JournalPipeline _pipeline;
    private readonly JournalWriterBackgroundTask _journalWriterBackgroundTask;

    private CancellationTokenSource? _cancellationTokenSource;

    public Database()
    {
        _journal = new Journal(0, "journal.txt");
        _pipeline = new JournalPipeline();
        _journalWriterBackgroundTask = new JournalWriterBackgroundTask(_pipeline, _journal);
    }

    public async Task<ulong> InsertDocumentAsync(ReadOnlyMemory<byte> document)
    {
        var entry = await _pipeline.SendAsync(
            document,
            DatabaseOperation.Insert,
            CancellationToken.None).ConfigureAwait(false);

        await entry.WaitUntilWriteToDiskCompletesAsync(CancellationToken.None).ConfigureAwait(false);

        return entry.Identity;
    }

    public void Start()
    {
        Console.WriteLine("starting jotdb database.");
        _cancellationTokenSource = new CancellationTokenSource();
        _documentCollection = new DocumentCollection("documents.jotdb");

        _journalWriterBackgroundTask.Start();

        _documentWriterBackgroundTask =
            _documentCollection.ProcessPendingDocumentWriteOperationsAsync(_cancellationTokenSource.Token);
    }

    public async Task ShutdownAsync()
    {
        Console.WriteLine("shutting down database.");
        await _cancellationTokenSource.CancelAsync().ConfigureAwait(false);
        await _documentWriterBackgroundTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        _journal.Dispose();
    }

    public void DeleteJournal()
    {
        Debug.WriteLine("deleting journal file.");
        File.Delete("journal.jotdb");
    }
}