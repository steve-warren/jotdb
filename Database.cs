namespace JotDB;

public class Database
{
    private readonly Journal _journal;
    private DocumentCollection _documentCollection;
    private Task? _documentWriterBackgroundTask;
    private readonly JournalPipeline _pipeline;
    private readonly JournalPipelineReceiver _journalPipelineReceiver;

    private CancellationTokenSource? _cancellationTokenSource;

    public Database()
    {
        _journal = Journal.Open("journal.txt");
        _pipeline = new JournalPipeline();
        _journalPipelineReceiver = new JournalPipelineReceiver(_pipeline, _journal);
    }

    public async Task<ulong> InsertDocumentAsync(ReadOnlyMemory<byte> document)
    {
        // sends the document to the journal pipeline to be written to disk.
        // this call only blocks when the pipeline is full.
        var entry = await _pipeline.SendAsync(
            document,
            DatabaseOperation.Insert,
            CancellationToken.None).ConfigureAwait(false);

        // we wait until the document has been written to disk.
        // that way, we can guarantee that the document is persisted.
        await entry.WaitUntilWriteToDiskCompletesAsync(CancellationToken.None)
                .ConfigureAwait(false);

        // send to the data file pipeline without waiting

        return entry.Identity;
    }

    public Task DeleteDocumentAsync(ulong documentId)
    {
        throw new NotImplementedException();
    }

    public void Start()
    {
        Console.WriteLine("starting jotdb database.");
        _cancellationTokenSource = new CancellationTokenSource();
        _documentCollection = new DocumentCollection("documents.jotdb");

        _journalPipelineReceiver.Start();

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
}