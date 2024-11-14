using JotDB.Storage;
using JotDB.Storage.Data;
using JotDB.Storage.Journaling;

namespace JotDB;

public class Database
{
    private readonly Journal _journal;
    private readonly JournalPipe _journalPipe;
    private readonly DataPipe _dataPipe;
    private readonly JournalPipeHandler _journalPipeHandler;

    private CancellationTokenSource? _cancellationTokenSource;

    public Database()
    {
        _journal = Journal.Open("journal.txt");
        _journalPipe = new JournalPipe();
        _dataPipe = new DataPipe();
        _journalPipeHandler = new JournalPipeHandler(_journalPipe, _dataPipe, _journal);
    }

    public async Task<ulong> InsertDocumentAsync(ReadOnlyMemory<byte> document)
    {
        // sends the document to the journal pipeline to be written to disk.
        // this call only blocks when the pipeline is full.
        var entry = await _journalPipe.SendAsync(
            document,
            DocumentOperationType.Insert,
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

        _journalPipeHandler.Start();
    }

    public async Task ShutdownAsync()
    {
        Console.WriteLine("shutting down database.");
        await _cancellationTokenSource.CancelAsync().ConfigureAwait(false);
        _journal.Dispose();
    }
}