using JotDB.Storage;

namespace JotDB;

public class Database
{
    private readonly JournalFile _journalFile;
    private readonly BackgroundWorker<JournalFile> _journalBackgroundWorker;

    private readonly CancellationTokenSource _cancellationTokenSource;

    public Database()
    {
        _journalFile = JournalFile.Open("journal.txt");

        _journalBackgroundWorker = new BackgroundWorker<JournalFile>(
            "journal file background worker",
            async (journalFile, cancellationToken) =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await journalFile.WaitToFlushAsync(cancellationToken);
                }
            }, _journalFile);

        _cancellationTokenSource = new CancellationTokenSource();
    }

    public async Task<ulong> InsertDocumentAsync(ReadOnlyMemory<byte> document)
    {
        var operationId = await _journalFile
            .WriteAsync(
                document,
                DocumentOperationType.Insert)
            .ConfigureAwait(false);

        return operationId;
    }

    public Task DeleteDocumentAsync(ulong documentId)
    {
        throw new NotImplementedException();
    }

    public void Start()
    {
        Console.WriteLine("starting jotdb database.");
        _journalBackgroundWorker.Start();
    }

    public async Task ShutdownAsync()
    {
        Console.WriteLine("shutting down database.");
        await _cancellationTokenSource.CancelAsync().ConfigureAwait(false);
        await _journalBackgroundWorker.StopAsync().ConfigureAwait(false);
        _journalFile.Dispose();
    }
}