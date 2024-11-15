using JotDB.Storage;

namespace JotDB;

public class Database
{
    private readonly JournalFile _journal;
    private readonly List<BackgroundWorker> _backgroundWorkers = [];

    private readonly CancellationTokenSource _cancellationTokenSource;

    public Database()
    {
        _journal = JournalFile.Open("journal.txt");
        _cancellationTokenSource = new CancellationTokenSource();
    }

    public JournalFile Journal => _journal;

    public void RegisterBackgroundWorker(
        string name,
        Func<Database, CancellationToken, Task> work)
    {
        var worker = new BackgroundWorker(this, name, work);
        _backgroundWorkers.Add(worker);
    }

    public async Task<ulong> InsertDocumentAsync(ReadOnlyMemory<byte> document)
    {
        var operationId = await _journal
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

        foreach (var worker in _backgroundWorkers)
            worker.Start();
    }

    public async Task ShutdownAsync()
    {
        Console.WriteLine("shutting down database.");
        await _cancellationTokenSource.CancelAsync().ConfigureAwait(false);

        foreach(var worker in _backgroundWorkers)
            await worker.StopAsync().ConfigureAwait(false);

        _journal.Dispose();
    }
}