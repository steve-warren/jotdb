using JotDB.Storage;
using JotDB.Storage.Documents;
using JotDB.Storage.Journaling;

namespace JotDB;

public class Database
{
    private readonly Journal _journal;
    private readonly JournalFileBuffer _journalFileBuffer;
    private readonly JournalPipeReader _journalPipeReader;

    private readonly DocumentCollection _documentCollection;
    private readonly DocumentCollectionFileBuffer _documentCollectionFileBuffer;

    private readonly CancellationTokenSource _cancellationTokenSource;

    public Database()
    {
        _journal = Journal.Open("journal.txt");
        _documentCollection = DocumentCollection.Open("documents.txt");
        _journalFileBuffer = new JournalFileBuffer();
        _documentCollectionFileBuffer = new DocumentCollectionFileBuffer();
        _journalPipeReader = new JournalPipeReader(_journalFileBuffer, _documentCollectionFileBuffer, _journal);
        
        _cancellationTokenSource = new CancellationTokenSource();
    }

    public async Task<ulong> InsertDocumentAsync(ReadOnlyMemory<byte> document)
    {
        // sends the document to the journal pipeline to be written to disk.
        // this call only blocks when the pipeline is full.
        var entry = await _journalFileBuffer.WriteAsync(
            document,
            DocumentOperationType.Insert,
            CancellationToken.None).ConfigureAwait(false);

        // we wait until the document has been written to disk.
        // that way, we can guarantee that the document is persisted.
        await entry.WaitUntilWriteToDiskCompletesAsync(CancellationToken.None)
                .ConfigureAwait(false);

        return entry.OperationId;
    }

    public Task DeleteDocumentAsync(ulong documentId)
    {
        throw new NotImplementedException();
    }

    public void Start()
    {
        Console.WriteLine("starting jotdb database.");

        _journalPipeReader.Start();
    }

    public async Task ShutdownAsync()
    {
        Console.WriteLine("shutting down database.");
        await _cancellationTokenSource.CancelAsync().ConfigureAwait(false);
        _journalFileBuffer.Close();
        _documentCollectionFileBuffer.Close();
        _journal.Dispose();
    }
}