using System.Diagnostics;
using JotDB.Storage.Documents;

namespace JotDB.Storage.Journaling;

public sealed class JournalPipeReader
{
    private readonly JournalFileBuffer _fileBuffer;
    private readonly DocumentCollectionFileBuffer _documentCollectionFileBuffer;
    private readonly Journal _journal;
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private Task _backgroundTask;

    public JournalPipeReader(
        JournalFileBuffer fileBuffer,
        DocumentCollectionFileBuffer documentCollectionFileBuffer,
        Journal journal)
    {
        _fileBuffer = fileBuffer;
        _documentCollectionFileBuffer = documentCollectionFileBuffer;
        _journal = journal;
        _backgroundTask = Task.CompletedTask;
    }

    public void Start()
    {
        Debug.WriteLine("starting journal pipeline receiver.");
        _backgroundTask = RunAsync();
    }

    public Task StopAsync()
    {
        Debug.WriteLine("stopping journal pipeline receiver.");
        _cancellationTokenSource.Cancel();
        return _backgroundTask;
    }

    /// <summary>
    /// Asynchronously runs and processes the journal entries from the journal pipeline
    /// until cancellation is requested.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private async Task RunAsync()
    {
        const int JOURNAL_BUFFER_SIZE = 8;

        var buffer = new DocumentOperation[JOURNAL_BUFFER_SIZE];

        // wait for journal entries to be written to the pipeline.
        while (await _fileBuffer
                   .WaitToReadAsync(_cancellationTokenSource.Token)
                   .ConfigureAwait(false))
        {
            var count = _fileBuffer.Read(buffer);
            Debug.WriteLine($"writing {count} journal entries to disk.");

            var span = buffer.AsSpan(0, count);

            // write the journal entries to disk.
            _journal.WriteToDisk(span);

            // write the journal entries to the data file.
            _documentCollectionFileBuffer.Write(span);
        }
    }
}