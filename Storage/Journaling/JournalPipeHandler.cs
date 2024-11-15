using System.Diagnostics;
using JotDB.Storage.Data;

namespace JotDB.Storage.Journaling;

public sealed class JournalPipeHandler
{
    private readonly JournalPipe _pipe;
    private readonly DataPipe _dataPipe;
    private readonly Journal _journal;
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private Task _backgroundTask;

    public JournalPipeHandler(
        JournalPipe pipe,
        DataPipe dataPipe,
        Journal journal)
    {
        _pipe = pipe;
        _dataPipe = dataPipe;
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
        var buffer = new DocumentOperation[8];

        // wait for journal entries to be written to the pipeline.
        while (await _pipe
                   .WaitToReadAsync(_cancellationTokenSource.Token)
                   .ConfigureAwait(false))
        {
            var count = _pipe.Read(buffer);
            Debug.WriteLine($"writing {count} journal entries to disk.");

            var span = buffer.AsSpan(0, count);

            // write the journal entries to disk.
            _journal.WriteToDisk(span);

            // write the journal entries to the data file.
            _dataPipe.Send(span);
        }
    }
}