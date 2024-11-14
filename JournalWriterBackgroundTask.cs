using System.Diagnostics;

namespace JotDB;

public sealed class JournalWriterBackgroundTask
{
    private readonly JournalPipeline _pipeline;
    private readonly Journal _journal;
    private Task _backgroundTask;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    public JournalWriterBackgroundTask(
        JournalPipeline pipeline,
        Journal journal)
    {
        _pipeline = pipeline;
        _journal = journal;
        _backgroundTask = Task.CompletedTask;
    }

    public void Start()
    {
        Debug.WriteLine("starting journal writer background task.");
        _backgroundTask = RunAsync();
    }

    public Task StopAsync()
    {
        Debug.WriteLine("stopping journal writer background task.");
        _cancellationTokenSource.Cancel();
        return _backgroundTask;
    }

    private async Task RunAsync()
    {
        var buffer = new JournalEntry[5];

        while (!_cancellationTokenSource.IsCancellationRequested)
        {
            var count = await _pipeline
                .WaitAndReceiveAsync(buffer.AsMemory(), _cancellationTokenSource.Token)
                .ConfigureAwait(false);

            _journal.WriteToDisk(buffer.AsSpan(0, count));
        }
    }
}