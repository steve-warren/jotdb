using System.Diagnostics;

namespace JotDB;

public sealed class JournalPipelineReceiver
{
    private readonly JournalPipeline _pipeline;
    private readonly Journal _journal;
    private Task _backgroundTask;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    public JournalPipelineReceiver(
        JournalPipeline pipeline,
        Journal journal)
    {
        _pipeline = pipeline;
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

    private async Task RunAsync()
    {
        var buffer = new JournalEntry[8];

        while (!_cancellationTokenSource.IsCancellationRequested)
        {
            // wait for journal entries to be written to the pipeline.
            var count = await _pipeline
                .WaitAndReceiveAsync(buffer.AsMemory(), _cancellationTokenSource.Token)
                .ConfigureAwait(false);

            Console.WriteLine($"writing {count} journal entries to disk.");

            // write the journal entries to disk.
            _journal.WriteToDisk(buffer.AsSpan(0, count));
        }
    }
}