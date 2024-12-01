using System.Diagnostics;

namespace JotDB.CommandLine;

public class WriteTestCommand : ICommand
{
    private readonly Database _database;

    public WriteTestCommand(Database database)
    {
        _database = database;
    }

    public required int NumberOfClients { get; init; }
    public required int ClientWaitTime { get; init; }
    public required Stream DocumentStream { get; init; }

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[4096];

        var count = DocumentStream.Read(buffer);
        var memory = buffer.AsMemory(0, count);

        var tasks = new List<Task>();

        var numberOfDocuments = 0;
        var watch = Stopwatch.StartNew();

        for (var i = 0; i < NumberOfClients; i++)
        {
            var id = i;

            tasks.Add(Task.Run(WriteDocument, CancellationToken.None));
            continue;

            async Task? WriteDocument()
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var operationId = await _database.InsertDocumentAsync(memory).ConfigureAwait(false);
                    Interlocked.Increment(ref numberOfDocuments);
                    await Task.Delay(ClientWaitTime, cancellationToken)
                        .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                }
            }
        }

        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        finally
        {
            Console.WriteLine($"{numberOfDocuments} in {watch.ElapsedMilliseconds}ms");
            Console.Out.Flush();
        }
    }
}