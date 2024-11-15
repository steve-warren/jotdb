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
        // todo: ensure size
        var buffer = new byte[DocumentStream.Length];

        await DocumentStream.ReadExactlyAsync(buffer, cancellationToken);

        var tasks = new List<Task>();

        var numberOfDocuments = 0;
        var watch = Stopwatch.StartNew();

        for (var i = 0; i < NumberOfClients; i++)
        {
            var id = i;

            tasks.Add(Task.Run(WriteDocument, cancellationToken));
            continue;

            async Task? WriteDocument()
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var operationId = await _database.InsertDocumentAsync(buffer).ConfigureAwait(false);
                    Interlocked.Increment(ref numberOfDocuments);
                    await Task.Delay(ClientWaitTime, cancellationToken)
                        .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                }
            }
        }

        await Task.WhenAll(tasks);

        Console.WriteLine($"{numberOfDocuments} in {watch.ElapsedMilliseconds}ms");
        Console.WriteLine(numberOfDocuments / watch.Elapsed.TotalSeconds);
    }
}