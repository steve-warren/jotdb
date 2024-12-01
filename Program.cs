using System.Diagnostics;
using System.Text;
using JotDB;
using JotDB.CommandLine;
using JotDB.Storage;

using var database = new Database();

database.AddBackgroundWorker(
    "journal writer",
    async (db, cancellationToken) =>
    {
        var journal = db.Journal;

        while (!cancellationToken.IsCancellationRequested)
        {
            await journal.WaitToFlushAsync(cancellationToken).ConfigureAwait(false);
        }
    });

database.AddBackgroundWorker(
    "segment writer",
    async (db, cancellationToken) =>
    {
        await foreach (var documentOperation in
                       db.Journal.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            switch (documentOperation.OperationType)
            {
                case DocumentOperationType.Insert:
                    db.AddToCache(documentOperation.OperationId, documentOperation.Data);
                    break;
                case DocumentOperationType.Update:
                case DocumentOperationType.Delete:
                default:
                    throw new NotImplementedException();
            }

            db.Checkpoint(documentOperation.OperationId);
        }
    });

var run = database.RunAsync();

Console.CancelKeyPress += (sender, e) =>
{
    database.TryShutdown();
    run.Wait();

    Console.WriteLine("process exited");
};

// macOS: quit and force quit
AppDomain.CurrentDomain.ProcessExit += (sender, e) =>
{
    database.TryShutdown();
    run.Wait();
    Console.WriteLine("process exited");
};

AppDomain.CurrentDomain.UnhandledException += (sender, e) =>
{
    database.TryShutdown();
    run.Wait();
    Console.WriteLine($"Unhandled exception occurred.");
};

if (args.Length > 0 &&
    args[0] == "write")
{
    Console.WriteLine("starting write test...");
    var command = new WriteTestCommand(database)
    {
        NumberOfClients = 1,
        ClientWaitTime = 2_000,
        DocumentStream = Console.OpenStandardInput()
    };

    await command.ExecuteAsync(CancellationToken.None).ConfigureAwait(false);
    return;
}

while (true)
{
    Console.Write("jot> ");
    var commandText = "";

    var key = Console.ReadKey(false);

    if (key.Key == ConsoleKey.UpArrow)
    {
        Console.Write("insert into c ");
        commandText = "insert into c " + Console.ReadLine();
    }

    else
        commandText = key.KeyChar + Console.ReadLine();

    if (commandText.StartsWith("insert into c "))
    {
        var data = Encoding.UTF8.GetBytes(commandText, 14, commandText.Length - 14);

        var watch = Stopwatch.StartNew();
        var id = await database.InsertDocumentAsync(data).ConfigureAwait(false);
        Console.WriteLine($"command completed in {watch.ElapsedMilliseconds}ms");
    }

    else if (commandText.StartsWith("select * from c"))
    {
        foreach (var id in database.GetCachedDocuments())
        {
            Console.WriteLine(id);
        }
    }

    else if (commandText == "exit")
    {
        break;
    }

    else
    {
        Console.WriteLine("unknown command");
    }
}

database.TryShutdown();

await run.ConfigureAwait(false);
