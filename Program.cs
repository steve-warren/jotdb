using System.Diagnostics;
using System.Text;
using JotDB;
using JotDB.CommandLine;

using var database = new Database();

database.AddBackgroundWorker(
    "journal writer background worker",
    async (db, cancellationToken) =>
    {
        var journal = db.Journal;

        while (!cancellationToken.IsCancellationRequested)
        {
            await journal.WaitToFlushAsync(cancellationToken);
        }
    });

database.AddBackgroundWorker(
    "page writer",
    async (db, cancellationToken) =>
    {
        await foreach (var documentOperation in db.Journal.WaitToReadAsync(cancellationToken))
        {
            Debug.WriteLine(documentOperation.OperationId + " write to in-memory page");
        }
    });

var run = database.RunAsync();

Console.CancelKeyPress += (sender, e) =>
{
    database.TryShutdown();
    run.Wait();

    Console.WriteLine("exiting process");
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
    var command = new WriteTestCommand(database)
    {
        NumberOfClients = 1,
        ClientWaitTime = 1_000,
        DocumentStream = Console.OpenStandardInput()
    };

    await command.ExecuteAsync(CancellationToken.None);
    return;
}

while (true)
{
    Console.Write("jot> ");
    var commandText = Console.ReadLine();

    if (commandText.StartsWith("insert into c "))
    {
        var data = Encoding.UTF8.GetBytes(commandText, 14, commandText.Length - 14);

        var watch = Stopwatch.StartNew();
        var id = await database.InsertDocumentAsync(data);
        Console.WriteLine($"command completed in {watch.ElapsedMilliseconds}ms");
    }

    else
    {
        Console.WriteLine("unknown command");
    }
}