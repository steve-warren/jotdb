using System.Diagnostics;
using JotDB;
using JotDB.CommandLine;

using var database = new Database();

database.AddBackgroundWorker(
    "journal writer",
    async (db, cancellationToken) =>
    {
        var journal = db.Journal;

        while (await journal.WaitAsync(cancellationToken))
        {
            Console.WriteLine("writing transactions to disk.");
            journal.WriteToDisk();
            Console.WriteLine("written transactions to disk.");
        }
    });

var run = database.RunAsync();

Console.CancelKeyPress += (_, e) =>
{
    database.TryShutdown();
    run.Wait();
    Console.WriteLine("process exited");
};

// macOS: quit and force quit
AppDomain.CurrentDomain.ProcessExit += (_, e) =>
{
    database.TryShutdown();

    run.Wait();
    Console.WriteLine("process exited");
};

AppDomain.CurrentDomain.UnhandledException += (_, e) =>
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
        NumberOfClients = 8,
        ClientWaitTime = 100,
        DocumentStream = Console.OpenStandardInput()
    };

    await command.ExecuteAsync(CancellationToken.None);
    return;
}

while (true)
{
    var data = """{ "michael" : "scott" }"""u8.ToArray();

    var watch = Stopwatch.StartNew();
    await database.InsertDocumentAsync(data);
    Console.WriteLine($"command completed in {watch.ElapsedMilliseconds}ms");
    await Task.Delay(10000);
}
