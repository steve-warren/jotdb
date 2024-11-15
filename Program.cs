using JotDB;
using JotDB.CommandLine;

using var database = new Database();

database.RegisterBackgroundWorker(
    "journal file background worker",
    async (db, cancellationToken) =>
    {
        var journal = db.Journal;

        while (!cancellationToken.IsCancellationRequested)
        {
            await journal.WaitToFlushAsync(cancellationToken);
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

if (args[0] == "write")
{
    var command = new WriteTestCommand(database)
    {
        NumberOfClients = 8,
        ClientWaitTime = 0,
        DocumentStream = Console.OpenStandardInput()
    };

    await command.ExecuteAsync(CancellationToken.None);
    return;
}

while (true)
{
    Console.Write("jot> ");
    var commandText = Console.ReadLine();

    if (commandText == "exit")
    {
        database.TryShutdown();
        await run;
        break;
    }
}