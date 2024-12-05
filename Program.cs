﻿using System.Diagnostics;
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

        while (await journal.WaitAsync(cancellationToken))
        {
            Console.WriteLine("writing transactions to disk.");
            journal.WriteToDisk();
            Console.WriteLine("written transactions to disk.");
        }
    });

database.AddBackgroundWorker(
    "exception thrower",
    async (db, cancellationToken) =>
    {
        await Task.Delay(1000, CancellationToken.None);
        throw new NotSupportedException();
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
    if (!database.TryShutdown()) return;

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
        NumberOfClients = 8,
        ClientWaitTime = 100,
        DocumentStream = Console.OpenStandardInput()
    };

    await command.ExecuteAsync(CancellationToken.None);
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
        await database.InsertDocumentAsync(data);
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

await run;