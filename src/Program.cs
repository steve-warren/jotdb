using System.Diagnostics;
using JotDB;

using var database = new Database();
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

var data =
    """
          {
            "transaction_id": 1,
            "transaction_date": "8/1/2022",
            "transaction_amount": 7908.04,
            "transaction_type": "transfer",
            "account_number": 5106756131,
            "merchant_name": "Skalith",
            "transaction_category": "entertainment",
            "transaction_description": "justo aliquam quis turpis eget elit sodales scelerisque mauris sit amet eros suspendisse accumsan tortor quis",
            "card_type": "amex",
            "location": "PO Box 70107"
          },
        """u8.ToArray();

for (var i = 0; i < Environment.ProcessorCount; i++)
{
    var id = i;
    Task.Run(async () =>
    {
        while (true)
        {
            var watch = Stopwatch.StartNew();
            await database.InsertDocumentAsync(data);
            Console.WriteLine($"task {id}: command completed in {watch.ElapsedMilliseconds}ms");
            await Task.Delay(Random.Shared.Next(1000, 4000));
        }
    });
}

run.Wait();