using JotDB;

using var database = new Database();
var run = database.RunAsync();
var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, _) =>
{
    database.TryShutdown();
    run.Wait();
};

// macOS: quit and force quit
AppDomain.CurrentDomain.ProcessExit += (_, _) =>
{
    cts.Cancel();
    database.TryShutdown();

    run.Wait();
};

AppDomain.CurrentDomain.UnhandledException += (_, _) =>
{
    database.TryShutdown();
    run.Wait();
    Console.WriteLine("Unhandled exception occurred.");
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

_ = Task.Run(() =>
{
    while (true)
    {
        Thread.Sleep(1000);
        Console.WriteLine($"{DateTime.Now} - {database
            .AverageTransactionExecutionTime.TotalMilliseconds} ms");
    }
}, cts.Token);

while (!cts.IsCancellationRequested)
{
    for (var i = 0; i < 1; i++)
    {
        _ = Task.WhenAll(
            database.InsertDocumentAsync(data, data, data));
    }

    Thread.Sleep(100);
}

run.Wait();