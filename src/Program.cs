using System.Security.Cryptography;
using JotDB;
using JotDB.Metrics;

using var database = new Database(inMemory: true);
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
          {
              "transaction_id": 2,
              "transaction_date": "1/28/2022",
              "transaction_amount": 627.91,
              "transaction_type": "withdrawal",
              "account_number": 2666541771,
              "merchant_name": "Rhyzio",
              "transaction_category": "shopping",
              "transaction_description": "turpis adipiscing lorem vitae mattis nibh ligula nec sem duis aliquam convallis nunc proin at turpis a",
              "card_type": "visa",
              "location": "PO Box 57660"
            }
        """u8.ToArray();

Console.WriteLine($"payload is {data.Length} bytes");

/*
_ = Task.Run(() =>
{
    while (!cts.IsCancellationRequested)
    {
        Thread.Sleep(1000);
        Console.WriteLine($"{DateTime.Now} - {database
            .AverageTransactionExecutionTime.TotalMilliseconds} ms\n{database
            .TransactionSequenceNumber:N0} transactions");
    }
}, cts.Token);
*/

var limit = 100;
var tasks = new Task[Environment.ProcessorCount];

var watch = StopwatchSlim.StartNew();
for (var i = 0; i < Environment.ProcessorCount; i++)
{
    tasks[i] = Task.Factory.StartNew(() =>
    {
        while (Interlocked.Decrement(ref limit) > 0)
            database.InsertDocumentAsync(data).GetAwaiter().GetResult();
    }, TaskCreationOptions.LongRunning);
}

Task.WaitAll(tasks);

Console.WriteLine(watch.Elapsed.TotalMilliseconds);

database.TryShutdown();
run.Wait();