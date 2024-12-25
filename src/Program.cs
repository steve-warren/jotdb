using JotDB;
using JotDB.Metrics;

using var database = new Database(inMemory: false);
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
          }
        """u8.ToArray();

Console.WriteLine($"payload is {data.Length} bytes");

_ = Task.Run(() =>
{
    while (!cts.IsCancellationRequested)
    {
        Thread.Sleep(1000);
        Console.WriteLine($"{DateTime.Now} - {database
            .AverageTransactionExecutionTime.TotalMilliseconds} ms {database
            .TransactionSequenceNumber:N0} transactions");

        Console.WriteLine(
            $"\tstrx time: {MetricSink.StorageTransactions.ExecutionTime.TotalMilliseconds} ms");
        Console.WriteLine(
            $"\tstrx merged_trx_count: {MetricSink.StorageTransactions.MergedTransactionCount}");
        Console.WriteLine(
            $"\tstrx bytes_committed: {MetricSink.StorageTransactions.BytesCommitted:N0} bytes");
    }
}, cts.Token);

while (!cts.IsCancellationRequested)
{
    var limit = 100;
    var tasks = new Task[4];

    var watch = StopwatchSlim.StartNew();
    for (var i = 0; i < 4; i++)
    {
        tasks[i] = Task.Factory.StartNew(() =>
        {
            while (Interlocked.Decrement(ref limit) > 0)
                database.InsertDocumentAsync(data).GetAwaiter().GetResult();
        }, TaskCreationOptions.LongRunning);
    }

    Task.WaitAll(tasks);

    Console.WriteLine($"{watch.Elapsed.TotalMilliseconds:N0} ms");

    Thread.Sleep(1000);
}

database.TryShutdown();
run.Wait();