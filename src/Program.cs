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

/*_ = Task.Run(() =>
{
    while (!cts.IsCancellationRequested)
    {
        OutputStats();
    }
}, cts.Token);
*/

var limit = 10_000;

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

OutputStats();

database.TryShutdown();
run.Wait();

void OutputStats()
{
    Thread.Sleep(1000);
    Console.WriteLine($"{DateTime.Now} - dtrx time: {database
        .AverageTransactionExecutionTime.TotalMilliseconds} ms {database
        .TransactionSequenceNumber:N0} transactions");

    Console.WriteLine(
        $"strx time: {MetricSink.StorageTransactions
            .AverageExecutionTime.TotalMilliseconds:N0} ms");
    Console.WriteLine(
        $"strx merged_trx_count: {MetricSink.StorageTransactions
            .AverageMergedTransactionCount:N0}");
    Console.WriteLine(
        $"strx bytes_committed: {MetricSink.StorageTransactions.AverageBytesCommitted:N0} bytes");
    Console.WriteLine(
        $"wal_avg_rotation_time: {MetricSink.WriteAheadLog
            .AverageRotationTime.TotalMilliseconds:N0} ms");
    Console.WriteLine(
        $"wal_rotation_count: {MetricSink.WriteAheadLog
            .RotationCount:N0} rotations");
    Console.WriteLine(
        $"wal_write_time: {MetricSink.WriteAheadLog.AverageWriteTime
            .TotalMicroseconds:N0} μs");
    Console.WriteLine(
        $"wal_write_count: {MetricSink.WriteAheadLog.WriteCount:N0} writes");
}