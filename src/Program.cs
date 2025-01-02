using System.Diagnostics;
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
          }
        """u8.ToArray();

Console.WriteLine($"payload is {data.Length} bytes");

var limit = 100;
var totalTime = Stopwatch.StartNew();

Parallel.ForAsync(0, limit, cts.Token, async (i, token) =>
{
    var transaction = database.CreateTransaction();
    transaction.Timeout = 15_000;

    var command = transaction.CreateCommand(DatabaseOperationType.Insert, data);

    command.Execute();
 
    await transaction.CommitAsync().ConfigureAwait(false);
}).GetAwaiter().GetResult();

var elapsed = totalTime.Elapsed;

Console.WriteLine($"Completed {limit} transactions in {elapsed.TotalSeconds} s");
Console.WriteLine($"{limit / elapsed.TotalSeconds} transactions per second.");

OutputStats();

database.TryShutdown();
run.Wait();
return;

void OutputStats()
{
    Console.WriteLine($"{DateTime.Now}");
    Console.Write("\e[38;2;127;255;212m");
    Console.WriteLine(
        $"dtrx avg_time: {MetricSink.DatabaseTransactions.AverageTransactionExecutionTime.TotalMilliseconds:N4} ms");
    Console.WriteLine(
        $"dtrx count: {MetricSink.DatabaseTransactions.TransactionCount:N0} transactions");
    Console.WriteLine(
        $"strx avg_time: {MetricSink.StorageTransactions
            .AverageExecutionTime.TotalMilliseconds:N4} ms");
    Console.WriteLine(
        $"strx avg_merged_trx_count: {MetricSink.StorageTransactions
            .AverageMergedTransactionCount:N4}");
    Console.WriteLine(
        $"strx avg_bytes_committed: {MetricSink.StorageTransactions.AverageBytesCommitted:N4} bytes");
    Console.WriteLine(
        $"wal avg_rotation_time: {MetricSink.WriteAheadLog
            .AverageRotationTime.TotalMilliseconds:N4} ms");
    Console.WriteLine(
        $"wal rotation_count: {MetricSink.WriteAheadLog
            .RotationCount:N0} rotations");
    Console.WriteLine(
        $"wal avg_write_time: {MetricSink.WriteAheadLog.AverageWriteTime
            .TotalMicroseconds:N4} μs");
    Console.WriteLine(
        $"wal write_count: {MetricSink.WriteAheadLog.WriteCount:N0} writes");
    Console.Write("\e[0m");
}