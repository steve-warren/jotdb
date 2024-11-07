using System.Diagnostics;
using System.Text;
using JotDB;

using var fs = new FileStream("data.txt", new FileStreamOptions
{
    Access = FileAccess.Write,
    Mode = FileMode.Append
});
var file = new AppendOnlyFile(fs);
var journal = new Journal(0, file);

var cts = new CancellationTokenSource();
_ = journal.ProcessJournalEntriesAsync(cts.Token);

Parallel.For(0, 100, i =>
{
    var watch = new Stopwatch();
    var data = """
                       {
                         "id": 1,
                         "first_name": "Libbey",
                         "last_name": "Claessens",
                         "email": "lclaessens0@nyu.edu",
                         "ip_address": "187.249.82.137"
                       }
                       """u8.ToArray();

    while (true)
    {
        //Console.WriteLine($"Thread {i} writing timestamp {timestamp}");

        watch.Restart();
        journal.WriteJournalEntryAsync(data).Wait();
        Console.WriteLine($"{watch.ElapsedMilliseconds} write");
        //Console.WriteLine($"Thread {i} completed.");

        Task.Delay(Random.Shared.Next(15, 1000)).Wait();
    }
});