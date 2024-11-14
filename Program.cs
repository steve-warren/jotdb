using System.Diagnostics;
using JotDB;

var cancellationTokenSource = new CancellationTokenSource();

var database = new Database();

Console.CancelKeyPress += (sender, e) =>
{
    e.Cancel = true;
    cancellationTokenSource.Cancel();
    database.ShutdownAsync().Wait();
};

database.Start();

var data = """
                   {
                     "id": 1,
                     "first_name": "Libbey",
                     "last_name": "Claessens",
                     "email": "lclaessens0@nyu.edu",
                     "ip_address": "187.249.82.137"
                   }
                   """u8.ToArray();

var tasks = new List<Task>();

for (var i = 0; i < 1; i++)
{
    var id = i;

    tasks.Add(Task.Run(WriteDocument));
    continue;

    async Task? WriteDocument()
    {
        var watch = new Stopwatch();

        while (!cancellationTokenSource.IsCancellationRequested)
        {
            watch.Restart();
            var transactionId = await database.InsertDocumentAsync(data).ConfigureAwait(false);
            Console.WriteLine($"Client write {transactionId} completed in {watch.ElapsedMilliseconds} ms");
            await Task.Delay(1000);
        }
    }
}

await Task.WhenAll(tasks);