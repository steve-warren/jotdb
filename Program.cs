using System.Diagnostics;
using JotDB;

var database = new Database();

database.DeleteJournal();
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

for (var i = 0; i < 100; i++)
{
    var id = i;
    tasks.Add(Task.Run(async () =>
    {
        var watch = new Stopwatch();

        while (true)
        {
            watch.Restart();
            await database.InsertDocumentAsync(data).ConfigureAwait(false);
            Console.WriteLine($"Client write completed in {watch.ElapsedMilliseconds} ms");
            await Task.Delay(1).ConfigureAwait(false);
        }
    }));
}

await Task.WhenAll(tasks);
await database.ShutdownAsync();