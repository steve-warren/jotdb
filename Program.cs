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
           [{"transaction_id":1,"transaction_date":"8/1/2022","transaction_amount":7908.04,"transaction_type":"transfer","account_number":5106756131,"merchant_name":"Skalith","transaction_category":"entertainment","transaction_description":"justo aliquam quis turpis eget elit sodales scelerisque mauris sit amet eros suspendisse accumsan tortor quis","card_type":"amex","location":"PO Box 70107"},
           {"transaction_id":2,"transaction_date":"1/28/2022","transaction_amount":627.91,"transaction_type":"withdrawal","account_number":2666541771,"merchant_name":"Rhyzio","transaction_category":"shopping","transaction_description":"turpis adipiscing lorem vitae mattis nibh ligula nec sem duis aliquam convallis nunc proin at turpis a","card_type":"visa","location":"PO Box 57660"},
           {"transaction_id":3,"transaction_date":"10/19/2022","transaction_amount":1067.03,"transaction_type":"deposit","account_number":6952642797,"merchant_name":"Rhyzio","transaction_category":"utilities","transaction_description":"ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae donec pharetra magna vestibulum aliquet ultrices","card_type":"visa","location":"Suite 52"},
           {"transaction_id":4,"transaction_date":"12/13/2022","transaction_amount":7385.37,"transaction_type":"deposit","account_number":3760909987,"merchant_name":"Abatz","transaction_category":"food","transaction_description":"purus sit amet nulla quisque arcu libero rutrum ac lobortis vel dapibus at diam nam","card_type":"mastercard","location":"PO Box 42295"},
           {"transaction_id":5,"transaction_date":"5/1/2022","transaction_amount":6008.59,"transaction_type":"withdrawal","account_number":1058018697,"merchant_name":"Eamia","transaction_category":"food","transaction_description":"mauris viverra diam vitae quam suspendisse potenti nullam porttitor lacus at turpis","card_type":"visa","location":"Suite 9"},
           {"transaction_id":6,"transaction_date":"6/10/2022","transaction_amount":9375.88,"transaction_type":"deposit","account_number":3545327136,"merchant_name":"Topicshots","transaction_category":"food","transaction_description":"ipsum aliquam non mauris morbi non lectus aliquam sit amet diam in magna bibendum","card_type":"mastercard","location":"Room 964"},
           {"transaction_id":7,"transaction_date":"12/17/2022","transaction_amount":7109.95,"transaction_type":"transfer","account_number":8420128570,"merchant_name":"Brainlounge","transaction_category":"shopping","transaction_description":"ac neque duis bibendum morbi non quam nec dui luctus rutrum nulla tellus in sagittis dui vel nisl duis","card_type":"visa","location":"Room 702"},
           {"transaction_id":8,"transaction_date":"6/2/2022","transaction_amount":169.61,"transaction_type":"deposit","account_number":2818151676,"merchant_name":"Katz","transaction_category":"entertainment","transaction_description":"ipsum primis in faucibus orci luctus et ultrices posuere cubilia","card_type":"amex","location":"Suite 65"},
           {"transaction_id":9,"transaction_date":"4/17/2022","transaction_amount":9259.9,"transaction_type":"transfer","account_number":9920037255,"merchant_name":"Chatterbridge","transaction_category":"entertainment","transaction_description":"feugiat non pretium quis lectus suspendisse potenti in eleifend quam a odio in hac habitasse platea dictumst maecenas","card_type":"visa","location":"Apt 480"},
           {"transaction_id":10,"transaction_date":"2/2/2022","transaction_amount":4181.09,"transaction_type":"withdrawal","account_number":7593061952,"merchant_name":"Dabfeed","transaction_category":"food","transaction_description":"eget orci vehicula condimentum curabitur in libero ut massa volutpat convallis morbi odio odio elementum eu interdum eu","card_type":"mastercard","location":"Suite 38"}]
           """u8.ToArray();

var tasks = new List<Task>();

var numberOfDocuments = 0;
var watch = Stopwatch.StartNew();

for (var i = 0; i < 8; i++)
{
    var id = i;

    tasks.Add(Task.Run(WriteDocument));
    continue;

    async Task? WriteDocument()
    {
        while (!cancellationTokenSource.IsCancellationRequested)
        {
            var transactionId = await database.InsertDocumentAsync(data).ConfigureAwait(false);
            Interlocked.Increment(ref numberOfDocuments);
            //await Task.Delay(1000, cancellationTokenSource.Token).ConfigureAwait(false);
        }
    }
}

await Task.WhenAll(tasks);

Console.WriteLine($"{numberOfDocuments} in {watch.ElapsedMilliseconds}ms");
Console.WriteLine(numberOfDocuments / watch.Elapsed.TotalSeconds);