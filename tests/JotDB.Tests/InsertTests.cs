using JotDB.Configuration;
using JotDB.Storage.Documents;
using JotDB.Storage.Journal;

namespace JotDB.Tests;

public partial class InsertTests : IAsyncLifetime
{
    private readonly Database _database = new(new WriteAheadLog(
        new WriteAheadLogOptions
        {
            Mode = "in-memory"
        }), new DocumentCollection("documents"));
    private readonly Task _run;

    private readonly ReadOnlyMemory<byte> _data =
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

    public InsertTests()
    {
        _run = _database.RunAsync();
    }

    public async Task DisposeAsync()
    {
        try
        {
            _database.TryShutdown();
            await _run;
        }

        finally
        {
            _database.Dispose();
        }
    }

    public Task InitializeAsync() => Task.CompletedTask;
}