namespace JotDB.Tests;

public partial class InsertTests
{
    [Fact]
    public async Task InsertSingle()
    {
        var transaction = _database.CreateTransaction();

        transaction.EnlistCommand(DatabaseCommandType.Insert,
            _data);

        await transaction.CommitAsync();
    }
}