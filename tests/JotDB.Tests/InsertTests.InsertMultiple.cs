namespace JotDB.Tests;

public partial class InsertTests
{
    [Fact]
    public async Task InsertMultiple()
    {
        var transaction = _database.CreateTransaction();

        transaction.EnlistCommand(DatabaseCommandType.Insert,
            _data);
        transaction.EnlistCommand(DatabaseCommandType.Insert,
            _data);

        await transaction.CommitAsync();
    }
}