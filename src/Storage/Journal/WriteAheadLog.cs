namespace JotDB.Storage.Journal;

public class WriteAheadLog
{
    public Task AppendAsync(Transaction transaction)
    {
        return Task.CompletedTask;
    }
}