using JotDB.Storage.Documents;

namespace JotDB;

public static class DatabaseCommandExecutor
{
    public static void Execute(
        PageCollection pageCollection,
        DatabaseCommand databaseCommand)
    {
        try
        {
            ExecutionStrategy.Execute(pageCollection, databaseCommand);
        }

        catch
        {
            throw;
        }
    }
}