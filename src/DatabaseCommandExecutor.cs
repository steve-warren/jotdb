namespace JotDB;

public static class DatabaseCommandExecutor
{
    public static void Execute(DatabaseCommand databaseCommand)
    {
        ExecutionStrategy.Execute(databaseCommand);
    }
}