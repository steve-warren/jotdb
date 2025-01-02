namespace JotDB;

public static class DatabaseCommandExecutor
{
    public static void Execute(DatabaseCommand databaseCommand)
    {
        switch(databaseCommand.Type)
        {
            case DatabaseOperationType.Insert:
                ExecutionStrategy.Insert(databaseCommand);
                break;
            case DatabaseOperationType.Update:
                ExecutionStrategy.Update(databaseCommand);
                break;
            case DatabaseOperationType.Delete:
                ExecutionStrategy.Delete(databaseCommand);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}