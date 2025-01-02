using System.Runtime.CompilerServices;
using JotDB.Storage.Documents;

namespace JotDB;

public static partial class ExecutionStrategy
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Execute(
        PageCollection pageCollection,
        DatabaseCommand databaseCommand)
    {
        switch(databaseCommand.CommandType)
        {
            case DatabaseCommandType.Insert:
                Insert(pageCollection, databaseCommand);
                break;
            case DatabaseCommandType.Update:
                Update(databaseCommand);
                break;
            case DatabaseCommandType.Delete:
                Delete(databaseCommand);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}
