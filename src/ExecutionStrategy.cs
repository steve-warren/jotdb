using System.Runtime.CompilerServices;

namespace JotDB;

public static partial class ExecutionStrategy
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Execute(DatabaseCommand databaseCommand)
    {
        switch(databaseCommand.CommandType)
        {
            case DatabaseCommandType.Insert:
                Insert(databaseCommand);
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
