using System.Runtime.CompilerServices;

namespace JotDB;

public static partial class ExecutionStrategy
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Delete(DatabaseCommand command)
    {
        throw new NotImplementedException();
    }
}