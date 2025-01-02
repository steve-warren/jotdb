using System.Runtime.CompilerServices;

namespace JotDB;

public static partial class ExecutionStrategy
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Insert(DatabaseCommand command)
    {
        // no-op
    }
}