using System.Runtime.CompilerServices;
using JotDB.Storage.Documents;

namespace JotDB;

public static partial class ExecutionStrategy
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Insert(
        PageCollection pageCollection,
        DatabaseCommand command)
    {
        using var page = pageCollection.Root;

        page.TryWrite(command.Data.Span);
    }
}