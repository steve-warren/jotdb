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
        //using var page = pageCollection.Allocate();

        //page.Write(command.Data.Span);
    }
}