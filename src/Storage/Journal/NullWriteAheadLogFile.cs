using JotDB.Memory;

namespace JotDB.Storage.Journal;

public class NullWriteAheadLogFile : IWriteAheadLogFile
{
    public void FlushToDisk()
    {
        // no-op
    }

    public void WriteToDisk(ReadOnlySpan<byte> span)
    {
        Thread.Sleep(1);
        // no-op
    }
}