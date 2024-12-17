using JotDB.Memory;

namespace JotDB.Storage;

public class NullWriteAheadLogFile : IWriteAheadLogFile
{
    public void FlushToDisk()
    {
        Thread.Sleep(TimeSpan.FromMilliseconds(0.01));
    }

    public void WriteToDisk(AlignedMemory memory)
    {
        Thread.Sleep(TimeSpan.FromMilliseconds(0.01));
    }
}